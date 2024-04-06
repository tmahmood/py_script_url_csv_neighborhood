import asyncio
import csv
from copy import copy, deepcopy
from pathlib import Path
from lxml.html import parse, resolve_base_href, fromstring

from playwright.async_api import async_playwright

from navigator import get_logger, Navigator, hash_url_and_split, prefix_data_cached, ConfigDict


def get_path(url, txt) -> Path:
    txt = txt.replace(" ", '_').replace("/", '--')
    upath = hash_url_and_split(url)
    path = prefix_data_cached(CFG, upath, txt)
    return path


async def get_doc(p, url, page):
    path = await download_url(p, url, page)
    with open(path) as f:
        content = f.read()
    doc = fromstring(content)
    doc.make_links_absolute('https://nextdoor.com/')
    return doc


async def get_links(p, url, page):
    doc = await get_doc(p, url, page)
    return doc.xpath("//a[@class='link']")


async def download_url(p, url, page) -> Path:
    path = get_path(url, page)
    if path.exists():
        return path
    nav = Navigator(p, headless=True)
    await nav.start()
    await nav.goto(url)
    page = nav.page()
    content = await page.inner_html('//html')
    with open(path, 'w', encoding="utf-8") as f:
        f.write(content)
    await nav.exit()
    return path


async def fetch_cities_inside_state(state_task_name, states_queue):
    """ gets all cities inside the given state"""
    _lg = get_logger(state_task_name)
    async with async_playwright() as p:
        _lg.info('started playwright')
        while True:
            url, txt, cities_queue, neighbours_queue = await states_queue.get()
            links = await get_links(p, url, txt)
            for link in links:
                _lg.debug(f"state: {txt}, city: {link.text}")
                cities_queue.put_nowait([
                    link.text,
                    link.attrib['href'],
                    neighbours_queue,
                    {'state': txt}
                ])
            states_queue.task_done()


async def fetch_neighbors_inside_city(name, cities_queue):
    """ fetch details of the city """

    _lg = get_logger(name)
    async with async_playwright() as p:
        _lg.info('started playwright')
        while True:
            name, url, neighbours_queue, data = await cities_queue.get()
            doc = await get_doc(p, url, name)
            links = doc.xpath('//h2[contains(text(),"Nearby neighborhoods")]/parent::div/following-sibling::div//a')
            for link in links:
                new_data = deepcopy(data)
                new_data['city'] = name
                _lg.debug(f"{new_data}, neighborhood: {link.text}")
                neighbours_queue.put_nowait([
                    link.text,
                    link.attrib['href'],
                    new_data
                ])
            cities_queue.task_done()


async def fetch_neighbours_details(name, neighbors_queue):
    """ now fetch details about the neighbors """
    _lg = get_logger(name)
    count = 0
    all_data = []
    async with async_playwright() as p:
        _lg.info('started playwright')
        while True:
            name, url, data = await neighbors_queue.get()
            doc = await get_doc(p, url, name)
            data['neighborhood'] = name
            data['url'] = url
            try:
                residents = doc.xpath('//span[contains(text(), "Residents")]')[0].getprevious()
                homeowners = doc.xpath('//span[contains(text(), "Homeowners")]')[0].getprevious()
                data['residents'] = residents.text
                data['homeowners'] = homeowners.text
                _lg.debug(f"{data}")
            except IndexError:
                data['residents'] = 'N/A'
                data['homeowners'] = 'N/A'
            all_data.append(data)
            count += 1
            if count > 100:
                break
            neighbors_queue.task_done()
    _lg.info("Writing to file ...")
    with open('data.csv', "w") as out:
        writer = csv.DictWriter(out, fieldnames=["state", "city", "neighborhood", "residents", "homeowners", "url"])
        writer.writeheader()
        for d in all_data:
            writer.writerow(d)


async def main():
    logger = get_logger("start-up")
    logger.info("Starting")
    states_queue = asyncio.Queue()
    cities_queue = asyncio.Queue()
    neighbours_queue = asyncio.Queue()

    CFG.cache_dir().mkdir(exist_ok=True, parents=True)
    async with asyncio.TaskGroup() as tg:
        # create tasks
        for k in range(4):
            tg.create_task(fetch_cities_inside_state(f"state-task-{k}", states_queue))
            tg.create_task(fetch_neighbors_inside_city(f"city-task-{k}", cities_queue))
            tg.create_task(fetch_neighbours_details(f"neighbours-task-{k}", neighbours_queue))
        # now starts processing
        async with async_playwright() as p:
            logger.info('started playwright')
            links = await get_links(p, "https://nextdoor.com/find-neighborhood/", "index")
            for link in links:
                states_queue.put_nowait([
                    link.attrib['href'],
                    link.text,
                    cities_queue,
                    neighbours_queue
                ])

        await states_queue.join()
        await cities_queue.join()
        logger.debug("done processing all list items")
    logger.debug("closed task groups, bye")


CFG = ConfigDict()

if __name__ == '__main__':
    CFG.cache_dir().mkdir(exist_ok=True, parents=True)
    asyncio.run(main())
