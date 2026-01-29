"""
Crawler module for Multimodal Search Engine
- Text + Image crawling
- Pluggable sources
- Async, rate-limited
- Metadata-first design
"""

import asyncio
import aiohttp
import hashlib
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from pathlib import Path
from src.constants import USER_AGENT, TIMEOUT, IMAGE_EXTENSIONS,MAX_CONCURRENT,base_urls
from src.exception import MyException
from src.logger import logger
import sys




def url_hash(url: str) -> str:
    logger.debug("Entered the url_has method")
    try:
        return hashlib.sha256(url.encode()).hexdigest() #converts url into 64 byte fix length so it is fast to compare
    except Exception as e:
        raise MyException(e,sys)


def is_image(url: str) -> bool:
    logger.debug("Entered the method is image")
    try:
        return Path(urlparse(url).path).suffix.lower() in IMAGE_EXTENSIONS
    except Exception as e:
        raise MyException(e,sys)


class WebCrawler:
    def __init__(self, base_urls: list[str], output_dir="data/raw"):
        self.base_urls = base_urls
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.visited = set()
        self.sem = asyncio.Semaphore(MAX_CONCURRENT)
        

    async def fetch(self, session: aiohttp.ClientSession, url: str):
        logger.debug("Entered teh method fetch of class WebCrawler")
        async with self.sem: #concept of semaphore to liimit the maximum requests in event loop

            try:
                logger.debug(f"Fetching URL: {url}")
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        return await resp.text(), resp.headers.get("Content-Type", "")
                    else:
                        logger.debug(f"Non-200 status {resp.status} for {url}")
            except asyncio.TimeoutError as e:
                raise MyException(e,sys)
            except aiohttp.ClientError as e:
                raise MyException(e,sys)
            except Exception as e:
                raise MyException(e,sys)
        return None, None

    async def download_image(self, session, url):
        logger.debug("Entered download_image method of class WebCrawller")
        async with self.sem:
            try:
                logger.debug(f"Downloading image: {url}")
                async with session.get(url, timeout=TIMEOUT) as resp:
                    if resp.status == 200:
                        img_bytes = await resp.read()
                        fname = url_hash(url) + Path(url).suffix
                        path = self.output_dir / "images" / fname
                        path.parent.mkdir(parents=True,exist_ok=True)
                        path.write_bytes(img_bytes)
                        return {
                            "url": url,
                            "path": str(path),
                        }
                    else:
                        logger.debug(f"Image download failed ({resp.status}) for {url}")
            except asyncio.TimeoutError as e:
                raise MyException(e,sys)
                
            except aiohttp.ClientError as e:
                raise MyException(e,sys)
            except Exception as e:
                raise MyException(e,sys)
        return None

    async def crawl_page(self, session, url):
        
        logger.debug("Entered crawl_pages method of class WebCrawller")
        if url in self.visited:
            logger.debug(f"Already visited: {url}")
            return None, []

        self.visited.add(url)
        logger.debug(f"Crawling page: {url}")

        try:
            html, content_type = await self.fetch(session, url)
            if not html or "text/html" not in content_type:
                logger.debug(f"Skipping non-HTML or empty content: {url}")
                return None, []

            soup = BeautifulSoup(html, "html.parser")

            
            text = soup.get_text(" ", strip=True)
            text_meta = {
                "url": url,
                "text": text,
                "type": "text",
            }

        
            images = []
            for img in soup.find_all("img"):
                src = img.get("src")
                if not src:
                    continue
                img_url = urljoin(url, src)
                if is_image(img_url):
                    images.append(img_url)

            return text_meta, images

        except Exception as e:
            raise MyException(e,sys)
            return None, []

    async def run(self):
        logger.debug("Entered run method of class webcrwller")
        try:
            headers = {"User-Agent": USER_AGENT}
            
            async with aiohttp.ClientSession(headers=headers) as session:
                tasks = [self.crawl_page(session, url) for url in self.base_urls]
                pages = await asyncio.gather(*tasks)

                all_images = []
                all_text = []

                for text_meta, images in pages:
                    if text_meta:
                        all_text.append(text_meta)
                    all_images.extend(images)

                img_tasks = [self.download_image(session, u) for u in all_images]
                img_meta = await asyncio.gather(*img_tasks)

            return all_text, [m for m in img_meta if m]
        except Exception as e:
            raise MyException(e,sys)



if __name__ == "__main__":
    seed_urls = ["https://en.wikipedia.org/wiki/Web_crawler"]
    crawler = WebCrawler(seed_urls)
    text_data, image_data = asyncio.run(crawler.run())
    print(f"Text docs: {len(text_data)}, Images: {len(image_data)}")
