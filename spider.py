from parser import HtmlParser
from queue import Queue
from threading import Thread, Timer
from time import sleep, time
import json, os, fire, logging

class Spider(object):
    def __init__(self, worker_num=10, chunk_size=10000, log_interval=600,
                 data_dir='data', log_dir='log'):
        self.chunk_size = chunk_size
        self.log_interval = log_interval
        self.urls = Queue()
        self.results = Queue()
        self.url_cache = set()
        self.name_cache = set()
        self.chunk_num = 0
        self.parser = HtmlParser(home='https://baike.baidu.com')

        self.last = 0
        self.state = 1

        if not os.path.exists(data_dir):
            os.mkdir(data_dir)
        if not os.path.exists(log_dir):
            os.mkdir(log_dir)
        self.data_dir = data_dir
        self.log_dir = log_dir

        self.writer = Thread(target=self._write)
        self.logger = Timer(log_interval, self._log)
        self.spiders = [Thread(target=self._scrap) for _ in range(worker_num)]


    def start(self, url):
        new_urls, new_data = self.parser.parse(url)
        self.results.put(new_data)
        self.url_cache.add(url)
        self.name_cache.add(new_data['name'])
        for url in new_urls:
            self.urls.put(url)
        
        self.logger.start()
        self.writer.start()
        for spider in self.spiders:
            spider.start()
        
    def _write(self):
        """只使用self.results
        """
        while self.state:
            self.chunk_num += 1
            n = 0
            with open(os.path.join(self.data_dir, '{}.json'.format(self.chunk_num)), 'wb') as fp:
                while n < self.chunk_size:
                    if not self.results.empty():
                        result = self.results.get()
                        line = json.dumps(result, ensure_ascii=False) + '\n'
                        fp.write(line.encode('utf8'))
                        n += 1
                    else:
                        sleep(10)

    def _log(self):
        now = len(self.name_cache)
        increase = now - self.last
        self.last = now
        if increase == 0:
            self.state = 0
            logging.info('Exit: no entities scraped in this round.')
            exit()
        else:
            with open(os.path.join(self.log_dir, 'log'), 'ab+') as fp:
                message = '新增词条数量：{}，已抓取词条数量：{}；缓存任务数量：{}，缓存结果数量：{}.'.format(
                    increase, now, self.urls._qsize(), self.results._qsize(),
                ) + '\n'
                fp.write(message.encode('utf8'))
        timer = Timer(self.log_interval, self._log)
        timer.start() 

    def _scrap(self):
        while self.state:
            if not self.urls.empty():
                url = self.urls.get()
                try:
                    new_urls, new_data = self.parser.parse(url)
                except:
                    sleep(1)
                    self.urls.put(url)
                    continue
                name = new_data['name']
                if name not in self.name_cache:
                    self.name_cache.add(name)
                    self.url_cache.add(url)
                    if new_data['infomation']: #剔除没有属性信息的词条
                        self.results.put(new_data)
                for url in new_urls:
                    if url not in self.url_cache:
                        self.urls.put(url)
            else:
                sleep(10)


def main(worker_num=20,
         chunk_size=10000,
         log_interval=600,
         data_dir='data',
         log_dir='log',
         start_url='https://baike.baidu.com/item/%E5%A7%9A%E6%98%8E/28'):
    
    spider = Spider(
        worker_num=worker_num,
        chunk_size=chunk_size,
        log_interval=log_interval,
        data_dir=data_dir,
        log_dir=log_dir,
    )
    spider.start(start_url)


if __name__ == '__main__':
    fire.Fire(main)
