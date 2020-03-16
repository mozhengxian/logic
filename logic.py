#! /usr/bin/env python
# -*-encoding:utf-8-*-
"""Usage:
  logic 指定间隔读取csv文件, 默认60
"""
import os
from collections import deque
import pandas as pd
from multiprocessing import Pool, Manager


def logic(path=None, index=60):
    dir_path = os.listdir(path)
    name = [i.split('.')[0] for i in dir_path]
    file = [os.path.join(path, i) for i in dir_path]
    p = Pool(4)
    q = Manager().Queue()
    for i, d in enumerate(file):
        p.apply_async(read_file, args=(d, index, q, name[i]))
    p.close()
    p.join()
    if not os.path.exists('data'):
        os.makedirs('data')
        os.chdir('data')
        while not q.empty():
            df, name = q.get()
            df.to_csv(f'{name}.csv', encoding='gbk', index=None)
            print(f'{name}.csv 写入完成!')
    else:
        os.chdir('data')
        while not q.empty():
            df, name = q.get()
            df.to_csv(f'{name}.csv', encoding='gbk', index=None)
            print(f'{name}.csv 写入完成!')


def read_file(path, index, queue, name):
    lists = deque()
    print(f'开始读取 {name}')
    for chuck in pd.read_csv(path, chunksize=5000, encoding='gbk',
                             skiprows=lambda x: index_num(x, index)):
        lists.append(chuck)
    lists = pd.concat(lists, axis=0)
    queue.put((lists, name))
    print(f'{name} 读取完毕!')


def index_num(index, x):
    if index % x != 0:
        return True
    return False


if __name__ == '__main__':
    file_path = input('input file path: ')
    csv_index = int(input('input index: '))
    logic(file_path, csv_index)
