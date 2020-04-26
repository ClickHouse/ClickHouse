#!/usr/bin/env python3

import sys

import util

if __name__ == '__main__':
    flag_name = sys.argv[1]
    path = sys.argv[2]
    meta, content = util.read_md_file(path)
    meta[flag_name] = True
    util.write_md_file(path, meta, content)
