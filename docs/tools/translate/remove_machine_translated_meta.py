#!/usr/bin/env python3
import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
import convert_toc
import util


if __name__ == '__main__':
    path = sys.argv[1][2:]
    convert_toc.init_redirects()
    try:
        path = convert_toc.redirects[path]
    except KeyError:
        pass
    meta, content = util.read_md_file(path)
    if 'machine_translated' in meta:
        del meta['machine_translated']
    if 'machine_translated_rev' in meta:
        del meta['machine_translated_rev']
    util.write_md_file(path, meta, content)
