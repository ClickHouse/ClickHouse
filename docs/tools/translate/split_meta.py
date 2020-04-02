#!/usr/bin/env python3
import sys

import util


if __name__ == '__main__':
    path = sys.argv[1]
    content_path = f'{path}.content'
    meta_path = f'{path}.meta'
    meta, content = util.read_md_file(path)
    
    with open(content_path, 'w') as f:
        print(content, file=f)

    util.write_md_file(meta_path, meta, '')
