#!/usr/bin/env python3
import os
import subprocess
import sys

import util


if __name__ == '__main__':
    path = sys.argv[1]
    content_path = f'{path}.content'
    meta_path = f'{path}.meta'
    meta, content = util.read_md_file(path)
    
    target_language = os.getenv('TARGET_LANGUAGE')
    if target_language is not None and target_language != 'en':
        rev = subprocess.check_output(
            'git rev-parse HEAD', shell=True
        ).decode('utf-8').strip()
        meta['machine_translated'] = True
        meta['machine_translated_rev'] = rev
    
    with open(content_path, 'w') as f:
        print(content, file=f)

    util.write_md_file(meta_path, meta, '')
