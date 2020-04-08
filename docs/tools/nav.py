import collections
import logging
import os

import util


def find_first_header(content):
    for line in content.split('\n'):
        if line.startswith('#'):
            no_hash = line.lstrip('#')
            return no_hash.split('{', 1)[0].strip()


def build_nav_entry(root):
    if root.endswith('images'):
        return None, None, None
    result_items = []
    index_meta, _ = util.read_md_file(os.path.join(root, 'index.md'))
    current_title = index_meta.get('toc_folder_title', index_meta.get('toc_title', 'hidden'))
    for filename in os.listdir(root):
        path = os.path.join(root, filename)
        if os.path.isdir(path):
            prio, title, payload = build_nav_entry(path)
            if title and payload:
                result_items.append((prio, title, payload))
        elif filename.endswith('.md'):
            path = os.path.join(root, filename)
            meta, content = util.read_md_file(path)
            path = path.split('/', 2)[-1]
            title = meta.get('toc_title', find_first_header(content))
            if title:
                title = title.strip().rstrip('.')
            else:
                title = meta.get('toc_folder_title', 'hidden')
            prio = meta.get('toc_priority', 9999)
            logging.debug(f'Nav entry: {prio}, {title}, {path}')
            result_items.append((prio, title, path))
    result_items = sorted(result_items, key=lambda x: (x[0], x[1]))
    result = collections.OrderedDict([(item[1], item[2]) for item in result_items])
    return index_meta.get('toc_priority', 10000), current_title, result


def build_nav(lang, args):
    docs_dir = os.path.join(args.docs_dir, lang)
    _, _, nav = build_nav_entry(docs_dir)
    result = []
    for key, value in nav.items():
        result.append({key: value})
    return result
