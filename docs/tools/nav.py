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
    index_meta, index_content = util.read_md_file(os.path.join(root, 'index.md'))
    current_title = index_meta.get('toc_folder_title', index_meta.get('toc_title', find_first_header(index_content)))
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
            if not content.strip():
                title = 'hidden'
            result_items.append((prio, title, path))
    result_items = sorted(result_items, key=lambda x: (x[0], x[1]))
    result = collections.OrderedDict([(item[1], item[2]) for item in result_items])
    return index_meta.get('toc_priority', 10000), current_title, result


def build_nav(lang, args):
    docs_dir = os.path.join(args.docs_dir, lang)
    _, _, nav = build_nav_entry(docs_dir)
    result = []
    index_key = None
    for key, value in nav.items():
        if key and value:
            if value == 'index.md':
                index_key = key
                continue
            result.append({key: value})
    if index_key:
        key = list(result[0].keys())[0]
        result[0][key][index_key] = 'index.md'
        result[0][key].move_to_end(index_key, last=False)
    print('result', result)
    return result
