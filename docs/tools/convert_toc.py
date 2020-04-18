#!/usr/bin/env python3

import os
import sys
import subprocess

import yaml

import util

lang = 'ru'
base_dir = os.path.join(os.path.dirname(__file__), '..')
en_dir = os.path.join(base_dir, 'en')
docs_dir = os.path.join(base_dir, lang)
redirects_file = os.path.join(base_dir, 'redirects.txt')
redirects = {}


def make_key(path):
    path = path.lower().replace(' ', '_').replace('c++', 'cpp')
    if path.startswith('clickhouse_'):
        path = path.replace('clickhouse_', '', 1)
    if path != 'data_types' and path.startswith('data_'):
        path = path.replace('data_', '', 1)
    to_remove = [
        '\'', '"', '.', ',', '(', ')',
        'how_to_', '_of_clickhouse'
    ]
    for token in to_remove:
        path = path.replace(token, '')
    return path


def process_md_file(title, idx, original_path, proper_path):
    proper_md_path = '/'.join(proper_path + [original_path.rsplit('/', 1)[-1]])
    if proper_md_path == 'introduction/index.md':
        proper_md_path = 'index.md'
    print(locals())
    if original_path != proper_md_path:
        redirects[original_path] = proper_md_path
    original_path = os.path.join(docs_dir, original_path)
    proper_md_path = os.path.join(docs_dir, proper_md_path)
    if os.path.exists(original_path):
        meta, content = util.read_md_file(original_path)
    else:
        meta, content = util.read_md_file(proper_md_path)
    meta['toc_title'] = title
    meta['toc_priority'] = idx
    if title == 'hidden':
        meta['toc_hidden'] = True

    for src, dst in redirects.items():
        content = content.replace('(' + src, '(' + dst)
        content = content.replace('../' + src, '../' + dst)
    
    util.write_md_file(proper_md_path, meta, content)
    if original_path != proper_md_path:
        subprocess.check_call(f'git add {proper_md_path}', shell=True)
        if os.path.exists(original_path):
            subprocess.check_call(f'rm {original_path}', shell=True)


def process_toc_entry(entry, path, idx):
    if isinstance(entry, list):
        for e in entry:
            idx = process_toc_entry(e, path, idx)
    elif isinstance(entry, dict):
        for key, value in entry.items():
            next_path = path + [make_key(key)]
            index_md_idx = idx
            idx += 1

            if isinstance(value, list):
                for v in value:
                    process_toc_entry(v, next_path, idx)
                    idx += 1
            else:
                process_md_file(key, idx, value, path)
                idx += 1

            index_md_path = os.path.join(docs_dir, '/'.join(next_path), 'index.md')
            if os.path.exists(os.path.dirname(index_md_path)):
                index_meta, index_content = util.read_md_file(index_md_path)
                if not index_meta.get('toc_folder_title'):
                    index_meta['toc_folder_title'] = key
                index_meta['toc_priority'] = index_md_idx
                util.write_md_file(index_md_path, index_meta, index_content)
                subprocess.check_call(f'git add {index_md_path}', shell=True)
    return idx


def process_toc_yaml(path):
    with util.cd(docs_dir):
        init_redirects()
        with open(path, 'r') as f:
            data = yaml.full_load(f.read())
        process_toc_entry(data['nav'], [], 1)
        update_redirects()


def init_redirects():
    with open(redirects_file, 'r') as f:
        for line in f:
            src, dst = line.strip().split(' ', 1)
            redirects[src] = dst


def update_redirects():
    with open(redirects_file, 'w') as f:
        for src, dst in sorted(redirects.items()):
            print(f'{src} {dst}', file=f)


def sync_translation():
    init_redirects()
    for src, dst in redirects.items():
        en_src = os.path.join(en_dir, src)
        lang_src = os.path.join(docs_dir, src)
        lang_dst = os.path.join(docs_dir, dst)
        if os.path.exists(lang_src):
            if os.path.islink(lang_src):
                pass
            else:
                en_meta, en_content = util.read_md_file(en_src)
                lang_meta, lang_content = util.read_md_file(lang_src)
                en_meta.update(lang_meta)

                for src_link, dst_link in redirects.items():
                    lang_content = lang_content.replace('(' + src_link, '(' + dst)
                    lang_content = lang_content.replace('../' + src_link, '../' + dst)
                    
                util.write_md_file(lang_dst, en_meta, lang_content)
                subprocess.check_call(f'git add {lang_dst}', shell=True)
                subprocess.check_call(f'rm {lang_src}', shell=True)


if __name__ == '__main__':
    sync_translation()
    # if len(sys.argv) == 1:
    #     process_toc_yaml(os.path.join(base_dir, f'toc_{lang}.yml'))
    # else:
    #     process_toc_yaml(sys.argv[1])
