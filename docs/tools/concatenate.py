# -*- coding: utf-8 -*-

import logging
import re
import os

import yaml


def recursive_values(item):
    if isinstance(item, dict):
        for _, value in item.items():
            yield from recursive_values(value)
    elif isinstance(item, list):
        for value in item:
            yield from recursive_values(value)
    elif isinstance(item, str):
        yield item


def concatenate(lang, docs_path, single_page_file, nav):
    lang_path = os.path.join(docs_path, lang)
    az_re = re.compile(r'[a-z]')

    proj_config = f'{docs_path}/toc_{lang}.yml'
    if os.path.exists(proj_config):
        with open(proj_config) as cfg_file:
            nav = yaml.full_load(cfg_file.read())['nav']
    files_to_concatenate = list(recursive_values(nav))
    files_count = len(files_to_concatenate)
    logging.info(f'{files_count} files will be concatenated into single md-file for {lang}.')
    logging.debug('Concatenating: ' + ', '.join(files_to_concatenate))
    assert files_count > 0, f'Empty single-page for {lang}'

    for path in files_to_concatenate:
        if path.endswith('introduction/info.md'):
            continue
        try:
            with open(os.path.join(lang_path, path)) as f:
                anchors = set()
                tmp_path = path.replace('/index.md', '/').replace('.md', '/')
                prefixes = ['', '../', '../../', '../../../']
                parts = tmp_path.split('/')
                anchors.add(parts[-2] + '/')
                anchors.add('/'.join(parts[1:]))

                for part in parts[0:-2] if len(parts) > 2 else parts:
                    for prefix in prefixes:
                        anchor = prefix + tmp_path
                        if anchor:
                            anchors.add(anchor)
                            anchors.add('../' + anchor)
                            anchors.add('../../' + anchor)
                    tmp_path = tmp_path.replace(part, '..')

                for anchor in anchors:
                    if re.search(az_re, anchor):
                        single_page_file.write('<a name="%s"></a>' % anchor)

                single_page_file.write('\n')

                in_metadata = False
                for l in f:
                    if l.startswith('---'):
                        in_metadata = not in_metadata
                    if l.startswith('#'):
                        l = '#' + l
                    if not in_metadata:
                        single_page_file.write(l)
        except IOError as e:
            logging.warning(str(e))

    single_page_file.flush()
