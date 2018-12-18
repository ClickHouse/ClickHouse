# -*- coding: utf-8 -*-

# - Single-page document.
#   - Requirements to the md-souces:
#       - Don't use links without anchors. It means, that you can not just link file. You should specify an anchor at the top of the file and then link to this anchor
#       - Anchors should be unique through whole document.
#   - Implementation:
#       - Script gets list of the file from the `pages` section of `mkdocs.yml`. It gets commented files too, and it right.
#       - Files are concatenated by order with incrementing level of headers in all files except the first one
#       - Script converts links to other files into inside page links.
#         - Skipping links started with 'http'
#         - Not http-links with anchor are cutted to the anchor sign (#).
#         - For not http-links without anchor script logs an error and cuts them from the resulting single-page document.

import logging
import re
import os


def concatenate(lang, docs_path, single_page_file):

    proj_config = os.path.join(docs_path, 'toc_%s.yml' % lang)
    lang_path = os.path.join(docs_path, lang)

    with open(proj_config) as cfg_file:
        files_to_concatenate = []
        for l in cfg_file:
            if '.md' in l and 'single_page' not in l:
                path = (l[l.index(':') + 1:]).strip(" '\n")
                files_to_concatenate.append(path)

    logging.info(
        str(len(files_to_concatenate)) +
        ' files will be concatenated into single md-file.')
    logging.debug('Concatenating: ' + ', '.join(files_to_concatenate))

    first_file = True

    for path in files_to_concatenate:
        with open(os.path.join(lang_path, path)) as f:
            anchors = set()
            tmp_path = path.replace('/index.md', '/').replace('.md', '/')
            prefixes = ['', '../', '../../', '../../../']
            parts = tmp_path.split('/')
            anchors.add(parts[-2] + '/')
            anchors.add('/'.join(parts[1:]))

            for part in parts[0:-2]:
                for prefix in prefixes:
                    anchors.add(prefix + tmp_path)
                tmp_path = tmp_path.replace(part, '..')

            for anchor in anchors:
                single_page_file.write('<a name="%s"></a>\n' % anchor)

            single_page_file.write('\n\n')

            for l in f:
                if l.startswith('#'):
                    l = '#' + l
                single_page_file.write(l)

    single_page_file.flush()
