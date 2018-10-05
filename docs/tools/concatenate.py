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

        single_page_file.write('\n\n')

        with open(os.path.join(lang_path, path)) as f:

            # function is passed into re.sub() to process links
            def link_proc(matchObj):
                text, link = matchObj.group().strip('[)').split('](')
                if link.startswith('http'):
                    return '[' + text + '](' + link + ')'
                else:
                    sharp_pos = link.find('#')
                    if sharp_pos > -1:
                        return '[' + text + '](' + link[sharp_pos:] + ')'
                    else:
                        raise RuntimeError(
                            'ERROR: Link [' + text + '](' + link + ') in file ' +
                            path + ' has no anchor. Please provide it.')

            for l in f:
                # Processing links in a string
                l = re.sub(r'\[.+?\]\(.+?\)', link_proc, l)

                # Correcting headers levels
                if not first_file:
                    if l.startswith('#'):
                        l = '#' + l
                else:
                    first_file = False

                single_page_file.write(l)

    single_page_file.flush()
