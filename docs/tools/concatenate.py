# -*- coding: utf-8 -*-

import logging
import re
import os


def concatenate(lang, docs_path, single_page_file):
    proj_config = os.path.join(docs_path, 'toc_%s.yml' % lang)
    lang_path = os.path.join(docs_path, lang)
    az_re = re.compile(r'[a-z]')

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

    for path in files_to_concatenate:
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
                        single_page_file.write('<a name="%s"></a>\n' % anchor)
    
                single_page_file.write('\n\n')
    
                for l in f:
                    if l.startswith('#'):
                        l = '#' + l
                    single_page_file.write(l)
        except IOError as e:
            logging.warning(str(e))

    single_page_file.flush()
