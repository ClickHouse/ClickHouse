#!/usr/bin/env python
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


import codecs
import sys
import re
import os

if len(sys.argv) < 2:
    print "Usage: concatenate.py language_dir"
    print "Example: concatenate.py ru"
    sys.exit(1)

if not os.path.exists(sys.argv[1]):
    print "Pass language_dir correctly. For example, 'ru'."
    sys.exit(2)

# Configuration
PROJ_CONFIG = 'mkdocs_' + sys.argv[1] + '.yml'
SINGLE_PAGE = sys.argv[1] + '_single_page/index.md'
DOCS_DIR = sys.argv[1] + '/'

# 1. Open mkdocs.yml file and read `pages` configuration to get an ordered list of files
cfg_file = open(PROJ_CONFIG)

files_to_concatenate = []

for l in cfg_file:
    if('.md' in l) and ('single_page' not in l):
        path = (l[l.index(':') + 1:]).strip(" '\n")
        files_to_concatenate.append(path)

print str(len(files_to_concatenate)) + " files will be concatenated into single md-file.\nFiles:"
print files_to_concatenate

# 2. Concatenate all of the files in the list

single_page_file = open(SINGLE_PAGE, 'w')

first_file = True

for path in files_to_concatenate:

    single_page_file.write('\n\n')

    file = open(DOCS_DIR + path)

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
                print 'ERROR: Link [' + text + '](' + link + ') in file ' + path + ' has no anchor. Please provide it.'
                # return '['+text+'](#'+link.replace('/','-')+')'

    for l in file:
        # Processing links in a string
        l = re.sub(r'\[.+?\]\(.+?\)', link_proc, l)

        # Correcting headers levels
        if not first_file:
            if(l.startswith('#')):
                l = '#' + l
        else:
            first_file = False

        single_page_file.write(l)

single_page_file.close()
