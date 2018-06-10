#!/usr/bin/env python
# -*- coding: utf-8 -*-

SOURCES_TREE = 'ru'
from os import walk


def get_header(filepath):
    f = open(filepath)
    header = ''
    for line in f:
        if line.startswith('#'):
            #            print line
            header = line[1:].strip(' \n')
            break

    f.close()
    return header


pages_file = open("strings_for_pages.txt", "w")
md_links_file = open("links_for_md.txt", "w")

for (dirpath, dirnames, filenames) in walk(SOURCES_TREE):
    for filename in filenames:

        if '.md' not in filename:
            continue

        header = get_header(dirpath + '/' + filename)
        path = dirpath.replace('docs/', '') + '/' + filename

        if filename == 'index.md':
            pages_file.write("- '" + header + "': " + "'" + path + "'\n")
        else:
            pages_file.write("  - '" + header + "': " + "'" + path + "'\n")

        md_links_file.write("[" + header + "](" + path + ")\n")

pages_file.close()
md_links_file.close()
