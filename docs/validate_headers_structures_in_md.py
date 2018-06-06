#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Gets all the files in SOURCES_TREE directory, shows all level headers
# for each file and skip or process files by user's selection.

import os

SOURCES_TREE = 'ru'
STOP_AT_THE_FIRST_FILE = False

for (dirpath, dirnames, filenames) in os.walk(SOURCES_TREE):
    for filename in filenames:
        if filename == 'conf.py':
            continue

        print '=== ' + dirpath + '/' + filename

        f = open(dirpath + '/' + filename)
        content = f.readlines()
        f.close()

        # Showing headers structure in md-file
        count_lines = 0
        for l in content:
            if l.startswith('#'):
                print l
            if l.startswith('==='):
                print content[count_lines - 1] + l
            if l.startswith('---'):
                print content[count_lines - 1] + l
            count_lines += 1

        # At this stage user should check the headers structucture and choose what to to
        # Replace headers markup or not
        choise = raw_input('What to do with a file (pass(s) or process(p)): ')

        if choise == 's':
            continue
        else:
            print 'processing...'
            count_lines = 0
            for l in content:
                if l.startswith('==='):
                    print count_lines, content[count_lines - 1], content[count_lines]
                    content[count_lines - 1] = '# ' + content[count_lines - 1]
                    content.pop(count_lines)

                if l.startswith('---'):
                    print count_lines, content[count_lines - 1], content[count_lines]
                    content[count_lines - 1] = '## ' + content[count_lines - 1]
                    content.pop(count_lines)

                count_lines += 1

        f = open(dirpath + '/' + filename, 'w')
        for l in content:
            f.write(l)
        f.close()

        if STOP_AT_THE_FIRST_FILE:
            break

    if STOP_AT_THE_FIRST_FILE:
        break
