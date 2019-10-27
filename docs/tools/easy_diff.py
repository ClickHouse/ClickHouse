#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys
import subprocess
from git import cmd
from tempfile import NamedTemporaryFile

SCRIPT_PATH = os.path.abspath(__file__)
CLICK_HOUSE_REPO_HOME = os.path.join(os.path.dirname(SCRIPT_PATH), '../../')


def diffFile(reference_file, working_file, git, temp_diff):
    if os.path.islink(working_file):
        print "Need translate document:" + reference_file

    if not os.path.exists(working_file):
        print 'Need link document ' + reference_file + ' to ' + working_file

    if os.path.exists(working_file) and not os.path.islink(working_file):
        git_hash = git.execute(['git', 'log', '-n', '1', '--pretty=format:"%H"', working_file])
        temp_diff.write(git.execute(['git', 'diff', git_hash.strip('"'), reference_file]).encode('utf-8'))
        temp_diff.write('\n'.encode('utf-8'))

    return 0


def diffDirectory(reference_directory, working_directory, git, temp_diff):
    if not os.path.isdir(reference_directory):
        raise RuntimeError('The [' + reference_directory + '] is not directory.')

    for list_item in os.listdir(reference_directory):
        working_item = os.path.join(working_directory, list_item)
        reference_item = os.path.join(reference_directory, list_item)
        if diffFile(reference_item, working_item, git, temp_diff) if os.path.isfile(reference_item) else diffDirectory(reference_item, working_item, git, temp_diff) != 0:
            return 1

    return 0


def findLanguageDoc(custom_document, other_language='en', children=[]):
    if len(custom_document) == 0:
        raise RuntimeError('The ' + os.path.join(custom_document, *children) + " is not in docs directory.")

    if os.path.samefile(os.path.join(CLICK_HOUSE_REPO_HOME, 'docs'), custom_document):
        return os.path.join(CLICK_HOUSE_REPO_HOME, 'docs', other_language, *children[1:])
    children.insert(0, os.path.split(custom_document)[1])
    return findLanguageDoc(os.path.split(custom_document)[0], other_language, children)


if __name__ == '__main__':
    git = cmd.Git(CLICK_HOUSE_REPO_HOME)
    git_pager = git.execute(['git', 'var', 'GIT_PAGER'])
    working_language = os.path.join(CLICK_HOUSE_REPO_HOME, 'docs', sys.argv[1])

    reference_language = findLanguageDoc(working_language)
    with NamedTemporaryFile(mode='r+') as temp_diff:
        if not os.path.isdir(reference_language):
            diffFile(reference_language, working_language, git, temp_diff)
        else:
            diffDirectory(reference_language, working_language, git, temp_diff)

        temp_diff.flush()
        subprocess.check_call([git_pager, temp_diff.name])
