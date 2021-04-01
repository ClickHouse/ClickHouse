#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, sys
import argparse
import subprocess
import contextlib
from git import cmd
from tempfile import NamedTemporaryFile

SCRIPT_DESCRIPTION = '''
    usage: ./easy_diff.py language/document path

    Show the difference between a language document and an English document.

    This script is based on the assumption that documents in other languages are fully synchronized with the en document at a commit.

    For example:
        Execute:
            ./easy_diff.py --no-pager zh/data_types
        Output:
            Need translate document:~/ClickHouse/docs/en/data_types/uuid.md
            Need link document:~/ClickHouse/docs/en/data_types/decimal.md to ~/ClickHouse/docs/zh/data_types/decimal.md
            diff --git a/docs/en/data_types/domains/ipv6.md b/docs/en/data_types/domains/ipv6.md
            index 1bfbe3400b..e2abaff017 100644
            --- a/docs/en/data_types/domains/ipv6.md
            +++ b/docs/en/data_types/domains/ipv6.md
            @@ -4,13 +4,13 @@

             ### Basic Usage

            -``` sql
            +```sql
             CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY url;

             DESCRIBE TABLE hits;
             ```

            -```
            +```text
             ┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
             │ url  │ String │              │                    │         │                  │
             │ from │ IPv6   │              │                    │         │                  │
            @@ -19,19 +19,19 @@ DESCRIBE TABLE hits;

             OR you can use `IPv6` domain as a key:

            -``` sql
            +```sql
             CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
            ... MORE

    OPTIONS:
        -h, --help  show this help message and exit
        --no-pager  use stdout as difference result output
'''

SCRIPT_PATH = os.path.abspath(__file__)
CLICKHOUSE_REPO_HOME = os.path.join(os.path.dirname(SCRIPT_PATH), '..', '..')
SCRIPT_COMMAND_EXECUTOR = cmd.Git(CLICKHOUSE_REPO_HOME)

SCRIPT_COMMAND_PARSER = argparse.ArgumentParser(add_help=False)
SCRIPT_COMMAND_PARSER.add_argument('path', type=bytes, nargs='?', default=None)
SCRIPT_COMMAND_PARSER.add_argument('--no-pager', action='store_true', default=False)
SCRIPT_COMMAND_PARSER.add_argument('-h', '--help', action='store_true', default=False)


def execute(commands):
    return SCRIPT_COMMAND_EXECUTOR.execute(commands)


def get_hash(file_name):
    return execute(['git', 'log', '-n', '1', '--pretty=format:"%H"', file_name])


def diff_file(reference_file, working_file, out):
    if not os.path.exists(reference_file):
        raise RuntimeError('reference file [' + os.path.abspath(reference_file) + '] is not exists.')

    if os.path.islink(working_file):
        out.writelines(["Need translate document:" + os.path.abspath(reference_file)])
    elif not os.path.exists(working_file):
        out.writelines(['Need link document ' + os.path.abspath(reference_file) + ' to ' + os.path.abspath(working_file)])
    elif get_hash(working_file) != get_hash(reference_file):
        out.writelines([(execute(['git', 'diff', get_hash(working_file).strip('"'), reference_file]).encode('utf-8'))])

    return 0


def diff_directory(reference_directory, working_directory, out):
    if not os.path.isdir(reference_directory):
        return diff_file(reference_directory, working_directory, out)

    for list_item in os.listdir(reference_directory):
        working_item = os.path.join(working_directory, list_item)
        reference_item = os.path.join(reference_directory, list_item)
        if diff_file(reference_item, working_item, out) if os.path.isfile(reference_item) else diff_directory(reference_item, working_item, out) != 0:
            return 1

    return 0


def find_language_doc(custom_document, other_language='en', children=[]):
    if len(custom_document) == 0:
        raise RuntimeError('The ' + os.path.join(custom_document, *children) + " is not in docs directory.")

    if os.path.samefile(os.path.join(CLICKHOUSE_REPO_HOME, 'docs'), custom_document):
        return os.path.join(CLICKHOUSE_REPO_HOME, 'docs', other_language, *children[1:])
    children.insert(0, os.path.split(custom_document)[1])
    return find_language_doc(os.path.split(custom_document)[0], other_language, children)


class ToPager:
    def __init__(self, temp_named_file):
        self.temp_named_file = temp_named_file

    def writelines(self, lines):
        self.temp_named_file.writelines(lines)

    def close(self):
        self.temp_named_file.flush()
        git_pager = execute(['git', 'var', 'GIT_PAGER'])
        subprocess.check_call([git_pager, self.temp_named_file.name])
        self.temp_named_file.close()


class ToStdOut:
    def writelines(self, lines):
        self.system_stdout_stream.writelines(lines)

    def close(self):
        self.system_stdout_stream.flush()

    def __init__(self, system_stdout_stream):
        self.system_stdout_stream = system_stdout_stream


if __name__ == '__main__':
    arguments = SCRIPT_COMMAND_PARSER.parse_args()
    if arguments.help or not arguments.path:
        sys.stdout.write(SCRIPT_DESCRIPTION)
        sys.exit(0)

    working_language = os.path.join(CLICKHOUSE_REPO_HOME, 'docs', arguments.path)
    with contextlib.closing(ToStdOut(sys.stdout) if arguments.no_pager else ToPager(NamedTemporaryFile('r+'))) as writer:
        exit(diff_directory(find_language_doc(working_language), working_language, writer))
