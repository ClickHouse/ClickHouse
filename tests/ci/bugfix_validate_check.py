#!/usr/bin/env python3

import argparse
import csv
import itertools
import os
import sys


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("report1")
    parser.add_argument("report2")
    return parser.parse_args()


def post_commit_status_from_file(file_path):
    res = []
    with open(file_path, "r", encoding="utf-8") as f:
        fin = csv.reader(f, delimiter="\t")
        res = list(itertools.islice(fin, 1))
    if len(res) < 1:
        raise Exception(f'Can\'t read from "{file_path}"')
    if len(res[0]) != 3:
        raise Exception(f'Can\'t read from "{file_path}"')
    return res[0]


def process_results(file_path):
    state, report_url, description = post_commit_status_from_file(file_path)
    prefix = os.path.basename(os.path.dirname(file_path))
    print(
        f"::notice:: bugfix check: {prefix} - {state}: {description} Report url: {report_url}"
    )
    return state == "success"


def main(args):
    is_ok = False
    is_ok = process_results(args.report1) or is_ok
    is_ok = process_results(args.report2) or is_ok
    sys.exit(0 if is_ok else 1)


if __name__ == "__main__":
    main(parse_args())
