import collections
import copy
import io
import logging
import os
import tarfile

import requests

import util


def choose_latest_releases():
    seen = collections.OrderedDict()
    candidates = requests.get('https://api.github.com/repos/yandex/ClickHouse/tags?per_page=100').json()
    for tag in candidates:
        name = tag.get('name', '')
        if 'v18' in name or 'stable' not in name:
            continue
        major_version = '.'.join((name.split('.', 2))[:2])
        if major_version not in seen:
            seen[major_version] = (name, tag.get('tarball_url'),)
            
    return seen.items()
    

def process_release(args, callback, release):
    name, (full_name, tarball_url,) = release
    logging.info('Building docs for %s', full_name)
    buf = io.BytesIO(requests.get(tarball_url).content)
    tar = tarfile.open(mode='r:gz', fileobj=buf)
    with util.temp_dir() as base_dir:
        tar.extractall(base_dir)
        args = copy.deepcopy(args)
        args.version_prefix = name
        args.docs_dir = os.path.join(base_dir, os.listdir(base_dir)[0], 'docs')
        callback(args)


def build_releases(args, callback):
    for release in args.stable_releases:
        process_release(args, callback, release)


