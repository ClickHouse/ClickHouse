import collections
import copy
import io
import logging
import os
import sys
import tarfile

import jinja2
import requests

import util


def choose_latest_releases():
    logging.info('Collecting release candidates')
    seen = collections.OrderedDict()
    candidates = []
    for page in range(1, 10):
        url = 'https://api.github.com/repos/ClickHouse/ClickHouse/tags?per_page=100&page=%d' % page
        candidates += requests.get(url).json()
    logging.info('Collected all release candidates')

    for tag in candidates:
        if isinstance(tag, dict):
            name = tag.get('name', '')
            is_unstable = ('stable' not in name) and ('lts' not in name)
            is_in_blacklist = ('v18' in name) or ('prestable' in name) or ('v1.1' in name)
            if is_unstable or is_in_blacklist:
                continue
            major_version = '.'.join((name.split('.', 2))[:2])
            if major_version not in seen:
                seen[major_version] = (name, tag.get('tarball_url'),)
                if len(seen) > 10:
                    break
        else:
            logging.fatal('Unexpected GitHub response: %s', str(candidates))
            sys.exit(1)

    logging.info('Found stable releases: %s', str(seen.keys()))
    return seen.items()


def process_release(args, callback, release):
    name, (full_name, tarball_url,) = release
    logging.info('Building docs for %s', full_name)
    buf = io.BytesIO(requests.get(tarball_url).content)
    tar = tarfile.open(mode='r:gz', fileobj=buf)
    with util.temp_dir() as base_dir:
        tar.extractall(base_dir)
        args = copy.copy(args)
        args.version_prefix = name
        args.is_stable_release = True
        args.docs_dir = os.path.join(base_dir, os.listdir(base_dir)[0], 'docs')
        try:
            callback(args)
        except jinja2.exceptions.TemplateSyntaxError:
            args.no_docs_macros = True
            callback(args)


def build_releases(args, callback):
    for release in args.stable_releases:
        process_release(args, callback, release)


def get_events(args):
    events = []
    skip = True
    with open(os.path.join(args.docs_dir, '..', 'README.md')) as f:
        for line in f:
            if skip:
                if 'Upcoming Events' in line:
                    skip = False
            else:
                if not line:
                    continue
                line = line.strip().split('](')
                if len(line) == 2:
                    tail = line[1].split(') ')
                    events.append({
                        'signup_link': tail[0],
                        'event_name':  line[0].replace('* [', ''),
                        'event_date':  tail[1].replace('on ', '').replace('.', '')
                    })
    return events
