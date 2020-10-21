import collections
import copy
import io
import logging
import os
import random
import sys
import tarfile
import time

import requests

import util


def yield_candidates():
    for page in range(1, 100):
        url = f'https://api.github.com/repos/ClickHouse/ClickHouse/tags?per_page=100&page={page}'
        github_token = os.getenv('GITHUB_TOKEN')
        if github_token:
            headers = {'authorization': f'OAuth {github_token}'}
        else:
            headers = {}
        for candidate in requests.get(url, headers=headers).json():
            yield candidate
    time.sleep(random.random() * 3)


def choose_latest_releases(args):
    logging.info('Collecting release candidates')
    seen_stable = collections.OrderedDict()
    seen_lts = collections.OrderedDict()
    candidates = []
    stable_count = 0
    lts_count = 0

    for tag in yield_candidates():
        if isinstance(tag, dict):
            name = tag.get('name', '')
            is_stable = 'stable' in name
            is_lts = 'lts' in name
            is_unstable = not (is_stable or is_lts)
            is_in_blacklist = ('v18' in name) or ('prestable' in name) or ('v1.1' in name)
            if is_unstable or is_in_blacklist:
                continue
            major_version = '.'.join((name.split('.', 2))[:2])
            if major_version not in seen_lts:
                if (stable_count >= args.stable_releases_limit) and (lts_count >= args.lts_releases_limit):
                    break

                payload = (name, tag.get('tarball_url'), is_lts,)
                logging.debug(payload)
                if is_lts:
                    if lts_count < args.lts_releases_limit:
                        seen_lts[major_version] = payload
                        try:
                            del seen_stable[major_version]
                        except KeyError:
                            pass
                    lts_count += 1
                else:
                    if stable_count < args.stable_releases_limit:
                        if major_version not in seen_stable:
                            seen_stable[major_version] = payload
                            stable_count += 1

            logging.debug(
                f'Stables: {stable_count}/{args.stable_releases_limit} LTS: {lts_count}/{args.lts_releases_limit}'
            )
        else:
            logging.fatal('Unexpected GitHub response: %s', str(candidates))
            sys.exit(1)

    logging.info('Found LTS releases: %s', ', '.join(seen_lts.keys()))
    logging.info('Found stable releases: %s', ', '.join(seen_stable.keys()))
    return sorted(list(seen_lts.items()) + list(seen_stable.items()))


def process_release(args, callback, release):
    name, (full_name, tarball_url, is_lts,) = release
    logging.info(f'Building docs for {full_name}')
    buf = io.BytesIO(requests.get(tarball_url).content)
    tar = tarfile.open(mode='r:gz', fileobj=buf)
    with util.temp_dir() as base_dir:
        tar.extractall(base_dir)
        args = copy.copy(args)
        args.version_prefix = name
        args.is_stable_release = True
        args.docs_dir = os.path.join(base_dir, os.listdir(base_dir)[0], 'docs')
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


if __name__ == '__main__':
    class DummyArgs(object):
        lts_releases_limit = 1
        stable_releases_limit = 3
    logging.basicConfig(
        level=logging.DEBUG,
        stream=sys.stderr
    )
    for item in choose_latest_releases(DummyArgs()):
        print(item)
