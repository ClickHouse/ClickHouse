#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import logging
import sys

import bs4


def test_single_page(input_path, lang):
    with open(input_path) as f:
        soup = bs4.BeautifulSoup(
            f,
            features='html.parser'
        )
        anchor_points = set()
        duplicate_anchor_points = 0
        links_to_nowhere = 0
        for tag in soup.find_all():
            for anchor_point in [tag.attrs.get('name'), tag.attrs.get('id')]:
                if anchor_point:
                    if anchor_point in anchor_points:
                        duplicate_anchor_points += 1
                        logging.info('Duplicate anchor point: %s' % anchor_point)
                    else:
                        anchor_points.add(anchor_point)
        for tag in soup.find_all():
            href = tag.attrs.get('href')
            if href and href.startswith('#'):
                if href[1:] not in anchor_points:
                    links_to_nowhere += 1
                    logging.info("Tag %s", tag)
                    logging.info('Link to nowhere: %s' % href)

        if duplicate_anchor_points:
            logging.warning('Found %d duplicate anchor points' % duplicate_anchor_points)

        if lang == 'en' and links_to_nowhere:
            print(f'Found {links_to_nowhere} links to nowhere', file=sys.stderr)
            # TODO: restore sys.exit(1)

        if len(anchor_points) <= 10:
            print('Html parsing is probably broken', file=sys.stderr)
            sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        stream=sys.stderr
    )
    test_single_page(sys.argv[1], sys.argv[2])
