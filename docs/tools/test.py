#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import logging
import os
import sys

import bs4

import logging
import os
import subprocess

import bs4


def test_amp(paths, lang):
    try:
        # Get latest amp validator version
        subprocess.check_call('amphtml-validator --help',
                              stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL,
                              shell=True)
    except subprocess.CalledProcessError:
        subprocess.check_call('npm i -g amphtml-validator', stderr=subprocess.DEVNULL, shell=True)

    paths = ' '.join(paths)
    command = f'amphtml-validator {paths}'
    try:
        subprocess.check_output(command, shell=True).decode('utf-8')
    except subprocess.CalledProcessError:
        logging.error(f'Invalid AMP for {lang}')
        raise


def test_template(template_path):
    if template_path.endswith('amp.html'):
        # Inline CSS/JS is ok for AMP pages
        return

    logging.debug(f'Running tests for {template_path} template')
    with open(template_path, 'r') as f:
        soup = bs4.BeautifulSoup(
            f,
            features='html.parser'
        )
        for tag in soup.find_all():
            style_attr = tag.attrs.get('style')
            assert not style_attr, f'Inline CSS is prohibited, found {style_attr} in {template_path}'

            if tag.name == 'script':
                if tag.attrs.get('type') == 'application/ld+json':
                    continue
                for content in tag.contents:
                    assert not content, f'Inline JavaScript is prohibited, found "{content}" in {template_path}'


def test_templates(base_dir):
    logging.info('Running tests for templates')
    for root, _, filenames in os.walk(base_dir):
        for filename in filenames:
            if filename.endswith('.html'):
                test_template(os.path.join(root, filename))


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
            if href and href.startswith('#') and href != '#':
                if href[1:] not in anchor_points:
                    links_to_nowhere += 1
                    logging.info("Tag %s", tag)
                    logging.info('Link to nowhere: %s' % href)

        if duplicate_anchor_points:
            logging.warning('Found %d duplicate anchor points' % duplicate_anchor_points)

        if links_to_nowhere:
            if lang == 'en':  # TODO: check all languages again
                logging.error(f'Found {links_to_nowhere} links to nowhere in {lang}')
                sys.exit(1)
            else:
                logging.warning(f'Found {links_to_nowhere} links to nowhere in {lang}')

        if len(anchor_points) <= 10:
            logging.error('Html parsing is probably broken')
            sys.exit(1)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.DEBUG,
        stream=sys.stderr
    )
    test_single_page(sys.argv[1], sys.argv[2])
