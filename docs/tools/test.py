#!/usr/bin/env python3

import logging
import os
import sys
import bs4
import subprocess


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
                    anchor_points.add(anchor_point)

        for tag in soup.find_all():
            href = tag.attrs.get('href')
            if href and href.startswith('#') and href != '#':
                if href[1:] not in anchor_points:
                    links_to_nowhere += 1
                    logging.info("Tag %s", tag)
                    logging.info('Link to nowhere: %s' % href)

        if links_to_nowhere:
            if lang == 'en' or lang == 'ru':
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
