#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import argparse
import contextlib
import logging
import os
import shutil
import sys
import tempfile

from mkdocs import config
from mkdocs import exceptions
from mkdocs.commands import build as mkdocs_build

from concatenate import concatenate


@contextlib.contextmanager
def temp_dir():
    path = tempfile.mkdtemp(dir=os.environ.get('TEMP'))
    try:
        yield path
    finally:
        shutil.rmtree(path)


@contextlib.contextmanager
def autoremoved_file(path):
    try:
        with open(path, 'w') as handle:
            yield handle
    finally:
        os.unlink(path)


def build_for_lang(lang, args):
    logging.info('Building %s docs' % lang)

    config_path = os.path.join(args.docs_dir, 'toc_%s.yml' % lang)

    try:
        theme_cfg = {
            'name': None,
            'custom_dir': 'mkdocs-material-theme',
            'language': lang,
            'feature': {
                'tabs': False
            },
            'palette': {
                'primary': 'white',
                'accent': 'white'
            },
            'font': False,
            'logo': 'images/logo.svg',
            'favicon': 'assets/images/favicon.ico',
            'include_search_page': False,
            'search_index_only': True,
            'static_templates': ['404.html'],
            'extra': {
                'single_page': False,
                'opposite_lang': 'en' if lang == 'ru' else 'ru',
                'search': {
                    'language': 'en' if lang == 'en' else 'en, %s' % lang
                }
            }

        }
        cfg = config.load_config(
            config_file=config_path,
            site_name='ClickHouse Documentation' if lang == 'en' else 'Документация ClickHouse',
            docs_dir=os.path.join(args.docs_dir, lang),
            site_dir=os.path.join(args.output_dir, lang),
            strict=True,
            theme=theme_cfg,
            copyright='©2016–2018 Yandex LLC',
            use_directory_urls=True,
            repo_name='yandex/ClickHouse',
            repo_url='https://github.com/yandex/ClickHouse/',
            edit_uri='edit/master/docs/%s' % lang,
            extra_css=['assets/stylesheets/custom.css'],
            markdown_extensions=[
                'admonition',
                'attr_list',
                'codehilite'
            ]
        )

        mkdocs_build.build(cfg)

        if not args.skip_single_page:
            build_single_page_version(lang, args, cfg)

    except exceptions.ConfigurationError as e:
        raise SystemExit('\n' + str(e))


def build_single_page_version(lang, args, cfg):
    logging.info('Building single page version for ' + lang)

    with autoremoved_file(os.path.join(args.docs_dir, lang, 'single.md')) as single_md:
        concatenate(lang, args.docs_dir, single_md)

        with temp_dir() as temp:

            cfg.load_dict({
                'docs_dir': os.path.join(args.docs_dir, lang),
                'site_dir': temp,
                'extra': {
                    'single_page': True,
                    'opposite_lang': 'en' if lang == 'ru' else 'ru',
                    'search': {
                        'language': 'en, ru'
                    }
                },
                'pages': [
                    {cfg.data.get('site_name'): 'single.md'}
                ]
            })

            mkdocs_build.build(cfg)

            shutil.copytree(
                os.path.join(temp, 'single'),
                os.path.join(args.output_dir, lang, 'single')
            )


def build_redirects(args):
    lang_re_fragment = args.lang.replace(',', '|')
    rewrites = []
    with open(os.path.join(args.docs_dir, 'redirects.txt'), 'r') as f:
        for line in f:
            from_path, to_path = line.split(' ', 1)
            from_path = '^/docs/(' + lang_re_fragment + ')/' + from_path.replace('.md', '/?') + '$'
            to_path = '/docs/$1/' + to_path.replace('.md', '/')
            rewrites.append(' '.join(['rewrite', from_path, to_path, 'permanent;']))

    with open(os.path.join(args.output_dir, 'redirects.conf'), 'w') as f:
        f.write('\n'.join(rewrites))


def build(args):
    for lang in args.lang.split(','):
        build_for_lang(lang, args)

    build_redirects(args)


if __name__ == '__main__':
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--lang', default='en,ru')
    arg_parser.add_argument('--docs-dir', default='..')
    arg_parser.add_argument('--theme-dir', default='mkdocs-material-theme')
    arg_parser.add_argument('--output-dir', default='../build')
    arg_parser.add_argument('--skip-single-page', action='store_true')
    arg_parser.add_argument('--verbose', action='store_true')

    args = arg_parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stderr
    )

    from build import build
    build(args)
