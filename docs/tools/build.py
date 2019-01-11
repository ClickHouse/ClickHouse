#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import argparse
import contextlib
import datetime
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import time

import markdown.extensions
import markdown.util

from mkdocs import config
from mkdocs import exceptions
from mkdocs.commands import build as mkdocs_build

from concatenate import concatenate
import mdx_clickhouse
import test

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

class ClickHouseMarkdown(markdown.extensions.Extension):
    class ClickHousePreprocessor(markdown.util.Processor):
        def run(self, lines):
            for line in lines:
                if '<!--hide-->' not in line:
                    yield line

    def extendMarkdown(self, md):
        md.preprocessors.register(self.ClickHousePreprocessor(), 'clickhouse_preprocessor', 31)

markdown.extensions.ClickHouseMarkdown = ClickHouseMarkdown

def build_for_lang(lang, args):
    logging.info('Building %s docs' % lang)
    os.environ['SINGLE_PAGE'] = '0'

    config_path = os.path.join(args.docs_dir, 'toc_%s.yml' % lang)

    try:
        theme_cfg = {
            'name': 'mkdocs',
            'custom_dir': os.path.join(os.path.dirname(__file__), args.theme_dir),
            'language': lang,
            'direction': 'rtl' if lang == 'fa' else 'ltr',
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
                'now': int(time.mktime(datetime.datetime.now().timetuple())) # TODO better way to avoid caching
            }
        }

        site_names = {
            'en': 'ClickHouse Documentation',
            'ru': 'Документация ClickHouse',
            'zh': 'ClickHouse文档',
            'fa': 'مستندات ClickHouse'
        }

        cfg = config.load_config(
            config_file=config_path,
            site_name=site_names.get(lang, site_names['en']),
            site_url='https://clickhouse.yandex/docs/%s/' % lang,
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
                'clickhouse',
                'admonition',
                'attr_list',
                'codehilite',
                'extra',
                {
                    'toc': {
                        'permalink': True,
                        'slugify': mdx_clickhouse.slugify
                    }
                }
            ],
            plugins=[{
                'search': {
                    'lang': ['en', 'ru'] if lang == 'ru' else ['en']
                }
            }],
            extra={
                'search': {
                    'language': 'en,ru' if lang == 'ru' else 'en'
                }
            }
        )

        mkdocs_build.build(cfg)

        if not args.skip_single_page:
            build_single_page_version(lang, args, cfg)

    except exceptions.ConfigurationError as e:
        raise SystemExit('\n' + str(e))


def build_single_page_version(lang, args, cfg):
    logging.info('Building single page version for ' + lang)
    os.environ['SINGLE_PAGE'] = '1'

    with autoremoved_file(os.path.join(args.docs_dir, lang, 'single.md')) as single_md:
        concatenate(lang, args.docs_dir, single_md)

        with temp_dir() as site_temp:
            with temp_dir() as docs_temp:
                docs_temp_lang = os.path.join(docs_temp, lang)
                shutil.copytree(os.path.join(args.docs_dir, lang), docs_temp_lang)
                for root, _, filenames in os.walk(docs_temp_lang):
                    for filename in filenames:
                        if filename != 'single.md' and filename.endswith('.md'):
                            os.unlink(os.path.join(root, filename))

                cfg.load_dict({
                    'docs_dir': docs_temp_lang,
                    'site_dir': site_temp,
                    'extra': {
                        'single_page': True
                    },
                    'nav': [
                        {cfg.data.get('site_name'): 'single.md'}
                    ]
                })

                mkdocs_build.build(cfg)

                single_page_output_path = os.path.join(args.docs_dir, args.output_dir, lang, 'single')

                if os.path.exists(single_page_output_path):
                    shutil.rmtree(single_page_output_path)

                shutil.copytree(
                    os.path.join(site_temp, 'single'),
                    single_page_output_path
                )

                if not args.skip_pdf:
                    single_page_index_html = os.path.abspath(os.path.join(single_page_output_path, 'index.html'))
                    single_page_pdf = single_page_index_html.replace('index.html', 'clickhouse_%s.pdf' % lang)
                    create_pdf_command = ['wkhtmltopdf', '--print-media-type', single_page_index_html, single_page_pdf]
                    logging.debug(' '.join(create_pdf_command))
                    subprocess.check_call(' '.join(create_pdf_command), shell=True)

                with temp_dir() as test_dir:
                    cfg.load_dict({
                        'docs_dir': docs_temp_lang,
                        'site_dir': test_dir,
                        'extra': {
                            'single_page': False
                        },
                        'nav': [
                            {cfg.data.get('site_name'): 'single.md'}
                        ]
                    })
                    mkdocs_build.build(cfg)
                    test.test_single_page(os.path.join(test_dir, 'single', 'index.html'), lang)
                    if args.save_raw_single_page:
                        shutil.copytree(test_dir, args.save_raw_single_page)


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
    arg_parser.add_argument('--lang', default='en,ru,zh,fa')
    arg_parser.add_argument('--docs-dir', default='.')
    arg_parser.add_argument('--theme-dir', default='mkdocs-material-theme')
    arg_parser.add_argument('--output-dir', default='build')
    arg_parser.add_argument('--skip-single-page', action='store_true')
    arg_parser.add_argument('--skip-pdf', action='store_true')
    arg_parser.add_argument('--save-raw-single-page', type=str)
    arg_parser.add_argument('--verbose', action='store_true')

    args = arg_parser.parse_args()
    os.chdir(os.path.join(os.path.dirname(__file__), '..'))

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stderr
    )

    logging.getLogger('MARKDOWN').setLevel(logging.INFO)

    from build import build
    build(args)
