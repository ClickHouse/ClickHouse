#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import argparse
import datetime
import logging
import os
import shutil
import subprocess
import sys
import time

import markdown.extensions
import markdown.util

from mkdocs import config
from mkdocs import exceptions
from mkdocs.commands import build as mkdocs_build

from concatenate import concatenate

from website import build_website, minify_website
import mdx_clickhouse
import test
import util


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
                'now': int(time.mktime(datetime.datetime.now().timetuple())) # TODO better way to avoid caching
            }
        }

        site_names = {
            'en': 'ClickHouse %s Documentation',
            'ru': 'Документация ClickHouse %s',
            'zh': 'ClickHouse文档 %s',
            'fa': 'مستندات  %sClickHouse'
        }

        if args.version_prefix:
            site_dir = os.path.join(args.docs_output_dir, args.version_prefix, lang)
        else:
            site_dir = os.path.join(args.docs_output_dir, lang)
            
        cfg = config.load_config(
            config_file=config_path,
            site_name=site_names.get(lang, site_names['en']) % args.version_prefix,
            site_url='https://clickhouse.yandex/docs/%s/' % lang,
            docs_dir=os.path.join(args.docs_dir, lang),
            site_dir=site_dir,
            strict=not args.version_prefix,
            theme=theme_cfg,
            copyright='©2016–2019 Yandex LLC',
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
                },
                'stable_releases': args.stable_releases,
                'version_prefix': args.version_prefix
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

    with util.autoremoved_file(os.path.join(args.docs_dir, lang, 'single.md')) as single_md:
        concatenate(lang, args.docs_dir, single_md)

        with util.temp_dir() as site_temp:
            with util.temp_dir() as docs_temp:
                docs_src_lang = os.path.join(args.docs_dir, lang)
                docs_temp_lang = os.path.join(docs_temp, lang)
                shutil.copytree(docs_src_lang, docs_temp_lang)
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

                if args.version_prefix:
                    single_page_output_path = os.path.join(args.docs_dir, args.docs_output_dir, args.version_prefix, lang, 'single')
                else:
                    single_page_output_path = os.path.join(args.docs_dir, args.docs_output_dir, lang, 'single')

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

                with util.temp_dir() as test_dir:
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
                    if not args.version_prefix:  # maybe enable in future
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

    with open(os.path.join(args.docs_output_dir, 'redirects.conf'), 'w') as f:
        f.write('\n'.join(rewrites))


def build_docs(args):
    for lang in args.lang.split(','):
        build_for_lang(lang, args)


def build(args):
    if os.path.exists(args.output_dir):
        shutil.rmtree(args.output_dir)

    if not args.skip_website:
        build_website(args)

    build_docs(args)
    
    from github import build_releases
    build_releases(args, build_docs)

    build_redirects(args)

    if not args.skip_website:
        minify_website(args)


if __name__ == '__main__':
    os.chdir(os.path.join(os.path.dirname(__file__), '..'))
    
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--lang', default='en,ru,zh,fa')
    arg_parser.add_argument('--docs-dir', default='.')
    arg_parser.add_argument('--theme-dir', default='mkdocs-material-theme')
    arg_parser.add_argument('--website-dir', default=os.path.join('..', 'website'))
    arg_parser.add_argument('--output-dir', default='build')
    arg_parser.add_argument('--enable-stable-releases', action='store_true')
    arg_parser.add_argument('--version-prefix', type=str, default='')
    arg_parser.add_argument('--skip-single-page', action='store_true')
    arg_parser.add_argument('--skip-pdf', action='store_true')
    arg_parser.add_argument('--skip-website', action='store_true')
    arg_parser.add_argument('--save-raw-single-page', type=str)
    arg_parser.add_argument('--verbose', action='store_true')

    args = arg_parser.parse_args()
    args.docs_output_dir = os.path.join(os.path.abspath(args.output_dir), 'docs')
    
    from github import choose_latest_releases
    args.stable_releases = choose_latest_releases() if args.enable_stable_releases else []
    
    

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stderr
    )

    logging.getLogger('MARKDOWN').setLevel(logging.INFO)

    from build import build
    build(args)
