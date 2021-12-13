#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import shutil
import subprocess
import sys
import time

import jinja2
import livereload
import markdown.util

import nav  # monkey patches mkdocs

from mkdocs import config
from mkdocs import exceptions
import mkdocs.commands.build

import amp
import blog
import mdx_clickhouse
import redirects
import single_page
import test
import util
import website

from cmake_in_clickhouse_generator import generate_cmake_flags_files

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
    logging.info(f'Building {lang} docs')
    os.environ['SINGLE_PAGE'] = '0'

    try:
        theme_cfg = {
            'name': None,
            'custom_dir': os.path.join(os.path.dirname(__file__), '..', args.theme_dir),
            'language': lang,
            'direction': 'rtl' if lang == 'fa' else 'ltr',
            'static_templates': ['404.html'],
            'extra': {
                'now': int(time.mktime(datetime.datetime.now().timetuple()))  # TODO better way to avoid caching
            }
        }

        # the following list of languages is sorted according to
        # https://en.wikipedia.org/wiki/List_of_languages_by_total_number_of_speakers
        languages = {
            'en': 'English',
            'zh': '中文',
            'ru': 'Русский',
            'ja': '日本語'
        }

        site_names = {
            'en': 'ClickHouse %s Documentation',
            'zh': 'ClickHouse文档 %s',
            'ru': 'Документация ClickHouse %s',
            'ja': 'ClickHouseドキュメント %s'
        }

        assert len(site_names) == len(languages)

        site_dir = os.path.join(args.docs_output_dir, lang)

        plugins = ['macros']
        if args.htmlproofer:
            plugins.append('htmlproofer')

        website_url = 'https://clickhouse.com'
        site_name = site_names.get(lang, site_names['en']) % ''
        site_name = site_name.replace('  ', ' ')

        raw_config = dict(
            site_name=site_name,
            site_url=f'{website_url}/docs/{lang}/',
            docs_dir=os.path.join(args.docs_dir, lang),
            site_dir=site_dir,
            strict=True,
            theme=theme_cfg,
            copyright='©2016–2021 ClickHouse, Inc.',
            use_directory_urls=True,
            repo_name='ClickHouse/ClickHouse',
            repo_url='https://github.com/ClickHouse/ClickHouse/',
            edit_uri=f'edit/master/docs/{lang}',
            markdown_extensions=mdx_clickhouse.MARKDOWN_EXTENSIONS,
            plugins=plugins,
            extra=dict(
                now=datetime.datetime.now().isoformat(),
                single_page=False,
                rev=args.rev,
                rev_short=args.rev_short,
                rev_url=args.rev_url,
                website_url=website_url,
                events=args.events,
                languages=languages,
                includes_dir=os.path.join(os.path.dirname(__file__), '..', '_includes'),
                is_amp=False,
                is_blog=False
            )
        )

        raw_config['nav'] = nav.build_docs_nav(lang, args)

        cfg = config.load_config(**raw_config)

        if not args.skip_multi_page:
            mkdocs.commands.build.build(cfg)

        if not args.skip_amp:
            amp.build_amp(lang, args, cfg)

        if not args.skip_single_page:
            single_page.build_single_page_version(lang, args, raw_config.get('nav'), cfg)

        mdx_clickhouse.PatchedMacrosPlugin.disabled = False

        logging.info(f'Finished building {lang} docs')

    except exceptions.ConfigurationError as e:
        raise SystemExit('\n' + str(e))


def build_docs(args):
    tasks = []
    for lang in args.lang.split(','):
        if lang:
            tasks.append((lang, args,))
    util.run_function_in_parallel(build_for_lang, tasks, threads=False)
    redirects.build_docs_redirects(args)


def build(args):
    if os.path.exists(args.output_dir):
        shutil.rmtree(args.output_dir)

    if not args.skip_website:
        website.build_website(args)

    if not args.skip_docs:
        generate_cmake_flags_files()

        build_docs(args)

    if not args.skip_blog:
        blog.build_blog(args)

    if not args.skip_website:
        website.process_benchmark_results(args)
        website.minify_website(args)
        redirects.build_static_redirects(args)


if __name__ == '__main__':
    os.chdir(os.path.join(os.path.dirname(__file__), '..'))

    # A root path to ClickHouse source code.
    src_dir = '..'

    website_dir = os.path.join(src_dir, 'website')

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--lang', default='en,ru,zh,ja')
    arg_parser.add_argument('--blog-lang', default='en,ru')
    arg_parser.add_argument('--docs-dir', default='.')
    arg_parser.add_argument('--theme-dir', default=website_dir)
    arg_parser.add_argument('--website-dir', default=website_dir)
    arg_parser.add_argument('--src-dir', default=src_dir)
    arg_parser.add_argument('--blog-dir', default=os.path.join(website_dir, 'blog'))
    arg_parser.add_argument('--output-dir', default='build')
    arg_parser.add_argument('--nav-limit', type=int, default='0')
    arg_parser.add_argument('--skip-multi-page', action='store_true')
    arg_parser.add_argument('--skip-single-page', action='store_true')
    arg_parser.add_argument('--skip-amp', action='store_true')
    arg_parser.add_argument('--skip-pdf', action='store_true')
    arg_parser.add_argument('--skip-website', action='store_true')
    arg_parser.add_argument('--skip-blog', action='store_true')
    arg_parser.add_argument('--skip-git-log', action='store_true')
    arg_parser.add_argument('--skip-docs', action='store_true')
    arg_parser.add_argument('--test-only', action='store_true')
    arg_parser.add_argument('--minify', action='store_true')
    arg_parser.add_argument('--htmlproofer', action='store_true')
    arg_parser.add_argument('--no-docs-macros', action='store_true')
    arg_parser.add_argument('--save-raw-single-page', type=str)
    arg_parser.add_argument('--livereload', type=int, default='0')
    arg_parser.add_argument('--verbose', action='store_true')

    args = arg_parser.parse_args()
    args.minify = False  # TODO remove

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stderr
    )

    logging.getLogger('MARKDOWN').setLevel(logging.INFO)

    args.docs_output_dir = os.path.join(os.path.abspath(args.output_dir), 'docs')
    args.blog_output_dir = os.path.join(os.path.abspath(args.output_dir), 'blog')

    from github import get_events
    args.rev = subprocess.check_output('git rev-parse HEAD', shell=True).decode('utf-8').strip()
    args.rev_short = subprocess.check_output('git rev-parse --short HEAD', shell=True).decode('utf-8').strip()
    args.rev_url = f'https://github.com/ClickHouse/ClickHouse/commit/{args.rev}'
    args.events = get_events(args)

    if args.test_only:
        args.skip_multi_page = True
        args.skip_blog = True
        args.skip_website = True
        args.skip_pdf = True
        args.skip_amp = True

    if args.skip_git_log or args.skip_amp:
        mdx_clickhouse.PatchedMacrosPlugin.skip_git_log = True

    from build import build
    build(args)

    if args.livereload:
        new_args = [arg for arg in sys.argv if not arg.startswith('--livereload')]
        new_args = sys.executable + ' ' + ' '.join(new_args)

        server = livereload.Server()
        server.watch(args.docs_dir + '**/*', livereload.shell(new_args, cwd='tools', shell=True))
        server.watch(args.website_dir + '**/*', livereload.shell(new_args, cwd='tools', shell=True))
        server.serve(
            root=args.output_dir,
            host='0.0.0.0',
            port=args.livereload
        )
        sys.exit(0)
