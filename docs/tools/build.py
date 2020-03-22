#!/usr/bin/env python3
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

import livereload
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
    logging.info(f'Building {lang} docs')
    os.environ['SINGLE_PAGE'] = '0'

    config_path = os.path.join(args.docs_dir, f'toc_{lang}.yml')
    if args.is_stable_release and not os.path.exists(config_path):
        logging.warning(f'Skipping {lang} docs, because {config} does not exist')
        return

    try:
        theme_cfg = {
            'name': None,
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
            'search_index_only': False,
            'static_templates': ['404.html'],
            'extra': {
                'now': int(time.mktime(datetime.datetime.now().timetuple()))  # TODO better way to avoid caching
            }
        }

        site_names = {
            'en': 'ClickHouse %s Documentation',
            'es': 'Documentación de ClickHouse %s',
            'ru': 'Документация ClickHouse %s',
            'zh': 'ClickHouse文档 %s',
            'ja': 'ClickHouseドキュメント %s',
            'fa': 'مستندات %sClickHouse'
        }

        if args.version_prefix:
            site_dir = os.path.join(args.docs_output_dir, args.version_prefix, lang)
        else:
            site_dir = os.path.join(args.docs_output_dir, lang)

        plugins = ['search']
        if not args.no_docs_macros:
            plugins.append('macros')
        if args.htmlproofer:
            plugins.append('htmlproofer')

        cfg = config.load_config(
            config_file=config_path,
            site_name=site_names.get(lang, site_names['en']) % args.version_prefix,
            site_url=f'https://clickhouse.yandex/docs/{lang}/',
            docs_dir=os.path.join(args.docs_dir, lang),
            site_dir=site_dir,
            strict=not args.version_prefix,
            theme=theme_cfg,
            copyright='©2016–2020 Yandex LLC',
            use_directory_urls=True,
            repo_name='ClickHouse/ClickHouse',
            repo_url='https://github.com/ClickHouse/ClickHouse/',
            edit_uri=f'edit/master/docs/{lang}',
            extra_css=[f'assets/stylesheets/custom.css?{args.rev_short}'],
            markdown_extensions=[
                'mdx_clickhouse',
                'admonition',
                'attr_list',
                'codehilite',
                'nl2br',
                'sane_lists',
                'pymdownx.magiclink',
                'pymdownx.superfences',
                'extra',
                {
                    'toc': {
                        'permalink': True,
                        'slugify': mdx_clickhouse.slugify
                    }
                }
            ],
            plugins=plugins,
            extra={
                'stable_releases': args.stable_releases,
                'version_prefix': args.version_prefix,
                'rev':       args.rev,
                'rev_short': args.rev_short,
                'rev_url':   args.rev_url,
                'events':    args.events
            }
        )

        mkdocs_build.build(cfg)

        if not args.skip_single_page:
            build_single_page_version(lang, args, cfg)

    logging.info(f'Finished building {lang} docs')

    except exceptions.ConfigurationError as e:
        raise SystemExit('\n' + str(e))


def build_single_page_version(lang, args, cfg):
    logging.info(f'Building single page version for {lang}')
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
                    single_page_pdf = single_page_index_html.replace('index.html', f'clickhouse_{lang}.pdf')
                    create_pdf_command = ['wkhtmltopdf', '--print-media-type', '--log-level', 'warn', single_page_index_html, single_page_pdf]
                    logging.info(' '.join(create_pdf_command))
                    subprocess.check_call(' '.join(create_pdf_command), shell=True)

                if not args.version_prefix:  # maybe enable in future
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
                        if args.save_raw_single_page:
                            shutil.copytree(test_dir, args.save_raw_single_page)

                        test.test_single_page(os.path.join(test_dir, 'single', 'index.html'), lang)
    logging.info(f'Finished building single page version for {lang}')


def write_redirect_html(out_path, to_url):
    with open(out_path, 'w') as f:
        f.write(f'''<!DOCTYPE HTML>
<html lang="en-US">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="0; url={to_url}">
        <script type="text/javascript">
            window.location.href = "{to_url}"
        </script>
        <title>Page Redirection</title>
    </head>
    <body>
        If you are not redirected automatically, follow this <a href="{to_url}">link</a>.
    </body>
</html>''')


def build_redirect_html(args, from_path, to_path):
    for lang in args.lang.split(','):
        out_path = os.path.join(args.docs_output_dir, lang, from_path.replace('.md', '/index.html'))
        out_dir = os.path.dirname(out_path)
        try:
            os.makedirs(out_dir)
        except OSError:
            pass
        version_prefix = args.version_prefix + '/' if args.version_prefix else '/'
        target_path = to_path.replace('.md', '/')
        to_url = f'/docs{version_prefix}{lang}/{target_path}'
        to_url = to_url.strip()
        write_redirect_html(out_path, to_url)


def build_redirects(args):
    lang_re_fragment = args.lang.replace(',', '|')
    rewrites = []

    with open(os.path.join(args.docs_dir, 'redirects.txt'), 'r') as f:
        for line in f:
            from_path, to_path = line.split(' ', 1)
            build_redirect_html(args, from_path, to_path)
            from_path = '^/docs/(' + lang_re_fragment + ')/' + from_path.replace('.md', '/?') + '$'
            to_path = '/docs/$1/' + to_path.replace('.md', '/')
            rewrites.append(' '.join(['rewrite', from_path, to_path, 'permanent;']))

    with open(os.path.join(args.docs_output_dir, 'redirects.conf'), 'w') as f:
        f.write('\n'.join(rewrites))


def build_docs(args):
    tasks = []
    for lang in args.lang.split(','):
        if lang:
            tasks.append((lang, args,))
    util.run_function_in_parallel(build_for_lang, tasks, threads=False)
    build_redirects(args)


def build(args):
    if os.path.exists(args.output_dir):
        shutil.rmtree(args.output_dir)

    if not args.skip_website:
        build_website(args)

    build_docs(args)

    from github import build_releases
    build_releases(args, build_docs)

    if not args.skip_website:
        minify_website(args)

    for static_redirect in [
        ('tutorial.html', '/docs/en/getting_started/tutorial/',),
        ('reference_en.html', '/docs/en/single/', ),
        ('reference_ru.html', '/docs/ru/single/',),
        ('docs/index.html', '/docs/en/',),
    ]:
        write_redirect_html(
            os.path.join(args.output_dir, static_redirect[0]),
            static_redirect[1]
        )


if __name__ == '__main__':
    os.chdir(os.path.join(os.path.dirname(__file__), '..'))

    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--lang', default='en,es,ru,zh,ja,fa')
    arg_parser.add_argument('--docs-dir', default='.')
    arg_parser.add_argument('--theme-dir', default='mkdocs-material-theme')
    arg_parser.add_argument('--website-dir', default=os.path.join('..', 'website'))
    arg_parser.add_argument('--output-dir', default='build')
    arg_parser.add_argument('--enable-stable-releases', action='store_true')
    arg_parser.add_argument('--version-prefix', type=str, default='')
    arg_parser.add_argument('--is-stable-release', action='store_true')
    arg_parser.add_argument('--skip-single-page', action='store_true')
    arg_parser.add_argument('--skip-pdf', action='store_true')
    arg_parser.add_argument('--skip-website', action='store_true')
    arg_parser.add_argument('--minify', action='store_true')
    arg_parser.add_argument('--htmlproofer', action='store_true')
    arg_parser.add_argument('--no-docs-macros', action='store_true')
    arg_parser.add_argument('--save-raw-single-page', type=str)
    arg_parser.add_argument('--livereload', type=int, default='0')
    arg_parser.add_argument('--verbose', action='store_true')

    args = arg_parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        stream=sys.stderr
    )

    logging.getLogger('MARKDOWN').setLevel(logging.INFO)

    args.docs_output_dir = os.path.join(os.path.abspath(args.output_dir), 'docs')

    from github import choose_latest_releases, get_events
    args.stable_releases = choose_latest_releases() if args.enable_stable_releases else []
    args.rev = subprocess.check_output('git rev-parse HEAD', shell=True).decode('utf-8').strip()
    args.rev_short = subprocess.check_output('git rev-parse --short HEAD', shell=True).decode('utf-8').strip()
    args.rev_url = f'https://github.com/ClickHouse/ClickHouse/commit/{args.rev}'
    args.events = get_events(args)

    from build import build
    build(args)
    
    if args.livereload:
        new_args = [arg for arg in sys.argv if not arg.startswith('--livereload')]
        new_args = sys.executable + ' ' + ' '.join(new_args)

        server = livereload.Server()
        server.watch(args.website_dir + '**/*', livereload.shell(new_args, cwd='tools', shell=True))
        server.watch(args.docs_dir + '**/*', livereload.shell(new_args, cwd='tools', shell=True))
        server.serve(
            root=args.output_dir,
            port=args.livereload
        )
        sys.exit(0)
