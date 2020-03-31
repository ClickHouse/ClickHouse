#!/usr/bin/env python3

import argparse
import datetime
import http.server
import logging
import multiprocessing
import os
import shutil
import socketserver
import subprocess
import sys
import time

import jinja2
import livereload
import markdown.util

from mkdocs import config
from mkdocs import exceptions
from mkdocs.commands import build as mkdocs_build

from concatenate import concatenate

import mdx_clickhouse
import test
import util
import website


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
            'custom_dir': os.path.join(os.path.dirname(__file__), '..', args.theme_dir),
            'language': lang,
            'direction': 'rtl' if lang == 'fa' else 'ltr',
            # TODO: cleanup
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
            'es': 'Español',
            'fr': 'Français',
            'ru': 'Русский',
            'ja': '日本語',
            'fa': 'فارسی'
        }

        site_names = {
            'en': 'ClickHouse %s Documentation',
            'es': 'Documentación de ClickHouse %s',
            'fr': 'Documentation ClickHouse %s',
            'ru': 'Документация ClickHouse %s',
            'zh': 'ClickHouse文档 %s',
            'ja': 'ClickHouseドキュメント %s',
            'fa': 'مستندات %sClickHouse'
        }

        assert len(site_names) == len(languages)

        if args.version_prefix:
            site_dir = os.path.join(args.docs_output_dir, args.version_prefix, lang)
        else:
            site_dir = os.path.join(args.docs_output_dir, lang)

        markdown_extensions = [
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
        ]

        plugins = ['macros']
        if args.htmlproofer:
            plugins.append('htmlproofer')

        raw_config = dict(
            config_file=config_path,
            site_name=site_names.get(lang, site_names['en']) % args.version_prefix,
            site_url=f'https://clickhouse.tech/docs/{lang}/',
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
            markdown_extensions=markdown_extensions,
            plugins=plugins,
            extra={
                'stable_releases': args.stable_releases,
                'version_prefix': args.version_prefix,
                'single_page': False,
                'rev':       args.rev,
                'rev_short': args.rev_short,
                'rev_url':   args.rev_url,
                'events':    args.events,
                'languages': languages
            }
        )

        cfg = config.load_config(**raw_config)

        try:
            mkdocs_build.build(cfg)
        except jinja2.exceptions.TemplateError:
            if not args.version_prefix:
                raise
            mdx_clickhouse.PatchedMacrosPlugin.disabled = True
            mkdocs_build.build(cfg)

        if not args.skip_single_page:
            build_single_page_version(lang, args, cfg)

        mdx_clickhouse.PatchedMacrosPlugin.disabled = False

        logging.info(f'Finished building {lang} docs')

    except exceptions.ConfigurationError as e:
        raise SystemExit('\n' + str(e))


def build_single_page_version(lang, args, cfg):
    logging.info(f'Building single page version for {lang}')
    os.environ['SINGLE_PAGE'] = '1'
    extra = cfg.data['extra']
    extra['single_page'] = True

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
                    'extra': extra,
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
                
                logging.info(f'Re-building single page for {lang} pdf/test')
                with util.temp_dir() as test_dir:
                    single_page_pdf = os.path.abspath(
                        os.path.join(single_page_output_path, f'clickhouse_{lang}.pdf')
                    )
                    extra['single_page'] = False
                    cfg.load_dict({
                        'docs_dir': docs_temp_lang,
                        'site_dir': test_dir,
                        'extra': extra,
                        'nav': [
                            {cfg.data.get('site_name'): 'single.md'}
                        ]
                    })
                    mkdocs_build.build(cfg)

                    css_in = ' '.join(website.get_css_in(args))
                    js_in = ' '.join(website.get_js_in(args))
                    subprocess.check_call(f'cat {css_in} > {test_dir}/css/base.css', shell=True)
                    subprocess.check_call(f'cat {js_in} > {test_dir}/js/base.js', shell=True)
                    if not args.skip_pdf:
                        port_for_pdf = util.get_free_port()
                        httpd = socketserver.TCPServer(
                            ('', port_for_pdf), http.server.SimpleHTTPRequestHandler
                        )
                        logging.info(f"Serving for {lang} pdf at port {port_for_pdf}")
                        process = multiprocessing.Process(target=httpd.serve_forever)
                        with util.cd(test_dir):
                            process.start()
                            create_pdf_command = [
                                'wkhtmltopdf',
                                '--print-media-type',
                                '--log-level', 'warn',
                                f'http://localhost:{port_for_pdf}/single/', single_page_pdf
                            ]
                            try:
                                if args.save_raw_single_page:
                                    shutil.copytree(test_dir, args.save_raw_single_page)
                                logging.info(' '.join(create_pdf_command))
                                subprocess.check_call(' '.join(create_pdf_command), shell=True)
                            finally:
                                logging.info(f"Stop serving for {lang} pdf at port {port_for_pdf}"))
                                process.kill()
                                while True:
                                    time.sleep(0.25)
                                    try:
                                        process.close()
                                        break
                                    except ValueError:
                                        logging.info(f"Waiting for {lang} httpd at port {port_for_pdf} to stop")


                    if not args.version_prefix:  # maybe enable in future
                        logging.info(f'Running tests for {lang}')
                        test.test_single_page(
                            os.path.join(test_dir, 'single', 'index.html'), lang)

        logging.info(f'Finished building single page version for {lang}')


def write_redirect_html(out_path, to_url):
    out_dir = os.path.dirname(out_path)
    try:
        os.makedirs(out_dir)
    except OSError:
        pass
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
        version_prefix = args.version_prefix + '/' if args.version_prefix else '/'
        target_path = to_path.replace('.md', '/')
        to_url = f'/docs{version_prefix}{lang}/{target_path}'
        to_url = to_url.strip()
        write_redirect_html(out_path, to_url)


def build_redirects(args):
    with open(os.path.join(args.docs_dir, 'redirects.txt'), 'r') as f:
        for line in f:
            from_path, to_path = line.split(' ', 1)
            build_redirect_html(args, from_path, to_path)


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
        website.build_website(args)

    build_docs(args)

    from github import build_releases
    build_releases(args, build_docs)

    if not args.skip_website:
        website.minify_website(args)

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
    website_dir = os.path.join('..', 'website')
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--lang', default='en,es,fr,ru,zh,ja,fa')
    arg_parser.add_argument('--docs-dir', default='.')
    arg_parser.add_argument('--theme-dir', default=website_dir)
    arg_parser.add_argument('--website-dir', default=website_dir)
    arg_parser.add_argument('--output-dir', default='build')
    arg_parser.add_argument('--enable-stable-releases', action='store_true')
    arg_parser.add_argument('--stable-releases-limit', type=int, default='10')
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
    args.stable_releases = choose_latest_releases(args) if args.enable_stable_releases else []
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
        server.watch(args.docs_dir + '**/*', livereload.shell(new_args, cwd='tools', shell=True))
        server.watch(args.website_dir + '**/*', livereload.shell(new_args, cwd='tools', shell=True))
        server.serve(
            root=args.output_dir,
            host='0.0.0.0',
            port=args.livereload
        )
        sys.exit(0)
