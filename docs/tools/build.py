#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import shutil
import subprocess
import sys
import time

import bs4
import jinja2
import livereload
import markdown.util

from mkdocs import config
from mkdocs import exceptions
from mkdocs.commands import build as mkdocs_build

from concatenate import concatenate

import mdx_clickhouse
import nav
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
            'tr': 'Türkçe',
            'fa': 'فارسی'
        }

        site_names = {
            'en': 'ClickHouse %s Documentation',
            'zh': 'ClickHouse文档 %s',
            'es': 'Documentación de ClickHouse %s',
            'fr': 'Documentation ClickHouse %s',
            'ru': 'Документация ClickHouse %s',
            'ja': 'ClickHouseドキュメント %s',
            'tr': 'ClickHouse Belgeleri %s',
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
            'pymdownx.details',
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

        website_url = 'https://clickhouse.tech'
        site_name = site_names.get(lang, site_names['en']) % args.version_prefix
        site_name = site_name.replace('  ', ' ')
        raw_config = dict(
            site_name=site_name,
            site_url=f'{website_url}/docs/{lang}/',
            docs_dir=os.path.join(args.docs_dir, lang),
            site_dir=site_dir,
            strict=not args.version_prefix,
            theme=theme_cfg,
            copyright='©2016–2020 Yandex LLC',
            use_directory_urls=True,
            repo_name='ClickHouse/ClickHouse',
            repo_url='https://github.com/ClickHouse/ClickHouse/',
            edit_uri=f'edit/master/docs/{lang}',
            markdown_extensions=markdown_extensions,
            plugins=plugins,
            extra={
                'stable_releases': args.stable_releases,
                'version_prefix': args.version_prefix,
                'single_page': False,
                'rev': args.rev,
                'rev_short': args.rev_short,
                'rev_url': args.rev_url,
                'website_url': website_url,
                'events': args.events,
                'languages': languages,
                'includes_dir':  os.path.join(os.path.dirname(__file__), '..', '_includes')
            }
        )

        if os.path.exists(config_path):
            raw_config['config_file'] = config_path
        else:
            raw_config['nav'] = nav.build_nav(lang, args)

        cfg = config.load_config(**raw_config)

        if not args.skip_multi_page:
            try:
                mkdocs_build.build(cfg)
            except jinja2.exceptions.TemplateError:
                if not args.version_prefix:
                    raise
                mdx_clickhouse.PatchedMacrosPlugin.disabled = True
                mkdocs_build.build(cfg)

        if not args.skip_single_page:
            build_single_page_version(lang, args, raw_config.get('nav'), cfg)

        mdx_clickhouse.PatchedMacrosPlugin.disabled = False

        logging.info(f'Finished building {lang} docs')

    except exceptions.ConfigurationError as e:
        raise SystemExit('\n' + str(e))


def build_single_page_version(lang, args, nav, cfg):
    logging.info(f'Building single page version for {lang}')
    os.environ['SINGLE_PAGE'] = '1'
    extra = cfg.data['extra']
    extra['single_page'] = True

    with util.autoremoved_file(os.path.join(args.docs_dir, lang, 'single.md')) as single_md:
        concatenate(lang, args.docs_dir, single_md, nav)

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

                if not args.test_only:
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

                    single_page_index_html = os.path.join(single_page_output_path, 'index.html')
                    single_page_content_js = os.path.join(single_page_output_path, 'content.js')
                    with open(single_page_index_html, 'r') as f:
                        sp_prefix, sp_js, sp_suffix = f.read().split('<!-- BREAK -->')
                    with open(single_page_index_html, 'w') as f:
                        f.write(sp_prefix)
                        f.write(sp_suffix)
                    with open(single_page_content_js, 'w') as f:
                        if args.minify:
                            import jsmin
                            sp_js = jsmin.jsmin(sp_js)
                        f.write(sp_js)

                logging.info(f'Re-building single page for {lang} pdf/test')
                with util.temp_dir() as test_dir:
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
                    if args.save_raw_single_page:
                        shutil.copytree(test_dir, args.save_raw_single_page)

                    if not args.version_prefix:  # maybe enable in future
                        logging.info(f'Running tests for {lang}')
                        test.test_single_page(
                            os.path.join(test_dir, 'single', 'index.html'), lang)

                    if not args.skip_pdf:
                        single_page_index_html = os.path.join(test_dir, 'single', 'index.html')
                        single_page_pdf = os.path.abspath(
                            os.path.join(single_page_output_path, f'clickhouse_{lang}.pdf')
                        )

                        with open(single_page_index_html, 'r') as f:
                            soup = bs4.BeautifulSoup(
                                f.read(),
                                features='html.parser'
                            )
                        soup_prefix = f'file://{test_dir}'
                        for img in soup.findAll('img'):
                            if img['src'].startswith('/'):
                                img['src'] = soup_prefix + img['src']
                        for script in soup.findAll('script'):
                            script['src'] = soup_prefix + script['src'].split('?', 1)[0]
                        for link in soup.findAll('link'):
                            link['href'] = soup_prefix + link['href'].split('?', 1)[0]

                        with open(single_page_index_html, 'w') as f:
                            f.write(str(soup))

                        create_pdf_command = [
                            'wkhtmltopdf',
                            '--print-media-type',
                            '--log-level', 'warn',
                            single_page_index_html, single_page_pdf
                        ]

                        logging.info(' '.join(create_pdf_command))
                        subprocess.check_call(' '.join(create_pdf_command), shell=True)

        logging.info(f'Finished building single page version for {lang}')


def write_redirect_html(out_path, to_url):
    out_dir = os.path.dirname(out_path)
    try:
        os.makedirs(out_dir)
    except OSError:
        pass
    with open(out_path, 'w') as f:
        f.write(f'''<!--[if IE 6]> Redirect: {to_url} <![endif]-->
<!DOCTYPE HTML>
<html lang="en-US">
    <head>
        <meta charset="UTF-8">
        <meta http-equiv="refresh" content="0; url={to_url}">
        <script type="text/javascript">
            window.location.href = "{to_url}";
        </script>
        <title>Page Redirection</title>
    </head>
    <body>
        If you are not redirected automatically, follow this <a href="{to_url}">link</a>.
    </body>
</html>''')


def build_redirect_html(args, from_path, to_path):
    for lang in args.lang.split(','):
        out_path = os.path.join(
            args.docs_output_dir, lang,
            from_path.replace('/index.md', '/index.html').replace('.md', '/index.html')
        )
        version_prefix = f'/{args.version_prefix}/' if args.version_prefix else '/'
        target_path = to_path.replace('/index.md', '/').replace('.md', '/')
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

    test.test_templates(args.website_dir)

    build_docs(args)

    from github import build_releases
    build_releases(args, build_docs)

    if not args.skip_website:
        website.process_benchmark_results(args)
        website.minify_website(args)

    for static_redirect in [
        ('benchmark.html', '/benchmark/dbms/'),
        ('benchmark_hardware.html', '/benchmark/hardware/'),
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
    arg_parser.add_argument('--lang', default='en,es,fr,ru,zh,ja,tr,fa')
    arg_parser.add_argument('--docs-dir', default='.')
    arg_parser.add_argument('--theme-dir', default=website_dir)
    arg_parser.add_argument('--website-dir', default=website_dir)
    arg_parser.add_argument('--output-dir', default='build')
    arg_parser.add_argument('--enable-stable-releases', action='store_true')
    arg_parser.add_argument('--stable-releases-limit', type=int, default='4')
    arg_parser.add_argument('--lts-releases-limit', type=int, default='2')
    arg_parser.add_argument('--version-prefix', type=str, default='')
    arg_parser.add_argument('--is-stable-release', action='store_true')
    arg_parser.add_argument('--skip-multi-page', action='store_true')
    arg_parser.add_argument('--skip-single-page', action='store_true')
    arg_parser.add_argument('--skip-pdf', action='store_true')
    arg_parser.add_argument('--skip-website', action='store_true')
    arg_parser.add_argument('--test-only', action='store_true')
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

    if args.test_only:
        args.skip_multi_page = True
        args.skip_website = True
        args.skip_pdf = True

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
