import logging
import os
import re
import shutil
import subprocess
import yaml

import bs4
import mkdocs.commands.build

import test
import util
import website


def recursive_values(item):
    if isinstance(item, dict):
        for _, value in list(item.items()):
            yield from recursive_values(value)
    elif isinstance(item, list):
        for value in item:
            yield from recursive_values(value)
    elif isinstance(item, str):
        yield item


def concatenate(lang, docs_path, single_page_file, nav):
    lang_path = os.path.join(docs_path, lang)
    az_re = re.compile(r'[a-z]')

    proj_config = f'{docs_path}/toc_{lang}.yml'
    if os.path.exists(proj_config):
        with open(proj_config) as cfg_file:
            nav = yaml.full_load(cfg_file.read())['nav']
    files_to_concatenate = list(recursive_values(nav))
    files_count = len(files_to_concatenate)
    logging.info(f'{files_count} files will be concatenated into single md-file for {lang}.')
    logging.debug('Concatenating: ' + ', '.join(files_to_concatenate))
    assert files_count > 0, f'Empty single-page for {lang}'

    for path in files_to_concatenate:
        if path.endswith('introduction/info.md'):
            continue
        try:
            with open(os.path.join(lang_path, path)) as f:
                anchors = set()
                tmp_path = path.replace('/index.md', '/').replace('.md', '/')
                prefixes = ['', '../', '../../', '../../../']
                parts = tmp_path.split('/')
                anchors.add(parts[-2] + '/')
                anchors.add('/'.join(parts[1:]))

                for part in parts[0:-2] if len(parts) > 2 else parts:
                    for prefix in prefixes:
                        anchor = prefix + tmp_path
                        if anchor:
                            anchors.add(anchor)
                            anchors.add('../' + anchor)
                            anchors.add('../../' + anchor)
                    tmp_path = tmp_path.replace(part, '..')

                for anchor in anchors:
                    if re.search(az_re, anchor):
                        single_page_file.write('<a name="%s"></a>' % anchor)

                single_page_file.write('\n')

                in_metadata = False
                for l in f:
                    if l.startswith('---'):
                        in_metadata = not in_metadata
                    if l.startswith('#'):
                        l = '#' + l
                    if not in_metadata:
                        single_page_file.write(l)
        except IOError as e:
            logging.warning(str(e))

    single_page_file.flush()


def build_single_page_version(lang, args, nav, cfg):
    logging.info(f'Building single page version for {lang}')
    os.environ['SINGLE_PAGE'] = '1'
    extra = cfg.data['extra']
    extra['single_page'] = True
    extra['is_amp'] = False

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
                    mkdocs.commands.build.build(cfg)

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
                    mkdocs.commands.build.build(cfg)

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
                            script_src = script.get('src')
                            if script_src:
                                script['src'] = soup_prefix + script_src.split('?', 1)[0]
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
