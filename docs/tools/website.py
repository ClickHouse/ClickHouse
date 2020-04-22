import hashlib
import logging
import os
import shutil
import subprocess

import closure
import cssmin
import htmlmin
import jinja2
import jsmin

import mdx_clickhouse


def build_website(args):
    logging.info('Building website')
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(args.website_dir),
        extensions=[
            'jinja2.ext.i18n',
            'jinja2_highlight.HighlightExtension'
        ]
    )
    env.extend(jinja2_highlight_cssclass='syntax p-3 my-3')
    translations_dir = os.path.join(args.website_dir, 'locale')
    env.install_gettext_translations(
        mdx_clickhouse.get_translations(translations_dir, 'en'),
        newstyle=True
    )

    shutil.copytree(
        args.website_dir,
        args.output_dir,
        ignore=shutil.ignore_patterns(
            '*.md',
            '*.sh',
            '*.css',
            '*.js',
            'build',
            'docs',
            'public',
            'node_modules',
            'templates',
            'feathericons'
        )
    )

    for root, _, filenames in os.walk(args.output_dir):
        for filename in filenames:
            if filename == 'main.html':
                continue

            path = os.path.join(root, filename)
            if not (filename.endswith('.html') or filename.endswith('.js')):
                continue
            logging.info('Processing %s', path)
            with open(path, 'rb') as f:
                content = f.read().decode('utf-8')

            template = env.from_string(content)
            content = template.render(args.__dict__)

            with open(path, 'wb') as f:
                f.write(content.encode('utf-8'))


def get_css_in(args):
    return [
        f"'{args.website_dir}/css/bootstrap.css'",
        f"'{args.website_dir}/css/docsearch.css'",
        f"'{args.website_dir}/css/base.css'",
        f"'{args.website_dir}/css/docs.css'",
        f"'{args.website_dir}/css/highlight.css'"
    ]


def get_js_in(args):
    return [
        f"'{args.website_dir}/js/jquery-3.4.1.js'",
        f"'{args.website_dir}/js/popper.js'",
        f"'{args.website_dir}/js/bootstrap.js'",
        f"'{args.website_dir}/js/base.js'",
        f"'{args.website_dir}/js/index.js'",
        f"'{args.website_dir}/js/docsearch.js'",
        f"'{args.website_dir}/js/docs.js'"
    ]


def minify_website(args):
    css_in = ' '.join(get_css_in(args))
    css_out = f'{args.output_dir}/css/base.css'
    if args.minify:
        command = f"purifycss -w '*algolia*' --min {css_in} '{args.output_dir}/*.html' " \
            f"'{args.output_dir}/docs/en/**/*.html' '{args.website_dir}/js/**/*.js' > {css_out}"
    else:
        command = f'cat {css_in} > {css_out}'
    logging.info(command)
    output = subprocess.check_output(command, shell=True)
    logging.debug(output)
    with open(css_out, 'rb') as f:
        css_digest = hashlib.sha3_224(f.read()).hexdigest()[0:8]

    js_in = get_js_in(args)
    js_out = f'{args.output_dir}/js/base.js'
    if args.minify:
        js_in = [js[1:-1] for js in js_in]
        closure_args = [
            '--js', *js_in, '--js_output_file', js_out,
            '--compilation_level', 'SIMPLE',
            '--dependency_mode', 'NONE',
            '--third_party', '--use_types_for_optimization',
            '--isolation_mode', 'IIFE'
        ]
        logging.info(closure_args)
        if closure.run(*closure_args):
            raise RuntimeError('failed to run closure compiler')
            
    else:
        logging.info(command)
        js_in = ' '.join(js_in)
        output = subprocess.check_output(f'cat {js_in} > {js_out}', shell=True)
        logging.debug(output)
    with open(js_out, 'rb') as f:
        js_digest = hashlib.sha3_224(f.read()).hexdigest()[0:8]
        logging.info(js_digest)

    if args.minify:
        logging.info('Minifying website')
        for root, _, filenames in os.walk(args.output_dir):
            for filename in filenames:
                path = os.path.join(root, filename)
                if not (
                    filename.endswith('.html') or
                    filename.endswith('.css')
                ):
                    continue

                logging.info('Minifying %s', path)
                with open(path, 'rb') as f:
                    content = f.read().decode('utf-8')
                if filename.endswith('.html'):
                    if not content.startswith('<!-- Redirect: '):
                        content = htmlmin.minify(content, remove_empty_space=False)
                    content = content.replace('base.css?css_digest', f'base.css?{css_digest}')
                    content = content.replace('base.js?js_digest', f'base.js?{js_digest}')
                elif filename.endswith('.css'):
                    content = cssmin.cssmin(content)
                elif filename.endswith('.js'):
                    content = jsmin.jsmin(content)
                with open(path, 'wb') as f:
                    f.write(content.encode('utf-8'))
