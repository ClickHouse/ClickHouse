import logging
import os
import shutil

import cssmin
import htmlmin
import jinja2
import jsmin


def copy_icons(args):
    logging.info('Copying icons')
    icons_dir = os.path.join(args.output_dir, 'images', 'icons')
    os.makedirs(icons_dir)
    for icon in [
        'github',
        'edit',
        'external-link'
    ]:
        icon = '%s.svg' % icon
        icon_src = os.path.join(args.website_dir, 'images', 'feathericons', 'icons', icon)
        icon_dst = os.path.join(icons_dir, icon)
        shutil.copy2(icon_src, icon_dst)


def build_website(args):
    logging.info('Building website')
    env = jinja2.Environment(
        loader=jinja2.FileSystemLoader(args.website_dir),
        extensions=['jinja2_highlight.HighlightExtension']
    )
    env.extend(jinja2_highlight_cssclass='syntax p-3 my-3')

    shutil.copytree(
        args.website_dir,
        args.output_dir,
        ignore=shutil.ignore_patterns(
            '*.md',
            '*.sh',
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
            if not (filename.endswith('.html') or filename.endswith('.css') or filename.endswith('.js')):
                continue
            logging.info('Processing %s', path)
            with open(path, 'rb') as f:
                content = f.read().decode('utf-8')

            template = env.from_string(content)
            content = template.render(args.__dict__)

            with open(path, 'wb') as f:
                f.write(content.encode('utf-8'))


def minify_website(args):
    if args.minify:
        logging.info('Minifying website')
        for root, _, filenames in os.walk(args.output_dir):
            for filename in filenames:
                path = os.path.join(root, filename)
                if not (
                    filename.endswith('.html') or
                    filename.endswith('.css') or
                    filename.endswith('.js')
                ):
                    continue

                logging.info('Minifying %s', path)
                with open(path, 'rb') as f:
                    content = f.read().decode('utf-8')
                if filename.endswith('.html'):
                    content = htmlmin.minify(content, remove_empty_space=False)
                elif filename.endswith('.css'):
                    content = cssmin.cssmin(content)
                elif filename.endswith('.js'):
                    content = jsmin.jsmin(content)
                with open(path, 'wb') as f:
                    f.write(content.encode('utf-8'))
