import logging
import os
import subprocess

import bs4
import cssmin
import jinja2
import mkdocs.commands.build

import mdx_clickhouse
import test
import util
import website


def prepare_amp_html(lang, args, root, site_temp, main_site_dir):
    src_path = root
    src_index = os.path.join(src_path, 'index.html')
    rel_path = os.path.relpath(src_path, site_temp)
    dst_path = os.path.join(main_site_dir, rel_path, 'amp')
    dst_index = os.path.join(dst_path, 'index.html')

    logging.debug(f'Generating AMP version for {rel_path} ({lang})')
    os.makedirs(dst_path)
    with open(src_index, 'r') as f:
        content = f.read()
    css_in = ' '.join(website.get_css_in(args))
    command = f"purifycss --min {css_in} '{src_index}'"
    logging.debug(command)
    inline_css = subprocess.check_output(command, shell=True).decode('utf-8')
    inline_css = inline_css.replace('!important', '').replace('/*!', '/*')
    inline_css = cssmin.cssmin(inline_css)
    content = content.replace('CUSTOM_CSS_PLACEHOLDER', inline_css)

    with open(dst_index, 'w') as f:
        f.write(content)

    return dst_index


def build_amp(lang, args, cfg):
    # AMP docs: https://amp.dev/documentation/
    logging.info(f'Building AMP version for {lang}')
    with util.temp_dir() as site_temp:
        extra = cfg.data['extra']
        main_site_dir = cfg.data['site_dir']
        extra['is_amp'] = True
        cfg.load_dict({
            'site_dir': site_temp,
            'extra': extra
        })

        try:
            mkdocs.commands.build.build(cfg)
        except jinja2.exceptions.TemplateError:
            if not args.version_prefix:
                raise
            mdx_clickhouse.PatchedMacrosPlugin.disabled = True
            mkdocs.commands.build.build(cfg)

        paths = []
        for root, _, filenames in os.walk(site_temp):
            if 'index.html' in filenames:
                paths.append(prepare_amp_html(lang, args, root, site_temp, main_site_dir))
    logging.info(f'Finished building AMP version for {lang}')


def html_to_amp(content):
    soup = bs4.BeautifulSoup(
        content,
        features='html.parser'
    )

    for tag in soup.find_all():
        if tag.attrs.get('id') == 'tostring':
            tag.attrs['id'] = '_tostring'
        if tag.name == 'img':
            tag.name = 'amp-img'
            tag.attrs['layout'] = 'responsive'
            src = tag.attrs['src']
            if not (src.startswith('/') or src.startswith('http')):
                tag.attrs['src'] = f'../{src}'
            if not tag.attrs.get('width'):
                tag.attrs['width'] = '640'
            if not tag.attrs.get('height'):
                tag.attrs['height'] = '320'
        if tag.name == 'iframe':
            tag.name = 'amp-iframe'
            tag.attrs['layout'] = 'responsive'
            del tag.attrs['alt']
            del tag.attrs['allowfullscreen']
            if not tag.attrs.get('width'):
                tag.attrs['width'] = '640'
            if not tag.attrs.get('height'):
                tag.attrs['height'] = '320'
        elif tag.name == 'a':
            href = tag.attrs.get('href')
            if href:
                if not (href.startswith('/') or href.startswith('http')):
                    if '#' in href:
                        href, anchor = href.split('#')
                    else:
                        anchor = None
                    href = f'../{href}amp/'
                    if anchor:
                        href = f'{href}#{anchor}'
                    tag.attrs['href'] = href
    content = str(soup)
    return website.minify_html(content)
