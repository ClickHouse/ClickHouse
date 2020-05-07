import logging
import os
import shutil

import jinja2
from mkdocs.commands import build as mkdocs_build

import util
import mdx_clickhouse


def build_amp(lang, args, cfg):
    # AMP docs: https://amp.dev/documentation/

    logging.info(f'Building AMP version for {lang}')
    with util.temp_dir() as site_temp:
        extra = cfg.data['extra']
        main_site_dir = cfg.data['site_dir']
        extra['is_amp'] = True
        cfg.load_dict({
            'site_dir': site_temp,
            'extra': extra,
        })

        try:
            mkdocs_build.build(cfg)
        except jinja2.exceptions.TemplateError:
            if not args.version_prefix:
                raise
            mdx_clickhouse.PatchedMacrosPlugin.disabled = True
            mkdocs_build.build(cfg)

        for root, _, filenames in os.walk(site_temp):
            if 'index.html' in filenames:
                src_path = root
                rel_path = os.path.relpath(src_path, site_temp)
                dst_path = os.path.join(main_site_dir, rel_path, 'amp')
                logging.info(f'{src_path} - {dst_path}')
                os.makedirs(dst_path)
                shutil.copy2(
                    os.path.join(src_path, 'index.html'),
                    os.path.join(dst_path, 'index.html')
                )

    logging.info(f'Finished building AMP version for {lang}')
