#!/usr/bin/env python3
import datetime
import logging
import os
import time

import nav  # monkey patches mkdocs

import mkdocs.commands
from mkdocs import config
from mkdocs import exceptions

import mdx_clickhouse
import redirects

import util


def build_for_lang(lang, args):
    logging.info(f"Building {lang} blog")

    try:
        theme_cfg = {
            "name": None,
            "custom_dir": os.path.join(os.path.dirname(__file__), "..", args.theme_dir),
            "language": lang,
            "direction": "ltr",
            "static_templates": ["404.html"],
            "extra": {
                "now": int(
                    time.mktime(datetime.datetime.now().timetuple())
                )  # TODO better way to avoid caching
            },
        }

        # the following list of languages is sorted according to
        # https://en.wikipedia.org/wiki/List_of_languages_by_total_number_of_speakers
        languages = {"en": "English"}

        site_names = {"en": "ClickHouse Blog"}

        assert len(site_names) == len(languages)

        site_dir = os.path.join(args.blog_output_dir, lang)

        plugins = ["macros"]
        if args.htmlproofer:
            plugins.append("htmlproofer")

        website_url = "https://clickhouse.com"
        site_name = site_names.get(lang, site_names["en"])
        blog_nav, post_meta = nav.build_blog_nav(lang, args)
        raw_config = dict(
            site_name=site_name,
            site_url=f"{website_url}/blog/{lang}/",
            docs_dir=os.path.join(args.blog_dir, lang),
            site_dir=site_dir,
            strict=True,
            theme=theme_cfg,
            nav=blog_nav,
            copyright="©2016–2022 ClickHouse, Inc.",
            use_directory_urls=True,
            repo_name="ClickHouse/ClickHouse",
            repo_url="https://github.com/ClickHouse/ClickHouse/",
            edit_uri=f"edit/master/website/blog/{lang}",
            markdown_extensions=mdx_clickhouse.MARKDOWN_EXTENSIONS,
            plugins=plugins,
            extra=dict(
                now=datetime.datetime.now().isoformat(),
                rev=args.rev,
                rev_short=args.rev_short,
                rev_url=args.rev_url,
                website_url=website_url,
                events=args.events,
                languages=languages,
                includes_dir=os.path.join(os.path.dirname(__file__), "..", "_includes"),
                is_blog=True,
                post_meta=post_meta,
                today=datetime.date.today().isoformat(),
            ),
        )

        cfg = config.load_config(**raw_config)
        mkdocs.commands.build.build(cfg)

        redirects.build_blog_redirects(args)

        env = util.init_jinja2_env(args)
        with open(
            os.path.join(args.website_dir, "templates", "blog", "rss.xml"), "rb"
        ) as f:
            rss_template_string = f.read().decode("utf-8").strip()
        rss_template = env.from_string(rss_template_string)
        with open(os.path.join(args.blog_output_dir, lang, "rss.xml"), "w") as f:
            f.write(rss_template.render({"config": raw_config}))

        logging.info(f"Finished building {lang} blog")

    except exceptions.ConfigurationError as e:
        raise SystemExit("\n" + str(e))


def build_blog(args):
    tasks = []
    for lang in args.blog_lang.split(","):
        if lang:
            tasks.append(
                (
                    lang,
                    args,
                )
            )
    util.run_function_in_parallel(build_for_lang, tasks, threads=False)
