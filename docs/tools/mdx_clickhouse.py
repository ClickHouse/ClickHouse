#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import datetime
import os
import subprocess

import jinja2
import markdown.inlinepatterns
import markdown.extensions
import markdown.util
import macros.plugin

import slugify as slugify_impl


def slugify(value, separator):
    return slugify_impl.slugify(
        value, separator=separator, word_boundary=True, save_order=True
    )


MARKDOWN_EXTENSIONS = [
    "mdx_clickhouse",
    "admonition",
    "attr_list",
    "def_list",
    "codehilite",
    "nl2br",
    "sane_lists",
    "pymdownx.details",
    "pymdownx.magiclink",
    "pymdownx.superfences",
    "extra",
    {"toc": {"permalink": True, "slugify": slugify}},
]


class ClickHouseLinkMixin(object):
    def handleMatch(self, m, data):
        try:
            el, start, end = super(ClickHouseLinkMixin, self).handleMatch(m, data)
        except IndexError:
            return

        if el is not None:
            href = el.get("href") or ""
            is_external = href.startswith("http:") or href.startswith("https:")
            if is_external:
                if not href.startswith("https://clickhouse.com"):
                    el.set("rel", "external nofollow noreferrer")
        return el, start, end


class ClickHouseAutolinkPattern(
    ClickHouseLinkMixin, markdown.inlinepatterns.AutolinkInlineProcessor
):
    pass


class ClickHouseLinkPattern(
    ClickHouseLinkMixin, markdown.inlinepatterns.LinkInlineProcessor
):
    pass


class ClickHousePreprocessor(markdown.util.Processor):
    def run(self, lines):
        for line in lines:
            if "<!--hide-->" not in line:
                yield line


class ClickHouseMarkdown(markdown.extensions.Extension):
    def extendMarkdown(self, md, md_globals):
        md.preprocessors["clickhouse"] = ClickHousePreprocessor()
        md.inlinePatterns["link"] = ClickHouseLinkPattern(
            markdown.inlinepatterns.LINK_RE, md
        )
        md.inlinePatterns["autolink"] = ClickHouseAutolinkPattern(
            markdown.inlinepatterns.AUTOLINK_RE, md
        )


def makeExtension(**kwargs):
    return ClickHouseMarkdown(**kwargs)


def get_translations(dirname, lang):
    import babel.support

    return babel.support.Translations.load(dirname=dirname, locales=[lang, "en"])


class PatchedMacrosPlugin(macros.plugin.MacrosPlugin):
    disabled = False

    def on_config(self, config):
        super(PatchedMacrosPlugin, self).on_config(config)
        self.env.comment_start_string = "{##"
        self.env.comment_end_string = "##}"
        self.env.loader = jinja2.FileSystemLoader(
            [
                os.path.join(config.data["site_dir"]),
                os.path.join(config.data["extra"]["includes_dir"]),
            ]
        )

    def on_env(self, env, config, files):
        import util

        env.add_extension("jinja2.ext.i18n")
        dirname = os.path.join(config.data["theme"].dirs[0], "locale")
        lang = config.data["theme"]["language"]
        env.install_gettext_translations(get_translations(dirname, lang), newstyle=True)
        util.init_jinja2_filters(env)
        return env

    def render(self, markdown):
        if not self.disabled:
            return self.render_impl(markdown)
        else:
            return markdown

    def on_page_markdown(self, markdown, page, config, files):
        markdown = super(PatchedMacrosPlugin, self).on_page_markdown(
            markdown, page, config, files
        )

        if os.path.islink(page.file.abs_src_path):
            lang = config.data["theme"]["language"]
            page.canonical_url = page.canonical_url.replace(f"/{lang}/", "/en/", 1)

        return markdown

    def render_impl(self, markdown):
        md_template = self.env.from_string(markdown)
        return md_template.render(**self.variables)


macros.plugin.MacrosPlugin = PatchedMacrosPlugin
