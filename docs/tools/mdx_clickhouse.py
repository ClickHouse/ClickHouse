#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import os

import markdown.inlinepatterns
import markdown.extensions
import markdown.util

import slugify as slugify_impl

class ClickHouseLinkMixin(object):

    def handleMatch(self, m):
        single_page = (os.environ.get('SINGLE_PAGE') == '1')
        try:
            el = super(ClickHouseLinkMixin, self).handleMatch(m)
        except IndexError:
            return

        if el is not None:
            href = el.get('href') or ''
            is_external = href.startswith('http:') or href.startswith('https:')
            if is_external:
                if not href.startswith('https://clickhouse.yandex'):
                    el.set('rel', 'external nofollow')
            elif single_page:
                if '#' in href:
                    el.set('href', '#' + href.split('#', 1)[1])
                else:
                    el.set('href', '#' + href.replace('/index.md', '/').replace('.md', '/'))
        return el


class ClickHouseAutolinkPattern(ClickHouseLinkMixin, markdown.inlinepatterns.AutolinkPattern):
    pass


class ClickHouseLinkPattern(ClickHouseLinkMixin, markdown.inlinepatterns.LinkPattern):
    pass


class ClickHousePreprocessor(markdown.util.Processor):
    def run(self, lines):
        for line in lines:
            if '<!--hide-->' not in line:
                yield line


class ClickHouseMarkdown(markdown.extensions.Extension):

    def extendMarkdown(self, md, md_globals):
        md.preprocessors['clickhouse'] = ClickHousePreprocessor()
        md.inlinePatterns['link'] = ClickHouseLinkPattern(markdown.inlinepatterns.LINK_RE, md)
        md.inlinePatterns['autolink'] = ClickHouseAutolinkPattern(markdown.inlinepatterns.AUTOLINK_RE, md)

def makeExtension(**kwargs):
    return ClickHouseMarkdown(**kwargs)

def slugify(value, separator):
    return slugify_impl.slugify(value, separator=separator, word_boundary=True, save_order=True)
