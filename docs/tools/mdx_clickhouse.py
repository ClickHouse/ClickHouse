#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import markdown.inlinepatterns
import markdown.extensions
import markdown.util


class NofollowMixin(object):
    def handleMatch(self, m):
        try:
            el = super(NofollowMixin, self).handleMatch(m)
        except IndexError:
            return

        if el is not None:
            href = el.get('href') or ''
            if href.startswith('http') and not href.startswith('https://clickhouse.yandex'):
                el.set('rel', 'external nofollow')
        return el


class NofollowAutolinkPattern(NofollowMixin, markdown.inlinepatterns.AutolinkPattern):
    pass


class NofollowLinkPattern(NofollowMixin, markdown.inlinepatterns.LinkPattern):
    pass


class ClickHousePreprocessor(markdown.util.Processor):
    def run(self, lines):
        for line in lines:
            if '<!--hide-->' not in line:
                yield line


class ClickHouseMarkdown(markdown.extensions.Extension):

    def extendMarkdown(self, md, md_globals):
        md.preprocessors['clickhouse'] = ClickHousePreprocessor()
        md.inlinePatterns['link'] = NofollowLinkPattern(markdown.inlinepatterns.LINK_RE, md)
        md.inlinePatterns['autolink'] = NofollowLinkPattern(markdown.inlinepatterns.AUTOLINK_RE, md)


def makeExtension(**kwargs):
    return ClickHouseMarkdown(**kwargs)
