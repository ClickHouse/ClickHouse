#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import markdown.extensions
import markdown.util


class ClickHousePreprocessor(markdown.util.Processor):
    def run(self, lines):
        for line in lines:
            if '<!--hide-->' not in line:
                yield line

class ClickHouseMarkdown(markdown.extensions.Extension):

    def extendMarkdown(self, md, md_globals):
        md.preprocessors['clickhouse'] = ClickHousePreprocessor()


def makeExtension(**kwargs):
    return ClickHouseMarkdown(**kwargs)
