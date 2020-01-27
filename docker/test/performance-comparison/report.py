#!/usr/bin/python3

import collections
import csv
import os
import sys

doc_template = """
<!DOCTYPE html>
<html>
  <style>
@font-face {{
    font-family:'Yandex Sans Display Web';
    src:url(https://yastatic.net/adv-www/_/H63jN0veW07XQUIA2317lr9UIm8.eot);
    src:url(https://yastatic.net/adv-www/_/H63jN0veW07XQUIA2317lr9UIm8.eot?#iefix) format('embedded-opentype'),
            url(https://yastatic.net/adv-www/_/sUYVCPUAQE7ExrvMS7FoISoO83s.woff2) format('woff2'),
            url(https://yastatic.net/adv-www/_/v2Sve_obH3rKm6rKrtSQpf-eB7U.woff) format('woff'),
            url(https://yastatic.net/adv-www/_/PzD8hWLMunow5i3RfJ6WQJAL7aI.ttf) format('truetype'),
            url(https://yastatic.net/adv-www/_/lF_KG5g4tpQNlYIgA0e77fBSZ5s.svg#YandexSansDisplayWeb-Regular) format('svg');
    font-weight:400;
    font-style:normal;
    font-stretch:normal
}}

body {{ font-family: "Yandex Sans Display Web", Arial, sans-serif; background: #EEE; }}
h1 {{ margin-left: 10px; }}
th, td {{ border: 0; padding: 5px 10px 5px 10px; text-align: left; vertical-align: top; line-height: 1.5; background-color: #FFF;
td {{ white-space: pre; font-family: Monospace, Courier New; }}
border: 0; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1); }}
a {{ color: #06F; text-decoration: none; }}
a:hover, a:active {{ color: #F40; text-decoration: underline; }}
table {{ border: 0; }}
.main {{ margin-left: 10%; }}
p.links a {{ padding: 5px; margin: 3px; background: #FFF; line-height: 2; white-space: nowrap; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1); }}
  </style>
  <title>{header}</title>
</head>
<body>
<div class="main">

<h1>{header}</h1>
{test_part}
<p class="links">
<a href="{raw_log_url}">{raw_log_name}</a>
<a href="{branch_url}">{branch_name}</a>
<a href="{commit_url}">Commit</a>
{additional_urls}
<a href="output.7z">Test output</a>
<a href="{task_url}">Task (private network)</a>
</p>
</body>
</html>
"""

table_template = """
<h2>{caption}</h2>
<table>
{header}
{rows}
</table>
"""

def tr(x):
    return '<tr>' + str(x) + '</tr>'

def td(x):
    return '<td>' + str(x) + '</td>'

def th(x):
    return '<th>' + str(x) + '</th>'

def table_row(r):
    return tr(''.join([td(f) for f in r]))

def table_header(r):
    return tr(''.join([th(f) for f in r]))

def tsv_rows(n):
    result = ''
    with open(n) as fd:
        for row in csv.reader(fd, delimiter="\t", quotechar='"'):
            result += table_row(row)
    return result

params = collections.defaultdict(str)
params['header'] = "ClickHouse Performance Comparison"
params['test_part'] = (table_template.format_map(
    collections.defaultdict(str,
        caption = 'Changes in performance',
        header = table_header(['Left', 'Right', 'Diff', 'RD', 'Query']),
        rows = tsv_rows('changed-perf.tsv'))) +
    table_template.format(
        caption = 'Slow on client',
        header = table_header(['Client', 'Server', 'Ratio', 'Query']),
        rows = tsv_rows('slow-on-client.tsv')) +
    table_template.format(
        caption = 'Unstable',
        header = table_header(['Left', 'Right', 'Diff', 'RD', 'Query']),
        rows = tsv_rows('unstable.tsv')) +
    table_template.format(
        caption = 'Run errors',
        header = table_header(['A', 'B']),
        rows = tsv_rows('run-errors.log'))
)
print(doc_template.format_map(params))
