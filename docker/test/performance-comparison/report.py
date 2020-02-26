#!/usr/bin/python3

import collections
import csv
import os
import sys
import traceback

report_errors = []
status = 'success'
message = 'See the report'

print("""
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
.cancela,.cancela:link,.cancela:visited,.cancela:hover,.cancela:focus,.cancela:active{{
    color: inherit;
    text-decoration: none;
}}
tr:nth-child(odd) td {{filter: brightness(95%);}}
  </style>
  <title>Clickhouse performance comparison</title>
</head>
<body>
<div class="main">

<h1>ClickHouse performance comparison</h1>
""".format())


table_template = """
<h2 id="{anchor}"><a class="cancela" href="#{anchor}">{caption}</a></h2>
<table>
{header}
{rows}
</table>
"""

table_anchor = 0
row_anchor = 0

def nextTableAnchor():
    global table_anchor
    table_anchor += 1
    return str(table_anchor)

def nextRowAnchor():
    global row_anchor
    global table_anchor
    row_anchor += 1
    return str(table_anchor) + "." + str(row_anchor)

def tr(x):
    a = nextRowAnchor()
    #return '<tr onclick="location.href=\'#{a}\'" id={a}>{x}</tr>'.format(a=a, x=str(x))
    return '<tr id={a}>{x}</tr>'.format(a=a, x=str(x))

def td(value, cell_class = ''):
    return '<td class="{cell_class}">{value}</td>'.format(
        cell_class = cell_class,
        value = value)

def th(x):
    return '<th>' + str(x) + '</th>'

def tableRow(r):
    return tr(''.join([td(f) for f in r]))

def tableHeader(r):
    return tr(''.join([th(f) for f in r]))

def tableStart(title):
    return """
<h2 id="{anchor}"><a class="cancela" href="#{anchor}">{title}</a></h2>
<table>""".format(
        anchor = nextTableAnchor(),
        title = title)

def tableEnd():
    return '</table>'

def tsvRowsRaw(n):
    result = []
    try:
        with open(n, encoding='utf-8') as fd:
            return [row for row in csv.reader(fd, delimiter="\t", quotechar='"')]
    except:
        report_errors.append(
            traceback.format_exception_only(
                *sys.exc_info()[:2])[-1])
        pass
    return []

def tsvTableRows(n):
    rawRows = tsvRowsRaw(n)
    result = ''
    for row in rawRows:
        result += tableRow(row)
    return result

print(table_template.format(
        anchor = nextTableAnchor(),
        caption = 'Tested commits',
        header = tableHeader(['Old', 'New']),
        rows = tableRow([open('left-commit.txt').read(), open('right-commit.txt').read()])))

print(table_template.format(
    anchor = nextTableAnchor(),
    caption = 'Changes in performance',
    header = tableHeader(['Old, s', 'New, s',
        'Relative difference (new&nbsp;-&nbsp;old)/old',
        'Randomization distribution quantiles [5%,&nbsp;50%,&nbsp;95%]',
        'Test', 'Query']),
    rows = tsvTableRows('changed-perf.tsv')))

print(table_template.format(
    anchor = nextTableAnchor(),
    caption = 'Slow on client',
    header = tableHeader(['Client time, s', 'Server time, s', 'Ratio', 'Query']),
    rows = tsvTableRows('slow-on-client.tsv')))

print(table_template.format(
    anchor = nextTableAnchor(),
    caption = 'Unstable queries',
    header = tableHeader(['Old, s', 'New, s',
        'Relative difference (new&nbsp;-&nbsp;old)/old',
        'Randomization distribution quantiles [5%,&nbsp;50%,&nbsp;95%]',
        'Test', 'Query']),
    rows = tsvTableRows('unstable-queries.tsv')))

print(table_template.format(
    anchor = nextTableAnchor(),
    caption = 'Run errors',
    header = tableHeader(['A', 'B']),
    rows = tsvTableRows('run-errors.log')))

print(table_template.format(
    anchor = nextTableAnchor(),
    caption = 'Tests with most unstable queries',
    header = tableHeader(['Test', 'Unstable', 'Changed perf', 'Total not OK']),
    rows = tsvTableRows('bad-tests.tsv')))

def print_test_times():
    rows = tsvRowsRaw('test-times.tsv')
    if not rows:
        return

    print(rows, file=sys.stderr)

    print(tableStart('Test times'))
    print(tableHeader([
        'Test',                                          #0
        'Wall clock time, s',                            #1
        'Total client time, s',                          #2
        'Total queries',                                 #3
        'Ignored short queries',                         #4
        'Longest query<br>(sum for all runs), s',        #5
        'Avg wall clock time<br>(sum for all runs), s',  #6
        'Shortest query<br>(sum for all runs), s',       #7
        ]))

    for r in rows:
        print(tableRow(r))

    print(tableEnd())

print_test_times()

if len(report_errors):
    print(tableStart('Errors while building the report'))
    print(tableHeader(['Error']))
    for x in report_errors:
        print(tableRow([x]))
    print(tableEnd())


print("""
<p class="links">
<a href="output.7z">Test output</a>
</p>
</body>
</html>
""")

if report_errors:
    status = 'error'
    message = 'Errors while building the report.'

print("""
<!--status: {status}-->
<!--message: {message}-->
""".format(status=status, message=message))
