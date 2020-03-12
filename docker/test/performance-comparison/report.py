#!/usr/bin/python3

import collections
import csv
import itertools
import os
import sys
import traceback

report_errors = []
status = 'success'
message = 'See the report'
message_array = []
error_tests = 0
slow_average_tests = 0
faster_queries = 0
slower_queries = 0
unstable_queries = 0

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

def td(value, cell_attributes = ''):
    return '<td {cell_attributes}>{value}</td>'.format(
        cell_attributes = cell_attributes,
        value = value)

def th(x):
    return '<th>' + str(x) + '</th>'

def tableRow(cell_values, cell_attributes = []):
    return tr(''.join([td(v, a)
        for v, a in itertools.zip_longest(
            cell_values, cell_attributes,
            fillvalue = '')]))

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

def tsvRows(n):
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

def htmlRows(n):
    rawRows = tsvRows(n)
    result = ''
    for row in rawRows:
        result += tableRow(row)
    return result

def printSimpleTable(caption, columns, rows):
    if not rows:
        return

    print(tableStart(caption))
    print(tableHeader(columns))
    for row in rows:
        print(tableRow(row))
    print(tableEnd())

printSimpleTable('Tested commits', ['Old', 'New'],
    [[open('left-commit.txt').read(), open('right-commit.txt').read()]])

def print_changes():
    rows = tsvRows('changed-perf.tsv')
    if not rows:
        return

    global faster_queries, slower_queries

    print(tableStart('Changes in performance'))
    columns = [
        'Old, s',                                                        # 0
        'New, s',                                                        # 1
        'Relative difference (new&nbsp;-&nbsp;old)/old',                 # 2
        'Randomization distribution quantiles \
            [5%,&nbsp;50%,&nbsp;95%,&nbsp;99%]',                          # 3
        'Test',                                                          # 4
        'Query',                                                         # 5
        ]

    print(tableHeader(columns))

    attrs = ['' for c in columns]
    for row in rows:
        if float(row[2]) < 0.:
            faster_queries += 1
            attrs[2] = 'style="background: #adbdff"'
        else:
            slower_queries += 1
            attrs[2] = 'style="background: #ffb0a0"'

        print(tableRow(row, attrs))

    print(tableEnd())

print_changes()

slow_on_client_rows = tsvRows('slow-on-client.tsv')
error_tests += len(slow_on_client_rows)
printSimpleTable('Slow on client',
    ['Client time, s', 'Server time, s', 'Ratio', 'Query'],
    slow_on_client_rows)

unstable_rows = tsvRows('unstable-queries.tsv')
unstable_queries += len(unstable_rows)
printSimpleTable('Unstable queries',
    [
        'Old, s', 'New, s', 'Relative difference (new&nbsp;-&nbsp;old)/old',
        'Randomization distribution quantiles [5%,&nbsp;50%,&nbsp;95%,&nbsp;99%]',
        'Test', 'Query'
    ],
    unstable_rows)

run_error_rows = tsvRows('run-errors.tsv')
error_tests += len(run_error_rows)
printSimpleTable('Run errors', ['Test', 'Error'], run_error_rows)

skipped_tests_rows = tsvRows('skipped-tests.tsv')
printSimpleTable('Skipped tests', ['Test', 'Reason'], skipped_tests_rows)

printSimpleTable('Tests with most unstable queries',
    ['Test', 'Unstable', 'Changed perf', 'Total not OK'],
    tsvRows('bad-tests.tsv'))

def print_test_times():
    global slow_average_tests
    rows = tsvRows('test-times.tsv')
    if not rows:
        return

    columns = [
        'Test',                                          #0
        'Wall clock time, s',                            #1
        'Total client time, s',                          #2
        'Total queries',                                 #3
        'Ignored short queries',                         #4
        'Longest query<br>(sum for all runs), s',        #5
        'Avg wall clock time<br>(sum for all runs), s',  #6
        'Shortest query<br>(sum for all runs), s',       #7
        ]

    print(tableStart('Test times'))
    print(tableHeader(columns))

    attrs = ['' for c in columns]
    for r in rows:
        if float(r[6]) > 15:
            slow_average_tests += 1
            attrs[6] = 'style="background: #ffb0a0"'
        else:
            attrs[6] = ''

        if float(r[5]) > 30:
            # Just a hint for now.
            # slow_average_tests += 1
            attrs[5] = 'style="background: #ffb0a0"'
        else:
            attrs[5] = ''

        print(tableRow(r, attrs))

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

if slow_average_tests:
    #status = 'failure'
    message_array.append(str(slow_average_tests) + ' 🕐')

if faster_queries:
    message_array.append(str(faster_queries) + ' 🐇')

if slower_queries:
    message_array.append(str(slower_queries) + ' 🐌')

if unstable_queries:
    message_array.append(str(unstable_queries) + ' ❓')

error_tests += slow_average_tests
if error_tests:
    message_array.append(str(error_tests) + ' ❌')

if message_array:
    message = ', '.join(message_array)

if report_errors:
    status = 'failure'
    message = 'Errors while building the report.'

print("""
<!--status: {status}-->
<!--message: {message}-->
""".format(status=status, message=message))
