#!/usr/bin/python3

import argparse
import ast
import collections
import csv
import itertools
import os
import sys
import traceback

parser = argparse.ArgumentParser(description='Create performance test report')
parser.add_argument('--report', default='main', choices=['main', 'all-queries'],
    help='Which report to build')
args = parser.parse_args()

report_errors = []
error_tests = 0
slow_average_tests = 0
faster_queries = 0
slower_queries = 0
unstable_queries = 0
very_unstable_queries = 0

# max seconds to run one query by itself, not counting preparation
allowed_single_run_time = 2

color_bad='#ffb0c0'
color_good='#b0d050'

header_template = """
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
th, td {{ border: 0; padding: 5px 10px 5px 10px; text-align: left; vertical-align: top; line-height: 1.5; background-color: #FFF;
td {{ white-space: pre; font-family: Monospace, Courier New; }}
border: 0; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1); }}
a {{ color: #06F; text-decoration: none; }}
a:hover, a:active {{ color: #F40; text-decoration: underline; }}
table {{ border: 0; }}
.main {{ margin: auto; max-width: 95%; }}
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
            fillvalue = '')
        if a is not None]))

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

def print_tested_commits():
    global report_errors
    try:
        printSimpleTable('Tested commits', ['Old', 'New'],
            [['<pre>{}</pre>'.format(x) for x in
                [open('left-commit.txt').read(),
                 open('right-commit.txt').read()]]])
    except:
        # Don't fail if no commit info -- maybe it's a manual run.
        report_errors.append(
            traceback.format_exception_only(
                *sys.exc_info()[:2])[-1])
        pass

def print_report_errors():
    global report_errors
    # Add the errors reported by various steps of comparison script
    try:
        report_errors += [l.strip() for l in open('report/errors.log')]
    except:
        report_errors.append(
            traceback.format_exception_only(
                *sys.exc_info()[:2])[-1])
        pass

    if len(report_errors):
        print(tableStart('Errors while building the report'))
        print(tableHeader(['Error']))
        for x in report_errors:
            print(tableRow([x]))
        print(tableEnd())

if args.report == 'main':
    print(header_template.format())

    print_tested_commits()

    run_error_rows = tsvRows('run-errors.tsv')
    error_tests += len(run_error_rows)
    printSimpleTable('Run errors', ['Test', 'Error'], run_error_rows)

    slow_on_client_rows = tsvRows('report/slow-on-client.tsv')
    error_tests += len(slow_on_client_rows)
    printSimpleTable('Slow on client',
                     ['Client time, s', 'Server time, s', 'Ratio', 'Test', 'Query'],
                     slow_on_client_rows)

    def print_changes():
        rows = tsvRows('report/changed-perf.tsv')
        if not rows:
            return

        global faster_queries, slower_queries

        print(tableStart('Changes in performance'))
        columns = [
            'Old, s',                                          # 0
            'New, s',                                          # 1
            'Relative difference (new&nbsp;&minus;&nbsp;old) / old',   # 2
            'p&nbsp;<&nbsp;0.001 threshold',                   # 3
            # Failed                                           # 4
            'Test',                                            # 5
            'Query',                                           # 6
            ]

        print(tableHeader(columns))

        attrs = ['' for c in columns]
        attrs[4] = None
        for row in rows:
            if int(row[4]):
                if float(row[2]) < 0.:
                    faster_queries += 1
                    attrs[2] = f'style="background: {color_good}"'
                else:
                    slower_queries += 1
                    attrs[2] = f'style="background: {color_bad}"'
            else:
                attrs[2] = ''

            print(tableRow(row, attrs))

        print(tableEnd())

    print_changes()

    def print_unstable_queries():
        global unstable_queries
        global very_unstable_queries

        unstable_rows = tsvRows('report/unstable-queries.tsv')
        if not unstable_rows:
            return

        unstable_queries += len(unstable_rows)

        columns = [
            'Old, s', #0
            'New, s', #1
            'Relative difference (new&nbsp;-&nbsp;old)/old', #2
            'p&nbsp;<&nbsp;0.001 threshold', #3
            # Failed #4
            'Test', #5
            'Query' #6
        ]

        print(tableStart('Unstable queries'))
        print(tableHeader(columns))

        attrs = ['' for c in columns]
        attrs[4] = None
        for r in unstable_rows:
            if int(r[4]):
                very_unstable_queries += 1
                attrs[3] = f'style="background: {color_bad}"'
            else:
                attrs[3] = ''

            print(tableRow(r, attrs))

        print(tableEnd())

    print_unstable_queries()

    skipped_tests_rows = tsvRows('analyze/skipped-tests.tsv')
    printSimpleTable('Skipped tests', ['Test', 'Reason'], skipped_tests_rows)

    printSimpleTable('Tests with most unstable queries',
        ['Test', 'Unstable', 'Changed perf', 'Total not OK'],
        tsvRows('report/bad-tests.tsv'))

    def print_test_times():
        global slow_average_tests
        rows = tsvRows('report/test-times.tsv')
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

        nominal_runs = 13  # FIXME pass this as an argument
        total_runs = (nominal_runs + 1) * 2  # one prewarm run, two servers
        attrs = ['' for c in columns]
        for r in rows:
            if float(r[6]) > 1.5 * total_runs:
                # FIXME should be 15s max -- investigate parallel_insert
                slow_average_tests += 1
                attrs[6] = f'style="background: {color_bad}"'
            else:
                attrs[6] = ''

            if float(r[5]) > allowed_single_run_time * total_runs:
                slow_average_tests += 1
                attrs[5] = f'style="background: {color_bad}"'
            else:
                attrs[5] = ''

            print(tableRow(r, attrs))

        print(tableEnd())

    print_test_times()

    print_report_errors()

    print("""
    <p class="links">
    <a href="all-queries.html">All queries</a>
    <a href="compare.log">Log</a>
    <a href="output.7z">Test output</a>
    </p>
    </body>
    </html>
    """)

    status = 'success'
    message = 'See the report'
    message_array = []

    if slow_average_tests:
        status = 'failure'
        message_array.append(str(slow_average_tests) + ' too long')

    if faster_queries:
        message_array.append(str(faster_queries) + ' faster')

    if slower_queries:
        if slower_queries > 3:
            status = 'failure'
        message_array.append(str(slower_queries) + ' slower')

    if unstable_queries:
        message_array.append(str(unstable_queries) + ' unstable')

#    Disabled before fix.
#    if very_unstable_queries:
#        status = 'failure'

    error_tests += slow_average_tests
    if error_tests:
        status = 'failure'
        message_array.append(str(error_tests) + ' errors')

    if message_array:
        message = ', '.join(message_array)

    if report_errors:
        status = 'failure'
        message = 'Errors while building the report.'

    print("""
    <!--status: {status}-->
    <!--message: {message}-->
    """.format(status=status, message=message))

elif args.report == 'all-queries':

    print(header_template.format())

    print_tested_commits()

    def print_all_queries():
        rows = tsvRows('report/all-queries.tsv')
        if not rows:
            return

        columns = [
            # Changed #0
            # Unstable #1
            'Old, s', #2
            'New, s', #3
            'Relative difference (new&nbsp;&minus;&nbsp;old) / old', #4
            'Times speedup / slowdown',                 #5
            'p&nbsp;<&nbsp;0.001 threshold',          #6
            'Test',                                   #7
            'Query',                                  #8
            ]

        print(tableStart('All query times'))
        print(tableHeader(columns))

        attrs = ['' for c in columns]
        attrs[0] = None
        attrs[1] = None
        for r in rows:
            if int(r[1]):
                attrs[6] = f'style="background: {color_bad}"'
            else:
                attrs[6] = ''

            if int(r[0]):
                if float(r[4]) > 0.:
                    attrs[4] = f'style="background: {color_bad}"'
                else:
                    attrs[4] = f'style="background: {color_good}"'
            else:
                attrs[4] = ''

            if (float(r[2]) + float(r[3])) / 2 > allowed_single_run_time:
                attrs[2] = f'style="background: {color_bad}"'
                attrs[3] = f'style="background: {color_bad}"'
            else:
                attrs[2] = ''
                attrs[3] = ''

            print(tableRow(r, attrs))

        print(tableEnd())

    print_all_queries()

    print_report_errors()

    print("""
    <p class="links">
    <a href="report.html">Main report</a>
    <a href="compare.log">Log</a>
    <a href="output.7z">Test output</a>
    </p>
    </body>
    </html>
    """)
