#!/usr/bin/python3

import argparse
import ast
import collections
import csv
import itertools
import json
import os
import os.path
import pprint
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
unstable_partial_queries = 0

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

body {{
    font-family: "Yandex Sans Display Web", Arial, sans-serif;
    background: #EEE;
}}

a {{ color: #06F; text-decoration: none; }}

a:hover, a:active {{ color: #F40; text-decoration: underline; }}

.main {{ margin: auto; max-width: 95%; }}

p.links a {{
    padding: 5px; margin: 3px; background: #FFF; line-height: 2;
    white-space: nowrap;
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1);
}}

.cancela,.cancela:link,.cancela:visited,.cancela:hover,
        .cancela:focus,.cancela:active {{
    color: inherit;
    text-decoration: none;
}}

table {{
    border: none;
    border-spacing: 0px;
    line-height: 1.5;
    box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1);
    text-align: left;
}}

th, td {{
    border: none;
    padding: 5px;
    vertical-align: top;
    background-color: #FFF;
    font-family: sans-serif;
}}

th {{
    border-bottom: 2px solid black;
}}

tr:nth-child(odd) td {{filter: brightness(90%);}}

.all-query-times tr :nth-child(1),
.all-query-times tr :nth-child(2),
.all-query-times tr :nth-child(3),
.all-query-times tr :nth-child(4),
.all-query-times tr :nth-child(5),
.all-query-times tr :nth-child(7),
.changes-in-performance tr :nth-child(1),
.changes-in-performance tr :nth-child(2),
.changes-in-performance tr :nth-child(3),
.changes-in-performance tr :nth-child(4),
.changes-in-performance tr :nth-child(5),
.changes-in-performance tr :nth-child(7),
.unstable-queries tr :nth-child(1),
.unstable-queries tr :nth-child(2),
.unstable-queries tr :nth-child(3),
.unstable-queries tr :nth-child(4),
.unstable-queries tr :nth-child(6),
.test-performance-changes tr :nth-child(2),
.test-performance-changes tr :nth-child(3),
.test-performance-changes tr :nth-child(4),
.test-performance-changes tr :nth-child(5),
.test-performance-changes tr :nth-child(6),
.test-times tr :nth-child(2),
.test-times tr :nth-child(3),
.test-times tr :nth-child(4),
.test-times tr :nth-child(5),
.test-times tr :nth-child(6),
.test-times tr :nth-child(7),
.test-times tr :nth-child(8),
.concurrent-benchmarks tr :nth-child(2),
.concurrent-benchmarks tr :nth-child(3),
.concurrent-benchmarks tr :nth-child(4),
.concurrent-benchmarks tr :nth-child(5),
.metric-changes tr :nth-child(2),
.metric-changes tr :nth-child(3),
.metric-changes tr :nth-child(4),
.metric-changes tr :nth-child(5)
{{ text-align: right; }}

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
        if a is not None and v is not None]))

def tableHeader(r):
    return tr(''.join([th(f) for f in r]))

def tableStart(title):
    anchor = nextTableAnchor();
    cls = '-'.join(title.lower().split(' ')[:3]);
    return f"""
        <h2 id="{anchor}">
            <a class="cancela" href="#{anchor}">{title}</a>
        </h2>
        <table class="{cls}">
    """

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
                     ['Client time,&nbsp;s', 'Server time,&nbsp;s', 'Ratio', 'Test', 'Query'],
                     slow_on_client_rows)

    unmarked_short_rows = tsvRows('report/unmarked-short-queries.tsv')
    error_tests += len(unmarked_short_rows)
    printSimpleTable('Short queries not marked as short',
        ['New client time, s', 'Test', '#', 'Query'],
        unmarked_short_rows)

    def print_partial():
        rows = tsvRows('report/partial-queries-report.tsv')
        if not rows:
            return
        global unstable_partial_queries, slow_average_tests
        print(tableStart('Partial queries'))
        columns = ['Median time, s', 'Relative time variance', 'Test', '#', 'Query']
        print(tableHeader(columns))
        attrs = ['' for c in columns]
        for row in rows:
            if float(row[1]) > 0.10:
                attrs[1] = f'style="background: {color_bad}"'
                unstable_partial_queries += 1
            else:
                attrs[1] = ''
            if float(row[0]) > allowed_single_run_time:
                attrs[0] = f'style="background: {color_bad}"'
                slow_average_tests += 1
            else:
                attrs[0] = ''
            print(tableRow(row, attrs))
        print(tableEnd())

    print_partial()

    def print_changes():
        rows = tsvRows('report/changed-perf.tsv')
        if not rows:
            return

        global faster_queries, slower_queries

        print(tableStart('Changes in performance'))
        columns = [
            'Old,&nbsp;s',                                          # 0
            'New,&nbsp;s',                                          # 1
            'Times speedup / slowdown',                 # 2
            'Relative difference (new&nbsp;&minus;&nbsp;old) / old',   # 3
            'p&nbsp;<&nbsp;0.001 threshold',                   # 4
            # Failed                                           # 5
            'Test',                                            # 6
            '#',                                               # 7
            'Query',                                           # 8
            ]

        print(tableHeader(columns))

        attrs = ['' for c in columns]
        attrs[5] = None
        for row in rows:
            if int(row[5]):
                if float(row[3]) < 0.:
                    faster_queries += 1
                    attrs[2] = attrs[3] = f'style="background: {color_good}"'
                else:
                    slower_queries += 1
                    attrs[2] = attrs[3] = f'style="background: {color_bad}"'
            else:
                attrs[2] = attrs[3] = ''

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
            'Old,&nbsp;s', #0
            'New,&nbsp;s', #1
            'Relative difference (new&nbsp;-&nbsp;old)/old', #2
            'p&nbsp;&lt;&nbsp;0.001 threshold', #3
            # Failed #4
            'Test', #5
            '#',    #6
            'Query' #7
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

    printSimpleTable('Test performance changes',
        ['Test', 'Queries', 'Unstable', 'Changed perf', 'Total not OK', 'Avg relative time diff'],
        tsvRows('report/test-perf-changes.tsv'))

    def print_test_times():
        global slow_average_tests
        rows = tsvRows('report/test-times.tsv')
        if not rows:
            return

        columns = [
            'Test',                                          #0
            'Wall clock time,&nbsp;s',                            #1
            'Total client time,&nbsp;s',                          #2
            'Total queries',                                 #3
            'Ignored short queries',                         #4
            'Longest query<br>(sum for all runs),&nbsp;s',        #5
            'Avg wall clock time<br>(sum for all runs),&nbsp;s',  #6
            'Shortest query<br>(sum for all runs),&nbsp;s',       #7
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

    def print_benchmark_results():
        if not os.path.isfile('benchmark/website-left.json'):
            return

        json_reports = [json.load(open(f'benchmark/website-{x}.json')) for x in ['left', 'right']]
        stats = [next(iter(x.values()))["statistics"] for x in json_reports]
        qps = [x["QPS"] for x in stats]
        queries = [x["num_queries"] for x in stats]
        errors = [x["num_errors"] for x in stats]
        relative_diff = (qps[1] - qps[0]) / max(0.01, qps[0]);
        times_diff = max(qps) / max(0.01, min(qps))

        all_rows = []
        header = ['Benchmark', 'Metric', 'Old', 'New', 'Relative difference', 'Times difference'];

        attrs = ['' for x in header]
        row = ['website', 'queries', f'{queries[0]:d}', f'{queries[1]:d}', '--', '--']
        attrs[0] = 'rowspan=2'
        all_rows.append([row, attrs])

        attrs = ['' for x in header]
        row = [None, 'queries/s', f'{qps[0]:.3f}', f'{qps[1]:.3f}', f'{relative_diff:.3f}', f'x{times_diff:.3f}']
        if abs(relative_diff) > 0.1:
            # More queries per second is better.
            if relative_diff > 0.:
                attrs[4] = f'style="background: {color_good}"'
            else:
                attrs[4] = f'style="background: {color_bad}"'
        else:
            attrs[4] = ''
        all_rows.append([row, attrs]);

        if max(errors):
            all_rows[0][1][0] = "rowspan=3"
            row = [''] * (len(header))
            attrs = ['' for x in header]

            attrs[0] = None
            row[1] = 'errors'
            row[2] = f'{errors[0]:d}'
            row[3] = f'{errors[1]:d}'
            row[4] = '--'
            row[5] = '--'
            if errors[0]:
                attrs[2] += f' style="background: {color_bad}" '
            if errors[1]:
                attrs[3] += f' style="background: {color_bad}" '

            all_rows.append([row, attrs])

        print(tableStart('Concurrent benchmarks'))
        print(tableHeader(header))
        for row, attrs in all_rows:
            print(tableRow(row, attrs))
        print(tableEnd())

    try:
        print_benchmark_results()
    except:
        report_errors.append(
            traceback.format_exception_only(
                *sys.exc_info()[:2])[-1])
        pass

    printSimpleTable('Metric changes',
        ['Metric', 'Old median value', 'New median value',
            'Relative difference', 'Times difference'],
        tsvRows('metrics/changes.tsv'))

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

    if unstable_partial_queries:
        unstable_queries += unstable_partial_queries
        error_tests += unstable_partial_queries
        status = 'failure'

    if unstable_queries:
        message_array.append(str(unstable_queries) + ' unstable')

#    Disabled before fix.
#    if very_unstable_queries:
#        status = 'failure'

    error_tests += slow_average_tests
    if error_tests:
        status = 'failure'
        message_array.insert(0, str(error_tests) + ' errors')

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
            'Old,&nbsp;s', #2
            'New,&nbsp;s', #3
            'Times speedup / slowdown',                 #4
            'Relative difference (new&nbsp;&minus;&nbsp;old) / old', #5
            'p&nbsp;&lt;&nbsp;0.001 threshold',          #6
            'Test',                                   #7
            '#',                                      #8
            'Query',                                  #9
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
                if float(r[5]) > 0.:
                    attrs[4] = attrs[5] = f'style="background: {color_bad}"'
                else:
                    attrs[4] = attrs[5] = f'style="background: {color_good}"'
            else:
                attrs[4] = attrs[5] = ''

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
