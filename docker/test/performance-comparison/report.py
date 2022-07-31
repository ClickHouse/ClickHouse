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

parser = argparse.ArgumentParser(description="Create performance test report")
parser.add_argument(
    "--report",
    default="main",
    choices=["main", "all-queries"],
    help="Which report to build",
)
args = parser.parse_args()

tables = []
errors_explained = []
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

color_bad = "#ffb0c0"
color_good = "#b0d050"

header_template = """
<!DOCTYPE html>
<html lang="en">
<link rel="preload" as="font" href="https://yastatic.net/adv-www/_/sUYVCPUAQE7ExrvMS7FoISoO83s.woff2" type="font/woff2" crossorigin="anonymous"/>
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
    font-stretch:normal;
    font-display: swap;
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

.unexpected-query-duration tr :nth-child(2),
.unexpected-query-duration tr :nth-child(3),
.unexpected-query-duration tr :nth-child(5),
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


def currentTableAnchor():
    global table_anchor
    return f"{table_anchor}"


def newTableAnchor():
    global table_anchor
    table_anchor += 1
    return currentTableAnchor()


def currentRowAnchor():
    global row_anchor
    global table_anchor
    return f"{table_anchor}.{row_anchor}"


def nextRowAnchor():
    global row_anchor
    global table_anchor
    return f"{table_anchor}.{row_anchor + 1}"


def advanceRowAnchor():
    global row_anchor
    global table_anchor
    row_anchor += 1
    return currentRowAnchor()


def tr(x, anchor=None):
    # return '<tr onclick="location.href=\'#{a}\'" id={a}>{x}</tr>'.format(a=a, x=str(x))
    anchor = anchor if anchor else advanceRowAnchor()
    return f"<tr id={anchor}>{x}</tr>"


def td(value, cell_attributes=""):
    return "<td {cell_attributes}>{value}</td>".format(
        cell_attributes=cell_attributes, value=value
    )


def th(value, cell_attributes=""):
    return "<th {cell_attributes}>{value}</th>".format(
        cell_attributes=cell_attributes, value=value
    )


def tableRow(cell_values, cell_attributes=[], anchor=None):
    return tr(
        "".join(
            [
                td(v, a)
                for v, a in itertools.zip_longest(
                    cell_values, cell_attributes, fillvalue=""
                )
                if a is not None and v is not None
            ]
        ),
        anchor,
    )


def tableHeader(cell_values, cell_attributes=[]):
    return tr(
        "".join(
            [
                th(v, a)
                for v, a in itertools.zip_longest(
                    cell_values, cell_attributes, fillvalue=""
                )
                if a is not None and v is not None
            ]
        )
    )


def tableStart(title):
    cls = "-".join(title.lower().split(" ")[:3])
    global table_anchor
    table_anchor = cls
    anchor = currentTableAnchor()
    help_anchor = "-".join(title.lower().split(" "))
    return f"""
        <h2 id="{anchor}">
            <a class="cancela" href="#{anchor}">{title}</a>
            <a class="cancela" href="https://github.com/ClickHouse/ClickHouse/tree/master/docker/test/performance-comparison#{help_anchor}"><sup style="color: #888">?</sup></a>
        </h2>
        <table class="{cls}">
    """


def tableEnd():
    return "</table>"


def tsvRows(n):
    try:
        with open(n, encoding="utf-8") as fd:
            result = []
            for row in csv.reader(fd, delimiter="\t", quoting=csv.QUOTE_NONE):
                new_row = []
                for e in row:
                    # The first one .encode('utf-8').decode('unicode-escape') decodes the escape characters from the strings.
                    # The second one (encode('latin1').decode('utf-8')) fixes the changes with unicode vs utf-8 chars, so
                    # 'Ð§ÐµÐ¼ Ð·Ð�Ð½Ð¸Ð¼Ð°ÐµÑ�Ð¬Ñ�Ñ�' is transformed back into 'Чем зАнимаешЬся'.

                    new_row.append(
                        e.encode("utf-8")
                        .decode("unicode-escape")
                        .encode("latin1")
                        .decode("utf-8")
                    )
                result.append(new_row)
        return result

    except:
        report_errors.append(traceback.format_exception_only(*sys.exc_info()[:2])[-1])
        pass
    return []


def htmlRows(n):
    rawRows = tsvRows(n)
    result = ""
    for row in rawRows:
        result += tableRow(row)
    return result


def addSimpleTable(caption, columns, rows, pos=None):
    global tables
    text = ""
    if not rows:
        return

    text += tableStart(caption)
    text += tableHeader(columns)
    for row in rows:
        text += tableRow(row)
    text += tableEnd()
    tables.insert(pos if pos else len(tables), text)


def add_tested_commits():
    global report_errors
    try:
        addSimpleTable(
            "Tested Commits",
            ["Old", "New"],
            [
                [
                    "<pre>{}</pre>".format(x)
                    for x in [
                        open("left-commit.txt").read(),
                        open("right-commit.txt").read(),
                    ]
                ]
            ],
        )
    except:
        # Don't fail if no commit info -- maybe it's a manual run.
        report_errors.append(traceback.format_exception_only(*sys.exc_info()[:2])[-1])
        pass


def add_report_errors():
    global tables
    global report_errors
    # Add the errors reported by various steps of comparison script
    try:
        report_errors += [l.strip() for l in open("report/errors.log")]
    except:
        report_errors.append(traceback.format_exception_only(*sys.exc_info()[:2])[-1])
        pass

    if not report_errors:
        return

    text = tableStart("Errors while Building the Report")
    text += tableHeader(["Error"])
    for x in report_errors:
        text += tableRow([x])
    text += tableEnd()
    # Insert after Tested Commits
    tables.insert(1, text)
    errors_explained.append(
        [
            f'<a href="#{currentTableAnchor()}">There were some errors while building the report</a>'
        ]
    )


def add_errors_explained():
    if not errors_explained:
        return

    text = '<a name="fail1"/>'
    text += tableStart("Error Summary")
    text += tableHeader(["Description"])
    for row in errors_explained:
        text += tableRow(row)
    text += tableEnd()

    global tables
    tables.insert(1, text)


if args.report == "main":
    print((header_template.format()))

    add_tested_commits()

    run_error_rows = tsvRows("run-errors.tsv")
    error_tests += len(run_error_rows)
    addSimpleTable("Run Errors", ["Test", "Error"], run_error_rows)
    if run_error_rows:
        errors_explained.append(
            [
                f'<a href="#{currentTableAnchor()}">There were some errors while running the tests</a>'
            ]
        )

    slow_on_client_rows = tsvRows("report/slow-on-client.tsv")
    error_tests += len(slow_on_client_rows)
    addSimpleTable(
        "Slow on Client",
        ["Client time,&nbsp;s", "Server time,&nbsp;s", "Ratio", "Test", "Query"],
        slow_on_client_rows,
    )
    if slow_on_client_rows:
        errors_explained.append(
            [
                f'<a href="#{currentTableAnchor()}">Some queries are taking noticeable time client-side (missing `FORMAT Null`?)</a>'
            ]
        )

    unmarked_short_rows = tsvRows("report/unexpected-query-duration.tsv")
    error_tests += len(unmarked_short_rows)
    addSimpleTable(
        "Unexpected Query Duration",
        ["Problem", 'Marked as "short"?', "Run time, s", "Test", "#", "Query"],
        unmarked_short_rows,
    )
    if unmarked_short_rows:
        errors_explained.append(
            [
                f'<a href="#{currentTableAnchor()}">Some queries have unexpected duration</a>'
            ]
        )

    def add_partial():
        rows = tsvRows("report/partial-queries-report.tsv")
        if not rows:
            return

        global unstable_partial_queries, slow_average_tests, tables
        text = tableStart("Partial Queries")
        columns = ["Median time, s", "Relative time variance", "Test", "#", "Query"]
        text += tableHeader(columns)
        attrs = ["" for c in columns]
        for row in rows:
            anchor = f"{currentTableAnchor()}.{row[2]}.{row[3]}"
            if float(row[1]) > 0.10:
                attrs[1] = f'style="background: {color_bad}"'
                unstable_partial_queries += 1
                errors_explained.append(
                    [
                        f"<a href=\"#{anchor}\">The query no. {row[3]} of test '{row[2]}' has excessive variance of run time. Keep it below 10%</a>"
                    ]
                )
            else:
                attrs[1] = ""
            if float(row[0]) > allowed_single_run_time:
                attrs[0] = f'style="background: {color_bad}"'
                errors_explained.append(
                    [
                        f'<a href="#{anchor}">The query no. {row[3]} of test \'{row[2]}\' is taking too long to run. Keep the run time below {allowed_single_run_time} seconds"</a>'
                    ]
                )
                slow_average_tests += 1
            else:
                attrs[0] = ""
            text += tableRow(row, attrs, anchor)
        text += tableEnd()
        tables.append(text)

    add_partial()

    def add_changes():
        rows = tsvRows("report/changed-perf.tsv")
        if not rows:
            return

        global faster_queries, slower_queries, tables

        text = tableStart("Changes in Performance")
        columns = [
            "Old,&nbsp;s",  # 0
            "New,&nbsp;s",  # 1
            "Ratio of speedup&nbsp;(-) or slowdown&nbsp;(+)",  # 2
            "Relative difference (new&nbsp;&minus;&nbsp;old) / old",  # 3
            "p&nbsp;<&nbsp;0.01 threshold",  # 4
            "",  # Failed                                           # 5
            "Test",  # 6
            "#",  # 7
            "Query",  # 8
        ]
        attrs = ["" for c in columns]
        attrs[5] = None

        text += tableHeader(columns, attrs)

        for row in rows:
            anchor = f"{currentTableAnchor()}.{row[6]}.{row[7]}"
            if int(row[5]):
                if float(row[3]) < 0.0:
                    faster_queries += 1
                    attrs[2] = attrs[3] = f'style="background: {color_good}"'
                else:
                    slower_queries += 1
                    attrs[2] = attrs[3] = f'style="background: {color_bad}"'
                    errors_explained.append(
                        [
                            f"<a href=\"#{anchor}\">The query no. {row[7]} of test '{row[6]}' has slowed down</a>"
                        ]
                    )
            else:
                attrs[2] = attrs[3] = ""

            text += tableRow(row, attrs, anchor)

        text += tableEnd()
        tables.append(text)

    add_changes()

    def add_unstable_queries():
        global unstable_queries, very_unstable_queries, tables

        unstable_rows = tsvRows("report/unstable-queries.tsv")
        if not unstable_rows:
            return

        unstable_queries += len(unstable_rows)

        columns = [
            "Old,&nbsp;s",  # 0
            "New,&nbsp;s",  # 1
            "Relative difference (new&nbsp;-&nbsp;old)/old",  # 2
            "p&nbsp;&lt;&nbsp;0.01 threshold",  # 3
            "",  # Failed #4
            "Test",  # 5
            "#",  # 6
            "Query",  # 7
        ]
        attrs = ["" for c in columns]
        attrs[4] = None

        text = tableStart("Unstable Queries")
        text += tableHeader(columns, attrs)

        for r in unstable_rows:
            anchor = f"{currentTableAnchor()}.{r[5]}.{r[6]}"
            if int(r[4]):
                very_unstable_queries += 1
                attrs[3] = f'style="background: {color_bad}"'
            else:
                attrs[3] = ""
                # Just don't add the slightly unstable queries we don't consider
                # errors. It's not clear what the user should do with them.
                continue

            text += tableRow(r, attrs, anchor)

        text += tableEnd()

        # Don't add an empty table.
        if very_unstable_queries:
            tables.append(text)

    add_unstable_queries()

    skipped_tests_rows = tsvRows("analyze/skipped-tests.tsv")
    addSimpleTable("Skipped Tests", ["Test", "Reason"], skipped_tests_rows)

    addSimpleTable(
        "Test Performance Changes",
        [
            "Test",
            "Ratio of speedup&nbsp;(-) or slowdown&nbsp;(+)",
            "Queries",
            "Total not OK",
            "Changed perf",
            "Unstable",
        ],
        tsvRows("report/test-perf-changes.tsv"),
    )

    def add_test_times():
        global slow_average_tests, tables
        rows = tsvRows("report/test-times.tsv")
        if not rows:
            return

        columns = [
            "Test",  # 0
            "Wall clock time, entire test,&nbsp;s",  # 1
            "Total client time for measured query runs,&nbsp;s",  # 2
            "Queries",  # 3
            "Longest query, total for measured runs,&nbsp;s",  # 4
            "Wall clock time per query,&nbsp;s",  # 5
            "Shortest query, total for measured runs,&nbsp;s",  # 6
            "",  # Runs                                               #7
        ]
        attrs = ["" for c in columns]
        attrs[7] = None

        text = tableStart("Test Times")
        text += tableHeader(columns, attrs)

        allowed_average_run_time = 3.75  # 60 seconds per test at (7 + 1) * 2 runs
        for r in rows:
            anchor = f"{currentTableAnchor()}.{r[0]}"
            total_runs = (int(r[7]) + 1) * 2  # one prewarm run, two servers
            if r[0] != "Total" and float(r[5]) > allowed_average_run_time * total_runs:
                # FIXME should be 15s max -- investigate parallel_insert
                slow_average_tests += 1
                attrs[5] = f'style="background: {color_bad}"'
                errors_explained.append(
                    [
                        f"<a href=\"#{anchor}\">The test '{r[0]}' is too slow to run as a whole. Investigate whether the create and fill queries can be sped up"
                    ]
                )
            else:
                attrs[5] = ""

            if r[0] != "Total" and float(r[4]) > allowed_single_run_time * total_runs:
                slow_average_tests += 1
                attrs[4] = f'style="background: {color_bad}"'
                errors_explained.append(
                    [
                        f"<a href=\"./all-queries.html#all-query-times.{r[0]}.0\">Some query of the test '{r[0]}' is too slow to run. See the all queries report"
                    ]
                )
            else:
                attrs[4] = ""

            text += tableRow(r, attrs, anchor)

        text += tableEnd()
        tables.append(text)

    add_test_times()

    addSimpleTable(
        "Metric Changes",
        [
            "Metric",
            "Old median value",
            "New median value",
            "Relative difference",
            "Times difference",
        ],
        tsvRows("metrics/changes.tsv"),
    )

    add_report_errors()
    add_errors_explained()

    for t in tables:
        print(t)

    print(
        f"""
    </div>
    <p class="links">
    <a href="all-queries.html">All queries</a>
    <a href="compare.log">Log</a>
    <a href="output.7z">Test output</a>
    {os.getenv("CHPC_ADD_REPORT_LINKS") or ''}
    </p>
    </body>
    </html>
    """
    )

    status = "success"
    message = "See the report"
    message_array = []

    if slow_average_tests:
        status = "failure"
        message_array.append(str(slow_average_tests) + " too long")

    if faster_queries:
        message_array.append(str(faster_queries) + " faster")

    if slower_queries:
        if slower_queries > 3:
            status = "failure"
        message_array.append(str(slower_queries) + " slower")

    if unstable_partial_queries:
        very_unstable_queries += unstable_partial_queries
        status = "failure"

    # Don't show mildly unstable queries, only the very unstable ones we
    # treat as errors.
    if very_unstable_queries:
        if very_unstable_queries > 5:
            error_tests += very_unstable_queries
            status = "failure"
        message_array.append(str(very_unstable_queries) + " unstable")

    error_tests += slow_average_tests
    if error_tests:
        status = "failure"
        message_array.insert(0, str(error_tests) + " errors")

    if message_array:
        message = ", ".join(message_array)

    if report_errors:
        status = "failure"
        message = "Errors while building the report."

    print(
        (
            """
    <!--status: {status}-->
    <!--message: {message}-->
    """.format(
                status=status, message=message
            )
        )
    )

elif args.report == "all-queries":

    print((header_template.format()))

    add_tested_commits()

    def add_all_queries():
        rows = tsvRows("report/all-queries.tsv")
        if not rows:
            return

        columns = [
            "",  # Changed #0
            "",  # Unstable #1
            "Old,&nbsp;s",  # 2
            "New,&nbsp;s",  # 3
            "Ratio of speedup&nbsp;(-) or slowdown&nbsp;(+)",  # 4
            "Relative difference (new&nbsp;&minus;&nbsp;old) / old",  # 5
            "p&nbsp;&lt;&nbsp;0.01 threshold",  # 6
            "Test",  # 7
            "#",  # 8
            "Query",  # 9
        ]
        attrs = ["" for c in columns]
        attrs[0] = None
        attrs[1] = None

        text = tableStart("All Query Times")
        text += tableHeader(columns, attrs)

        for r in rows:
            anchor = f"{currentTableAnchor()}.{r[7]}.{r[8]}"
            if int(r[1]):
                attrs[6] = f'style="background: {color_bad}"'
            else:
                attrs[6] = ""

            if int(r[0]):
                if float(r[5]) > 0.0:
                    attrs[4] = attrs[5] = f'style="background: {color_bad}"'
                else:
                    attrs[4] = attrs[5] = f'style="background: {color_good}"'
            else:
                attrs[4] = attrs[5] = ""

            if (float(r[2]) + float(r[3])) / 2 > allowed_single_run_time:
                attrs[2] = f'style="background: {color_bad}"'
                attrs[3] = f'style="background: {color_bad}"'
            else:
                attrs[2] = ""
                attrs[3] = ""

            text += tableRow(r, attrs, anchor)

        text += tableEnd()
        tables.append(text)

    add_all_queries()
    add_report_errors()
    for t in tables:
        print(t)

    print(
        f"""
    </div>
    <p class="links">
    <a href="report.html">Main report</a>
    <a href="compare.log">Log</a>
    <a href="output.7z">Test output</a>
    {os.getenv("CHPC_ADD_REPORT_LINKS") or ''}
    </p>
    </body>
    </html>
    """
    )
