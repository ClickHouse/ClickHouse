# -*- coding: utf-8 -*-
import os
import datetime

### FIXME: BEST FRONTEND PRACTICIES BELOW

HTML_BASE_TEST_TEMPLATE = """
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
th {{ cursor: pointer; }}
.failed {{ cursor: pointer; }}
.failed-content.open {{}}
.failed-content {{ display: none; }}

  </style>
  <title>{title}</title>
</head>
<body>
<div class="main">

<h1>{header}</h1>
<p class="links">
<a href="{raw_log_url}">{raw_log_name}</a>
<a href="{commit_url}">Commit</a>
{additional_urls}
<a href="{task_url}">Task (github actions)</a>
</p>
{test_part}
</body>
<script type="text/javascript">
    /// Straight from https://stackoverflow.com/questions/14267781/sorting-html-table-with-javascript

    const getCellValue = (tr, idx) => {{
        var classes = tr.classList;
        var elem = tr;
        if (classes.contains("failed-content") || classes.contains("failed-content.open"))
            elem = tr.previousElementSibling;
        return elem.children[idx].innerText || elem.children[idx].textContent;
    }}

    const comparer = (idx, asc) => (a, b) => ((v1, v2) =>
        v1 !== '' && v2 !== '' && !isNaN(v1) && !isNaN(v2) ? v1 - v2 : v1.toString().localeCompare(v2)
        )(getCellValue(asc ? a : b, idx), getCellValue(asc ? b : a, idx));

    // do the work...
    document.querySelectorAll('th').forEach(th => th.addEventListener('click', (() => {{
        const table = th.closest('table');
        Array.from(table.querySelectorAll('tr:nth-child(n+2)'))
            .sort(comparer(Array.from(th.parentNode.children).indexOf(th), this.asc = !this.asc))
            .forEach(tr => table.appendChild(tr) );
    }})));

    Array.from(document.getElementsByClassName("failed")).forEach(tr => tr.addEventListener('click', function() {{
        var content = this.nextElementSibling;
        content.classList.toggle("failed-content.open");
        content.classList.toggle("failed-content");
    }}));
</script>
</html>
"""

HTML_TEST_PART = """
<table>
<tr>
{headers}
</tr>
{rows}
</table>
"""

BASE_HEADERS = ["Test name", "Test status"]


class ReportColorTheme:
    class ReportColor:
        yellow = "#FFB400"
        red = "#F00"
        green = "#0A0"
        blue = "#00B4FF"

    default = (ReportColor.green, ReportColor.red, ReportColor.yellow)
    bugfixcheck = (ReportColor.yellow, ReportColor.blue, ReportColor.blue)


def _format_header(header, branch_name, branch_url=None):
    result = " ".join([w.capitalize() for w in header.split(" ")])
    result = result.replace("Clickhouse", "ClickHouse")
    result = result.replace("clickhouse", "ClickHouse")
    if "ClickHouse" not in result:
        result = "ClickHouse " + result
    result += " for "
    if branch_url:
        result += '<a href="{url}">{name}</a>'.format(url=branch_url, name=branch_name)
    else:
        result += branch_name
    return result


def _get_status_style(status, colortheme=None):
    ok_statuses = ("OK", "success", "PASSED")
    fail_statuses = ("FAIL", "failure", "error", "FAILED", "Timeout")

    if colortheme is None:
        colortheme = ReportColorTheme.default

    style = "font-weight: bold;"
    if status in ok_statuses:
        style += f"color: {colortheme[0]};"
    elif status in fail_statuses:
        style += f"color: {colortheme[1]};"
    else:
        style += f"color: {colortheme[2]};"
    return style


def _get_html_url_name(url):
    if isinstance(url, str):
        return os.path.basename(url).replace("%2B", "+").replace("%20", " ")
    if isinstance(url, tuple):
        return url[1].replace("%2B", "+").replace("%20", " ")
    return None


def _get_html_url(url):
    href = None
    name = None
    if isinstance(url, str):
        href, name = url, _get_html_url_name(url)
    if isinstance(url, tuple):
        href, name = url[0], _get_html_url_name(url)
    if href and name:
        return '<a href="{href}">{name}</a>'.format(
            href=href, name=_get_html_url_name(url)
        )
    return ""


def create_test_html_report(
    header,
    test_result,
    raw_log_url,
    task_url,
    branch_url,
    branch_name,
    commit_url,
    additional_urls=None,
    with_raw_logs=False,
    statuscolors=None,
):
    if additional_urls is None:
        additional_urls = []

    if test_result:
        rows_part = ""
        num_fails = 0
        has_test_time = False
        has_test_logs = False
        for result in test_result:
            test_name = result[0]
            test_status = result[1]

            test_logs = None
            test_time = None
            if len(result) > 2:
                test_time = result[2]
                has_test_time = True

            if len(result) > 3:
                test_logs = result[3]
                has_test_logs = True

            row = "<tr>"
            is_fail = test_status in ("FAIL", "FLAKY")
            if is_fail and with_raw_logs and test_logs is not None:
                row = '<tr class="failed">'
            row += "<td>" + test_name + "</td>"
            style = _get_status_style(test_status, colortheme=statuscolors)

            # Allow to quickly scroll to the first failure.
            is_fail_id = ""
            if is_fail:
                num_fails = num_fails + 1
                is_fail_id = 'id="fail' + str(num_fails) + '" '

            row += (
                "<td "
                + is_fail_id
                + 'style="{}">'.format(style)
                + test_status
                + "</td>"
            )

            if test_time is not None:
                row += "<td>" + test_time + "</td>"

            if test_logs is not None and not with_raw_logs:
                test_logs_html = "<br>".join([_get_html_url(url) for url in test_logs])
                row += "<td>" + test_logs_html + "</td>"

            row += "</tr>"
            rows_part += row
            if test_logs is not None and with_raw_logs:
                row = '<tr class="failed-content">'
                # TODO: compute colspan too
                row += '<td colspan="3"><pre>' + test_logs + "</pre></td>"
                row += "</tr>"
                rows_part += row

        headers = BASE_HEADERS
        if has_test_time:
            headers.append("Test time, sec.")
        if has_test_logs and not with_raw_logs:
            headers.append("Logs")

        headers = "".join(["<th>" + h + "</th>" for h in headers])
        test_part = HTML_TEST_PART.format(headers=headers, rows=rows_part)
    else:
        test_part = ""

    additional_html_urls = " ".join(
        [_get_html_url(url) for url in sorted(additional_urls, key=_get_html_url_name)]
    )

    result = HTML_BASE_TEST_TEMPLATE.format(
        title=_format_header(header, branch_name),
        header=_format_header(header, branch_name, branch_url),
        raw_log_name=os.path.basename(raw_log_url),
        raw_log_url=raw_log_url,
        task_url=task_url,
        test_part=test_part,
        branch_name=branch_name,
        commit_url=commit_url,
        additional_urls=additional_html_urls,
    )
    return result


HTML_BASE_BUILD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
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
border: 0; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1); }}
a {{ color: #06F; text-decoration: none; }}
a:hover, a:active {{ color: #F40; text-decoration: underline; }}
table {{ border: 0; }}
.main {{ margin: auto; }}
p.links a {{ padding: 5px; margin: 3px; background: #FFF; line-height: 2; white-space: nowrap; box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.05), 0 8px 25px -5px rgba(0, 0, 0, 0.1); }}
tr:hover td {{filter: brightness(95%);}}
  </style>
<title>{title}</title>
</head>
<body>
<div class="main">
<h1>{header}</h1>
<table>
<tr>
<th>Compiler</th>
<th>Build type</th>
<th>Sanitizer</th>
<th>Bundled</th>
<th>Splitted</th>
<th>Status</th>
<th>Build log</th>
<th>Build time</th>
<th class="artifacts">Artifacts</th>
</tr>
{rows}
</table>
<p class="links">
<a href="{commit_url}">Commit</a>
<a href="{task_url}">Task (github actions)</a>
</p>
</body>
</html>
"""

LINK_TEMPLATE = '<a href="{url}">{text}</a>'


def create_build_html_report(
    header,
    build_results,
    build_logs_urls,
    artifact_urls_list,
    task_url,
    branch_url,
    branch_name,
    commit_url,
):
    rows = ""
    for (build_result, build_log_url, artifact_urls) in zip(
        build_results, build_logs_urls, artifact_urls_list
    ):
        row = "<tr>"
        row += "<td>{}</td>".format(build_result.compiler)
        if build_result.build_type:
            row += "<td>{}</td>".format(build_result.build_type)
        else:
            row += "<td>{}</td>".format("relwithdebuginfo")
        if build_result.sanitizer:
            row += "<td>{}</td>".format(build_result.sanitizer)
        else:
            row += "<td>{}</td>".format("none")

        row += "<td>{}</td>".format(build_result.bundled)
        row += "<td>{}</td>".format(build_result.splitted)

        if build_result.status:
            style = _get_status_style(build_result.status)
            row += '<td style="{}">{}</td>'.format(style, build_result.status)
        else:
            style = _get_status_style("error")
            row += '<td style="{}">{}</td>'.format(style, "error")

        row += '<td><a href="{}">link</a></td>'.format(build_log_url)

        if build_result.elapsed_seconds:
            delta = datetime.timedelta(seconds=build_result.elapsed_seconds)
        else:
            delta = "unknown"

        row += "<td>{}</td>".format(str(delta))

        links = ""
        link_separator = "<br/>"
        if artifact_urls:
            for artifact_url in artifact_urls:
                links += LINK_TEMPLATE.format(
                    text=_get_html_url_name(artifact_url), url=artifact_url
                )
                links += link_separator
            if links:
                links = links[: -len(link_separator)]
            row += "<td>{}</td>".format(links)

        row += "</tr>"
        rows += row
    return HTML_BASE_BUILD_TEMPLATE.format(
        title=_format_header(header, branch_name),
        header=_format_header(header, branch_name, branch_url),
        rows=rows,
        task_url=task_url,
        branch_name=branch_name,
        commit_url=commit_url,
    )
