# -*- coding: utf-8 -*-
from ast import literal_eval
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Tuple
from html import escape
import csv
import os
import datetime

### BEST FRONTEND PRACTICES BELOW

HTML_BASE_TEST_TEMPLATE = """
<!DOCTYPE html>
<html>
  <style>

:root {{
    --color: white;
    --background: hsl(190deg, 90%, 5%) linear-gradient(180deg, hsl(190deg, 90%, 10%), hsl(190deg, 90%, 0%));
    --td-background: hsl(190deg, 90%, 15%);
    --th-background: hsl(180deg, 90%, 15%);
    --link-color: #FF5;
    --link-hover-color: #F40;
    --menu-background: hsl(190deg, 90%, 20%);
    --menu-hover-background: hsl(190deg, 100%, 50%);
    --menu-hover-color: black;
    --text-gradient: linear-gradient(90deg, #8F8, #F88);
    --shadow-intensity: 1;
    --tr-hover-filter: brightness(120%);
    --table-border-color: black;
}}

[data-theme="light"] {{
    --color: black;
    --background: hsl(190deg, 90%, 90%) linear-gradient(180deg, #EEE, #DEE);
    --td-background: white;
    --th-background: #EEE;
    --link-color: #08F;
    --link-hover-color: #F40;
    --menu-background: white;
    --menu-hover-background: white;
    --menu-hover-color: #F40;
    --text-gradient: linear-gradient(90deg, black, black);
    --shadow-intensity: 0.1;
    --tr-hover-filter: brightness(95%);
    --table-border-color: #DDD;
}}

.gradient {{
    background-image: var(--text-gradient);
    background-size: 100%;
    background-repeat: repeat;
    background-clip: text;
    -webkit-text-fill-color: transparent;
    -moz-text-fill-color: transparent;
    -webkit-background-clip: text;
    -moz-background-clip: text;
}}
html {{ min-height: 100%; font-family: "DejaVu Sans", "Noto Sans", Arial, sans-serif; background: var(--background); color: var(--color); }}
h1 {{ margin-left: 10px; }}
th, td {{ padding: 5px 10px 5px 10px; text-align: left; vertical-align: top; line-height: 1.5; border: 1px solid var(--table-border-color); }}
td {{ background: var(--td-background); }}
th {{ background: var(--th-background); }}
a {{ color: var(--link-color); text-decoration: none; }}
a:hover, a:active {{ color: var(--link-hover-color); text-decoration: none; }}
table {{ box-shadow: 0 8px 25px -5px rgba(0, 0, 0, var(--shadow-intensity)); border-collapse: collapse; border-spacing: 0; }}
p.links a {{ padding: 5px; margin: 3px; background: var(--menu-background); line-height: 2.5; white-space: nowrap; box-shadow: 0 8px 25px -5px rgba(0, 0, 0, var(--shadow-intensity)); }}
p.links a:hover {{ background: var(--menu-hover-background); color: var(--menu-hover-color); }}
th {{ cursor: pointer; }}
tr:hover {{ filter: var(--tr-hover-filter); }}
.failed {{ cursor: pointer; }}
.failed-content {{ display: none; }}
#fish {{ display: none; float: right; position: relative; top: -20em; right: 2vw; margin-bottom: -20em; width: 30vw; filter: brightness(7%); z-index: -1; }}

.themes {{
    float: right;
    font-size: 20pt;
    margin-bottom: 1rem;
}}

#toggle-dark, #toggle-light {{
    padding-right: 0.5rem;
    user-select: none;
    cursor: pointer;
}}

#toggle-dark:hover, #toggle-light:hover {{
    display: inline-block;
    transform: translate(1px, 1px);
    filter: brightness(125%);
}}

  </style>
  <title>{title}</title>
</head>
<body>
<div class="main">
<span class="nowrap themes"><span id="toggle-dark">🌚</span><span id="toggle-light">🌞</span></span>
<h1><span class="gradient">{header}</span></h1>
<p class="links">
<a href="{raw_log_url}">{raw_log_name}</a>
<a href="{commit_url}">Commit</a>
{additional_urls}
<a href="{task_url}">Task (github actions)</a>
<a href="{job_url}">Job (github actions)</a>
</p>
{test_part}
<img id="fish" src="https://presentations.clickhouse.com/images/fish.png" />
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

    document.querySelectorAll('th').forEach(th => th.addEventListener('click', (() => {{
        const table = th.closest('table');
        Array.from(table.querySelectorAll('tr:nth-child(n+2)'))
            .sort(comparer(Array.from(th.parentNode.children).indexOf(th), this.asc = !this.asc))
            .forEach(tr => table.appendChild(tr) );
    }})));

    Array.from(document.getElementsByClassName("failed")).forEach(tr => tr.addEventListener('click', function() {{
        var content = this.nextElementSibling;
        content.classList.toggle("failed-content");
    }}));

    let theme = 'dark';

    function setTheme(new_theme) {{
        theme = new_theme;
        document.documentElement.setAttribute('data-theme', theme);
        window.localStorage.setItem('theme', theme);
        drawFish();
    }}

    function drawFish() {{
        document.getElementById('fish').style.display = (document.body.clientHeight > 3000 && theme == 'dark') ? 'block' : 'none';
    }}

    document.getElementById('toggle-light').addEventListener('click', e => setTheme('light'));
    document.getElementById('toggle-dark').addEventListener('click', e => setTheme('dark'));

    let new_theme = window.localStorage.getItem('theme');
    if (new_theme && new_theme != theme) {{
        setTheme(new_theme);
    }}

    drawFish();
</script>
</body>
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


@dataclass
class TestResult:
    name: str
    status: str
    # the following fields are optional
    time: Optional[float] = None
    log_files: Optional[List[Path]] = None
    raw_logs: Optional[str] = None
    # the field for uploaded logs URLs
    log_urls: Optional[List[str]] = None

    def set_raw_logs(self, raw_logs: str) -> None:
        self.raw_logs = raw_logs

    def set_log_files(self, log_files_literal: str) -> None:
        self.log_files = []  # type: Optional[List[Path]]
        log_paths = literal_eval(log_files_literal)
        if not isinstance(log_paths, list):
            raise ValueError(
                f"Malformed input: must be a list literal: {log_files_literal}"
            )
        for log_path in log_paths:
            file = Path(log_path)
            assert file.exists(), file
            self.log_files.append(file)


TestResults = List[TestResult]


def read_test_results(results_path: Path, with_raw_logs: bool = True) -> TestResults:
    results = []  # type: TestResults
    with open(results_path, "r", encoding="utf-8") as descriptor:
        reader = csv.reader(descriptor, delimiter="\t")
        for line in reader:
            name = line[0]
            status = line[1]
            time = None
            if len(line) >= 3 and line[2] and line[2] != "\\N":
                # The value can be emtpy, but when it's not,
                # it's the time spent on the test
                try:
                    time = float(line[2])
                except ValueError:
                    pass

            result = TestResult(name, status, time)
            if len(line) == 4 and line[3]:
                # The value can be emtpy, but when it's not,
                # the 4th value is a pythonic list, e.g. ['file1', 'file2']
                if with_raw_logs:
                    # Python does not support TSV, so we unescape manually
                    result.set_raw_logs(
                        line[3].replace("\\t", "\t").replace("\\n", "\n")
                    )
                else:
                    result.set_log_files(line[3])

            results.append(result)

    return results


@dataclass
class BuildResult:
    compiler: str
    build_type: str
    sanitizer: str
    status: str
    elapsed_seconds: int
    comment: str


BuildResults = List[BuildResult]


class ReportColorTheme:
    class ReportColor:
        yellow = "#FFB400"
        red = "#F00"
        green = "#0A0"
        blue = "#00B4FF"

    default = (ReportColor.green, ReportColor.red, ReportColor.yellow)
    bugfixcheck = (ReportColor.yellow, ReportColor.blue, ReportColor.blue)


ColorTheme = Tuple[str, str, str]


def _format_header(
    header: str, branch_name: str, branch_url: Optional[str] = None
) -> str:
    # Following line does not lower CI->Ci and SQLancer->Sqlancer. It only
    # capitalizes the first letter and doesn't touch the rest of the word
    result = " ".join([w[0].upper() + w[1:] for w in header.split(" ") if w])
    result = result.replace("Clickhouse", "ClickHouse")
    result = result.replace("clickhouse", "ClickHouse")
    if "ClickHouse" not in result:
        result = f"ClickHouse {result}"
    if branch_url:
        result = f'{result} for <a href="{branch_url}">{branch_name}</a>'
    else:
        result = f"{result} for {branch_name}"
    return result


def _get_status_style(status: str, colortheme: Optional[ColorTheme] = None) -> str:
    ok_statuses = ("OK", "success", "PASSED")
    fail_statuses = ("FAIL", "failure", "error", "FAILED", "Timeout", "NOT_FAILED")

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
        return f'<a href="{href}">{_get_html_url_name(url)}</a>'
    return ""


def create_test_html_report(
    header: str,
    test_results: TestResults,
    raw_log_url: str,
    task_url: str,
    job_url: str,
    branch_url: str,
    branch_name: str,
    commit_url: str,
    additional_urls: Optional[List[str]] = None,
    statuscolors: Optional[ColorTheme] = None,
) -> str:
    if additional_urls is None:
        additional_urls = []

    if test_results:
        rows_part = ""
        num_fails = 0
        has_test_time = False
        has_log_urls = False

        # Display entires with logs at the top (they correspond to failed tests)
        test_results.sort(
            key=lambda result: result.raw_logs is None and result.log_files is None
        )

        for test_result in test_results:
            colspan = 0
            if test_result.log_files is not None:
                has_log_urls = True

            row = "<tr>"
            has_error = test_result.status in ("FAIL", "NOT_FAILED")
            if has_error and test_result.raw_logs is not None:
                row = '<tr class="failed">'
            row += "<td>" + test_result.name + "</td>"
            colspan += 1
            style = _get_status_style(test_result.status, colortheme=statuscolors)

            # Allow to quickly scroll to the first failure.
            fail_id = ""
            if has_error:
                num_fails = num_fails + 1
                fail_id = f'id="fail{num_fails}" '

            row += f'<td {fail_id}style="{style}">{test_result.status}</td>'
            colspan += 1

            if test_result.time is not None:
                has_test_time = True
                row += f"<td>{test_result.time}</td>"
                colspan += 1

            if test_result.log_urls is not None:
                has_log_urls = True
                test_logs_html = "<br>".join(
                    [_get_html_url(url) for url in test_result.log_urls]
                )
                row += "<td>" + test_logs_html + "</td>"
                colspan += 1

            row += "</tr>"
            rows_part += row
            if test_result.raw_logs is not None:
                raw_logs = escape(test_result.raw_logs)
                row = (
                    '<tr class="failed-content">'
                    f'<td colspan="{colspan}"><pre>{raw_logs}</pre></td>'
                    "</tr>"
                )
                rows_part += row

        headers = BASE_HEADERS.copy()
        if has_test_time:
            headers.append("Test time, sec.")
        if has_log_urls:
            headers.append("Logs")

        headers_html = "".join(["<th>" + h + "</th>" for h in headers])
        test_part = HTML_TEST_PART.format(headers=headers_html, rows=rows_part)
    else:
        test_part = ""

    additional_html_urls = " ".join(
        [_get_html_url(url) for url in sorted(additional_urls, key=_get_html_url_name)]
    )

    raw_log_name = os.path.basename(raw_log_url)
    if "?" in raw_log_name:
        raw_log_name = raw_log_name.split("?")[0]

    html = HTML_BASE_TEST_TEMPLATE.format(
        title=_format_header(header, branch_name),
        header=_format_header(header, branch_name, branch_url),
        raw_log_name=raw_log_name,
        raw_log_url=raw_log_url,
        task_url=task_url,
        job_url=job_url,
        test_part=test_part,
        branch_name=branch_name,
        commit_url=commit_url,
        additional_urls=additional_html_urls,
    )
    return html


HTML_BASE_BUILD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
  <style>
body {{ font-family: "DejaVu Sans", "Noto Sans", Arial, sans-serif; background: #EEE; }}
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
<th>Status</th>
<th>Build log</th>
<th>Build time</th>
<th class="artifacts">Artifacts</th>
<th>Comment</th>
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
    header: str,
    build_results: BuildResults,
    build_logs_urls: List[str],
    artifact_urls_list: List[List[str]],
    task_url: str,
    branch_url: str,
    branch_name: str,
    commit_url: str,
) -> str:
    rows = ""
    for build_result, build_log_url, artifact_urls in zip(
        build_results, build_logs_urls, artifact_urls_list
    ):
        row = "<tr>"
        row += f"<td>{build_result.compiler}</td>"
        if build_result.build_type:
            row += f"<td>{build_result.build_type}</td>"
        else:
            row += "<td>relwithdebuginfo</td>"
        if build_result.sanitizer:
            row += f"<td>{build_result.sanitizer}</td>"
        else:
            row += "<td>none</td>"

        if build_result.status:
            style = _get_status_style(build_result.status)
            row += f'<td style="{style}">{build_result.status}</td>'
        else:
            style = _get_status_style("error")
            row += f'<td style="{style}">error</td>'

        row += f'<td><a href="{build_log_url}">link</a></td>'

        if build_result.elapsed_seconds:
            delta = datetime.timedelta(seconds=build_result.elapsed_seconds)
        else:
            delta = "unknown"  # type: ignore

        row += f"<td>{delta}</td>"

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
            row += f"<td>{links}</td>"

        row += f"<td>{build_result.comment}</td>"

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
