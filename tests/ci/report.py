# -*- coding: utf-8 -*-
from ast import literal_eval
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Final, Iterable, List, Literal, Optional, Tuple
from html import escape
import csv
import datetime
import json
import logging
import os

from build_download_helper import get_gh_api
from ci_config import BuildConfig, CI_CONFIG


logger = logging.getLogger(__name__)

ERROR: Final = "error"
FAILURE: Final = "failure"
PENDING: Final = "pending"
SUCCESS: Final = "success"

OK: Final = "OK"
FAIL: Final = "FAIL"

StatusType = Literal["error", "failure", "pending", "success"]
# The order of statuses from the worst to the best
_STATES = {ERROR: 0, FAILURE: 1, PENDING: 2, SUCCESS: 3}


def get_worst_status(statuses: Iterable[str]) -> str:
    worst_status = None
    for status in statuses:
        if _STATES.get(status) is None:
            continue
        if worst_status is None:
            worst_status = status
            continue
        if _STATES.get(status) < _STATES.get(worst_status):
            worst_status = status

        if worst_status == ERROR:
            break

    if worst_status is None:
        return ""
    return worst_status


### BEST FRONTEND PRACTICES BELOW

HEAD_HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
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
.expandable {{ cursor: pointer; }}
.expandable-content {{ display: none; }}
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
"""

FOOTER_HTML_TEMPLATE = """<img id="fish" src="https://presentations.clickhouse.com/images/fish.png" />
<script type="text/javascript">
    /// Straight from https://stackoverflow.com/questions/14267781/sorting-html-table-with-javascript

    const getCellValue = (tr, idx) => {{
        var classes = tr.classList;
        var elem = tr;
        if (classes.contains("expandable-content") || classes.contains("expandable-content.open"))
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

    Array.from(document.getElementsByClassName("expandable")).forEach(tr => tr.addEventListener('click', function() {{
        var content = this.nextElementSibling;
        content.classList.toggle("expandable-content");
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


HTML_BASE_TEST_TEMPLATE = (
    f"{HEAD_HTML_TEMPLATE}"
    """<p class="links">
<a href="{raw_log_url}">{raw_log_name}</a>
<a href="{commit_url}">Commit</a>
{additional_urls}
<a href="{task_url}">Task (github actions)</a>
<a href="{job_url}">Job (github actions)</a>
</p>
{test_part}
"""
    f"{FOOTER_HTML_TEMPLATE}"
)

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
    build_name: str
    log_url: str
    build_urls: List[str]
    version: str
    status: StatusType
    elapsed_seconds: int
    job_api_url: str
    _job_name: Optional[str] = None
    _job_html_url: Optional[str] = None
    _job_html_link: Optional[str] = None
    _grouped_urls: Optional[List[List[str]]] = None

    @property
    def build_config(self) -> Optional[BuildConfig]:
        return CI_CONFIG.build_config.get(self.build_name, None)

    @property
    def comment(self) -> str:
        if self.build_config is None:
            return self._wrong_config_message
        return self.build_config.comment

    @property
    def compiler(self) -> str:
        if self.build_config is None:
            return self._wrong_config_message
        return self.build_config.compiler

    @property
    def debug_build(self) -> bool:
        if self.build_config is None:
            return False
        return self.build_config.debug_build

    @property
    def sanitizer(self) -> str:
        if self.build_config is None:
            return self._wrong_config_message
        return self.build_config.sanitizer

    @property
    def grouped_urls(self) -> List[List[str]]:
        "Combine and preserve build_urls by artifact types"
        if self._grouped_urls is not None:
            return self._grouped_urls
        if not self.build_urls:
            self._grouped_urls = [[]]
            return self._grouped_urls
        artifacts_groups = {
            "apk": [],
            "deb": [],
            "binary": [],
            "tgz": [],
            "rpm": [],
            "performance": [],
        }  # type: Dict[str, List[str]]
        for url in self.build_urls:
            if url.endswith("performance.tar.zst"):
                artifacts_groups["performance"].append(url)
            elif (
                url.endswith(".deb")
                or url.endswith(".buildinfo")
                or url.endswith(".changes")
                or url.endswith(".tar.gz")
            ):
                artifacts_groups["deb"].append(url)
            elif url.endswith(".apk"):
                artifacts_groups["apk"].append(url)
            elif url.endswith(".rpm"):
                artifacts_groups["rpm"].append(url)
            elif url.endswith(".tgz") or url.endswith(".tgz.sha512"):
                artifacts_groups["tgz"].append(url)
            else:
                artifacts_groups["binary"].append(url)
        self._grouped_urls = [urls for urls in artifacts_groups.values() if urls]
        return self._grouped_urls

    @property
    def _wrong_config_message(self) -> str:
        return "missing"

    @property
    def file_name(self) -> Path:
        return self.get_report_name(self.build_name)

    @property
    def is_missing(self) -> bool:
        "The report is created for missing json file"
        return not (
            self.log_url
            or self.build_urls
            or self.version != "missing"
            or self.status != ERROR
        )

    @property
    def job_link(self) -> str:
        if self._job_html_link is not None:
            return self._job_html_link
        self._job_html_link = f'<a href="{self.job_html_url}">{self.job_name}</a>'
        return self._job_html_link

    @property
    def job_html_url(self) -> str:
        if self._job_html_url is not None:
            return self._job_html_url
        self._set_properties()
        return self._job_html_url or ""

    @property
    def job_name(self) -> str:
        if self._job_name is not None:
            return self._job_name
        self._set_properties()
        return self._job_name or ""

    @job_name.setter
    def job_name(self, job_name: str) -> None:
        self._job_name = job_name

    def _set_properties(self) -> None:
        if all(p is not None for p in (self._job_name, self._job_html_url)):
            return
        try:
            job_data = get_gh_api(self.job_api_url).json()
        except Exception:
            job_data = {}
        # job_name can be set manually
        self._job_name = self._job_name or job_data.get("name", "unknown")
        self._job_html_url = job_data.get("html_url", "")

    @staticmethod
    def get_report_name(name: str) -> Path:
        return Path(f"build_report_{name}.json")

    @staticmethod
    def read_json(directory: Path, build_name: str) -> "BuildResult":
        path = directory / BuildResult.get_report_name(build_name)
        try:
            with open(path, "r", encoding="utf-8") as pf:
                data = json.load(pf)  # type: dict
        except FileNotFoundError:
            logger.warning(
                "File %s for build named '%s' is not found", path, build_name
            )
            return BuildResult.missing_result(build_name)

        return BuildResult(
            data.get("build_name", build_name),
            data.get("log_url", ""),
            data.get("build_urls", []),
            data.get("version", ""),
            data.get("status", ERROR),
            data.get("elapsed_seconds", 0),
            data.get("job_api_url", ""),
        )

    @staticmethod
    def missing_result(build_name: str) -> "BuildResult":
        return BuildResult(build_name, "", [], "missing", ERROR, 0, "missing")

    def write_json(self, directory: Path) -> Path:
        path = directory / self.file_name
        path.write_text(
            json.dumps(
                {
                    "build_name": self.build_name,
                    "log_url": self.log_url,
                    "build_urls": self.build_urls,
                    "version": self.version,
                    "status": self.status,
                    "elapsed_seconds": self.elapsed_seconds,
                    "job_api_url": self.job_api_url,
                }
            ),
            encoding="utf-8",
        )
        # TODO: remove after the artifacts are in S3 completely
        env_path = Path(os.getenv("GITHUB_ENV", "/dev/null"))
        with env_path.open("a", encoding="utf-8") as ef:
            ef.write(f"BUILD_URLS={path.stem}")

        return path


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
    ok_statuses = (OK, SUCCESS, "PASSED")
    fail_statuses = (FAIL, FAILURE, ERROR, "FAILED", "Timeout", "NOT_FAILED")

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
        rows_part = []
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

            row = []
            if test_result.raw_logs is not None:
                row.append('<tr class="expandable">')
            else:
                row.append("<tr>")
            row.append(f"<td>{test_result.name}</td>")
            colspan += 1
            style = _get_status_style(test_result.status, colortheme=statuscolors)

            # Allow to quickly scroll to the first failure.
            fail_id = ""
            has_error = test_result.status in ("FAIL", "NOT_FAILED")
            if has_error:
                num_fails = num_fails + 1
                fail_id = f'id="fail{num_fails}" '

            row.append(f'<td {fail_id}style="{style}">{test_result.status}</td>')
            colspan += 1

            if test_result.time is not None:
                has_test_time = True
                row.append(f"<td>{test_result.time}</td>")
                colspan += 1

            if test_result.log_urls is not None:
                has_log_urls = True
                test_logs_html = "<br>".join(
                    [_get_html_url(url) for url in test_result.log_urls]
                )
                row.append(f"<td>{test_logs_html}</td>")
                colspan += 1

            row.append("</tr>")
            rows_part.append("\n".join(row))
            if test_result.raw_logs is not None:
                raw_logs = escape(test_result.raw_logs)
                row_raw_logs = (
                    '<tr class="expandable-content">'
                    f'<td colspan="{colspan}"><pre>{raw_logs}</pre></td>'
                    "</tr>"
                )
                rows_part.append(row_raw_logs)

        headers = BASE_HEADERS.copy()
        if has_test_time:
            headers.append("Test time, sec.")
        if has_log_urls:
            headers.append("Logs")

        headers_html = "".join(["<th>" + h + "</th>" for h in headers])
        test_part = HTML_TEST_PART.format(headers=headers_html, rows="".join(rows_part))
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


HTML_BASE_BUILD_TEMPLATE = (
    f"{HEAD_HTML_TEMPLATE}"
    """<p class="links">
<a href="{commit_url}">Commit</a>
<a href="{task_url}">Task (github actions)</a>
</p>
<table>
<tr>
<th>Config/job name</th>
<th>Compiler</th>
<th>Build type</th>
<th>Version</th>
<th>Sanitizer</th>
<th>Status</th>
<th>Build log</th>
<th>Build time</th>
<th class="artifacts">Artifacts</th>
<th>Comment</th>
</tr>
{rows}
</table>
"""
    f"{FOOTER_HTML_TEMPLATE}"
)

LINK_TEMPLATE = '<a href="{url}">{text}</a>'


def create_build_html_report(
    header: str,
    build_results: BuildResults,
    task_url: str,
    branch_url: str,
    branch_name: str,
    commit_url: str,
) -> str:
    rows = []
    for build_result in build_results:
        for artifact_urls in build_result.grouped_urls:
            row = ["<tr>"]
            row.append(
                f"<td>{build_result.build_name}<br/>{build_result.job_link}</td>"
            )
            row.append(f"<td>{build_result.compiler}</td>")
            if build_result.debug_build:
                row.append("<td>debug</td>")
            else:
                row.append("<td>relwithdebuginfo</td>")
            row.append(f"<td>{build_result.version}</td>")
            if build_result.sanitizer:
                row.append(f"<td>{build_result.sanitizer}</td>")
            else:
                row.append("<td>none</td>")

            if build_result.status:
                style = _get_status_style(build_result.status)
                row.append(f'<td style="{style}">{build_result.status}</td>')
            else:
                style = _get_status_style(ERROR)
                row.append(f'<td style="{style}">error</td>')

            row.append(f'<td><a href="{build_result.log_url}">link</a></td>')

            delta = "unknown"
            if build_result.elapsed_seconds:
                delta = str(datetime.timedelta(seconds=build_result.elapsed_seconds))

            row.append(f"<td>{delta}</td>")

            links = []
            link_separator = "<br/>"
            if artifact_urls:
                for artifact_url in artifact_urls:
                    links.append(
                        LINK_TEMPLATE.format(
                            text=_get_html_url_name(artifact_url), url=artifact_url
                        )
                    )
            row.append(f"<td>{link_separator.join(links)}</td>")

            comment = build_result.comment
            if (
                build_result.build_config is not None
                and build_result.build_config.sparse_checkout
            ):
                comment += " (note: sparse checkout is used)"
            row.append(f"<td>{comment}</td>")

            row.append("</tr>")
            rows.append("".join(row))
    return HTML_BASE_BUILD_TEMPLATE.format(
        title=_format_header(header, branch_name),
        header=_format_header(header, branch_name, branch_url),
        rows="".join(rows),
        task_url=task_url,
        branch_name=branch_name,
        commit_url=commit_url,
    )
