import logging
import os
from ast import literal_eval
from dataclasses import dataclass
from pathlib import Path
from typing import Final, Iterable, List, Literal, Optional, Sequence, Tuple, Union

from build_download_helper import APIException, get_gh_api
from env_helper import (
    GITHUB_JOB,
    GITHUB_REPOSITORY,
    GITHUB_RUN_ID,
    GITHUB_RUN_URL,
    GITHUB_WORKSPACE,
)

logger = logging.getLogger(__name__)

ERROR: Final = "error"
FAILURE: Final = "failure"
PENDING: Final = "pending"
SUCCESS: Final = "success"

OK: Final = "OK"
FAIL: Final = "FAIL"
SKIPPED: Final = "SKIPPED"

StatusType = Literal["error", "failure", "pending", "success"]
STATUSES = [ERROR, FAILURE, PENDING, SUCCESS]  # type: List[StatusType]

# These parameters are set only on demand, and only once
_GITHUB_JOB_ID = ""
_GITHUB_JOB_URL = ""
_GITHUB_JOB_API_URL = ""


def GITHUB_JOB_ID(safe: bool = True) -> str:
    # pylint:disable=global-statement
    global _GITHUB_JOB_ID
    global _GITHUB_JOB_URL
    global _GITHUB_JOB_API_URL
    if _GITHUB_JOB_ID:
        return _GITHUB_JOB_ID
    try:
        _GITHUB_JOB_ID, _GITHUB_JOB_URL, _GITHUB_JOB_API_URL = get_job_id_url(
            GITHUB_JOB
        )
    except APIException as e:
        logging.warning("Unable to retrieve the job info from GH API: %s", e)
        if not safe:
            raise e
    return _GITHUB_JOB_ID


def GITHUB_JOB_URL(safe: bool = True) -> str:
    try:
        GITHUB_JOB_ID()
    except APIException:
        if safe:
            logging.warning("Using run URL as a fallback to not fail the job")
            return GITHUB_RUN_URL
        raise

    return _GITHUB_JOB_URL


def GITHUB_JOB_API_URL(safe: bool = True) -> str:
    GITHUB_JOB_ID(safe)
    return _GITHUB_JOB_API_URL


def get_job_id_url(job_name: str) -> Tuple[str, str, str]:
    job_id = ""
    job_url = ""
    job_api_url = ""
    if GITHUB_RUN_ID == "0":
        job_id = "0"
    if job_id:
        return job_id, job_url, job_api_url
    jobs = []
    page = 1
    while not job_id:
        response = get_gh_api(
            f"https://api.github.com/repos/{GITHUB_REPOSITORY}/"
            f"actions/runs/{GITHUB_RUN_ID}/jobs?per_page=100&page={page}"
        )
        page += 1
        data = response.json()
        jobs.extend(data["jobs"])
        for job in data["jobs"]:
            if job["name"] != job_name:
                continue
            job_id = job["id"]
            job_url = job["html_url"]
            job_api_url = job["url"]
            return job_id, job_url, job_api_url
        if (
            len(jobs) >= data["total_count"]  # just in case of inconsistency
            or len(data["jobs"]) == 0  # if we excided pages
        ):
            job_id = "0"

    if not job_url:
        # This is a terrible workaround for the case of another broken part of
        # GitHub actions. For nested workflows it doesn't provide a proper job_name
        # value, but only the final one. So, for `OriginalJob / NestedJob / FinalJob`
        # full name, job_name contains only FinalJob
        matched_jobs = []
        for job in jobs:
            nested_parts = job["name"].split(" / ")
            if len(nested_parts) <= 1:
                continue
            if nested_parts[-1] == job_name:
                matched_jobs.append(job)
        if len(matched_jobs) == 1:
            # The best case scenario
            job_id = matched_jobs[0]["id"]
            job_url = matched_jobs[0]["html_url"]
            job_api_url = matched_jobs[0]["url"]
            return job_id, job_url, job_api_url
        if matched_jobs:
            logging.error(
                "We could not get the ID and URL for the current job name %s, there "
                "are more than one jobs match it for the nested workflows. Please, "
                "refer to https://github.com/actions/runner/issues/2577",
                job_name,
            )

    return job_id, job_url, job_api_url


# The order of statuses from the worst to the best
def _state_rank(status: str) -> int:
    "return the index of status or index of SUCCESS in case of wrong status"
    try:
        return STATUSES.index(status)  # type: ignore
    except ValueError:
        return 3


def get_status(status: str) -> StatusType:
    "function to get the StatusType for a status or ERROR"
    try:
        ind = STATUSES.index(status)  # type: ignore
        return STATUSES[ind]
    except ValueError:
        return ERROR


def get_worst_status(statuses: Iterable[str]) -> StatusType:
    worst_status = SUCCESS  # type: StatusType
    for status in statuses:
        ind = _state_rank(status)
        if ind < _state_rank(worst_status):
            worst_status = STATUSES[ind]

        if worst_status == ERROR:
            break

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
th {{ background: var(--th-background); white-space: nowrap; }}
a {{ color: var(--link-color); text-decoration: none; }}
a:hover, a:active {{ color: var(--link-hover-color); text-decoration: none; }}
table {{ box-shadow: 0 8px 25px -5px rgba(0, 0, 0, var(--shadow-intensity)); border-collapse: collapse; border-spacing: 0; }}
p.links a {{ padding: 5px; margin: 3px; background: var(--menu-background); line-height: 2.5; white-space: nowrap; box-shadow: 0 8px 25px -5px rgba(0, 0, 0, var(--shadow-intensity)); }}
p.links a:hover {{ background: var(--menu-hover-background); color: var(--menu-hover-color); }}
th {{ cursor: pointer; }}
tr:hover {{ filter: var(--tr-hover-filter); }}
.expandable {{ cursor: pointer; }}
.expandable-content {{ display: none; }}
pre {{ white-space: pre-wrap; }}
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
# should not be in TEMP directory or any directory that may be cleaned during the job execution
JOB_REPORT_FILE = Path(GITHUB_WORKSPACE) / "job_report.json"

JOB_STARTED_TEST_NAME = "STARTED"
JOB_FINISHED_TEST_NAME = "COMPLETED"
JOB_TIMEOUT_TEST_NAME = "Job Timeout Expired"


@dataclass
class TestResult:
    name: str
    status: str
    # the following fields are optional
    time: Optional[float] = None
    log_files: Optional[Union[Sequence[str], Sequence[Path]]] = None
    raw_logs: Optional[str] = None
    # the field for uploaded logs URLs
    log_urls: Optional[Sequence[str]] = None

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
            assert Path(log_path).exists(), log_path
            self.log_files.append(log_path)

    @staticmethod
    def create_check_timeout_expired(duration: Optional[float] = None) -> "TestResult":
        return TestResult(JOB_TIMEOUT_TEST_NAME, "FAIL", time=duration)


TestResults = List[TestResult]


class ReportColorTheme:
    class ReportColor:
        yellow = "#FFB400"
        red = "#F00"
        green = "#0A0"
        blue = "#00B4FF"

    default = (ReportColor.green, ReportColor.red, ReportColor.yellow)


ColorTheme = Tuple[str, str, str]


def _format_header(
    header: str, branch_name: str, branch_url: Optional[str] = None
) -> str:
    result = header
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
    base_name = ""
    if isinstance(url, str):
        base_name = os.path.basename(url)
    if isinstance(url, tuple):
        base_name = url[1]

    if "?" in base_name:
        base_name = base_name.split("?")[0]

    if base_name is not None:
        return base_name.replace("%2B", "+").replace("%20", " ")
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
