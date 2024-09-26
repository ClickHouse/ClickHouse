import json
import logging
import os
import re
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import requests

logger = logging.getLogger(__name__)


class Envs:
    GITHUB_REPOSITORY = os.getenv("GITHUB_REPOSITORY", "ClickHouse/ClickHouse")
    WORKFLOW_RESULT_FILE = os.getenv(
        "WORKFLOW_RESULT_FILE", "/tmp/workflow_results.json"
    )


LABEL_CATEGORIES = {
    "pr-backward-incompatible": ["Backward Incompatible Change"],
    "pr-bugfix": [
        "Bug Fix",
        "Bug Fix (user-visible misbehavior in an official stable release)",
        "Bug Fix (user-visible misbehaviour in official stable or prestable release)",
        "Bug Fix (user-visible misbehavior in official stable or prestable release)",
    ],
    "pr-critical-bugfix": ["Critical Bug Fix (crash, LOGICAL_ERROR, data loss, RBAC)"],
    "pr-build": [
        "Build/Testing/Packaging Improvement",
        "Build Improvement",
        "Build/Testing Improvement",
        "Build",
        "Packaging Improvement",
    ],
    "pr-documentation": [
        "Documentation (changelog entry is not required)",
        "Documentation",
    ],
    "pr-feature": ["New Feature"],
    "pr-improvement": ["Improvement"],
    "pr-not-for-changelog": [
        "Not for changelog (changelog entry is not required)",
        "Not for changelog",
    ],
    "pr-performance": ["Performance Improvement"],
    "pr-ci": ["CI Fix or Improvement (changelog entry is not required)"],
}

CATEGORY_TO_LABEL = {
    c: lb for lb, categories in LABEL_CATEGORIES.items() for c in categories
}


class WithIter(type):
    def __iter__(cls):
        return (v for k, v in cls.__dict__.items() if not k.startswith("_"))


@contextmanager
def cd(path: Union[Path, str]) -> Iterator[None]:
    oldpwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)


def kill_ci_runner(message: str) -> None:
    """The function to kill the current process with all parents when it's possible.
    Works only when run with the set `CI` environment"""
    if not os.getenv("CI", ""):  # cycle import env_helper
        logger.info("Running outside the CI, won't kill the runner")
        return
    print(f"::error::{message}")

    def get_ppid_name(pid: int) -> Tuple[int, str]:
        # Avoid using psutil, it's not in stdlib
        stats = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
        return int(stats[3]), stats[1]

    pid = os.getpid()
    pids = {}  # type: Dict[str, str]
    while pid:
        ppid, name = get_ppid_name(pid)
        pids[str(pid)] = name
        pid = ppid
    logger.error(
        "Sleeping 5 seconds and killing all possible processes from following:\n %s",
        "\n ".join(f"{p}: {n}" for p, n in pids.items()),
    )
    time.sleep(5)
    # The current process will be killed too
    subprocess.run(f"kill -9 {' '.join(pids.keys())}", check=False, shell=True)


def is_hex(s):
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def normalize_string(string: str) -> str:
    res = string.lower()
    for r in ((" ", "_"), ("(", "_"), (")", "_"), (",", "_"), ("/", "_"), ("-", "_")):
        res = res.replace(*r)
    return res


class GHActions:
    class ActionsNames:
        RunConfig = "RunConfig"

    class ActionStatuses:
        ERROR = "error"
        FAILURE = "failure"
        PENDING = "pending"
        SUCCESS = "success"

    @classmethod
    def _get_workflow_results(cls):
        if not Path(Envs.WORKFLOW_RESULT_FILE).exists():
            print(
                f"ERROR: Failed to get workflow results from file [{Envs.WORKFLOW_RESULT_FILE}]"
            )
            return {}
        with open(Envs.WORKFLOW_RESULT_FILE, "r", encoding="utf-8") as json_file:
            try:
                res = json.load(json_file)
            except json.JSONDecodeError as e:
                print(f"ERROR: json decoder exception {e}")
                return {}
        return res

    @classmethod
    def print_workflow_results(cls):
        res = cls._get_workflow_results()
        results = [f"{job}: {data['result']}" for job, data in res.items()]
        cls.print_in_group("Workflow results", results)

    @classmethod
    def get_workflow_job_result(cls, wf_job_name: str) -> Optional[str]:
        res = cls._get_workflow_results()
        if wf_job_name in res:
            return res[wf_job_name]["result"]  # type: ignore
        else:
            return None

    @staticmethod
    def print_in_group(group_name: str, lines: Union[Any, List[Any]]) -> None:
        lines = list(lines)
        print(f"::group::{group_name}")
        for line in lines:
            print(line)
        print("::endgroup::")

    @staticmethod
    def get_commit_status_by_name(
        token: str, commit_sha: str, status_name: Union[str, Sequence]
    ) -> str:
        assert len(token) == 40
        assert len(commit_sha) == 40
        assert is_hex(commit_sha)
        assert not is_hex(token)
        url = f"https://api.github.com/repos/{Envs.GITHUB_REPOSITORY}/commits/{commit_sha}/statuses?per_page={200}"
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        response = requests.get(url, headers=headers, timeout=5)

        if isinstance(status_name, str):
            status_name = (status_name,)
        if response.status_code == 200:
            assert "next" not in response.links, "Response truncated"
            statuses = response.json()
            for status in statuses:
                if status["context"] in status_name:
                    return status["state"]  # type: ignore
        return ""

    @staticmethod
    def check_wf_completed(token: str, commit_sha: str) -> bool:
        headers = {
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
        }
        url = f"https://api.github.com/repos/{Envs.GITHUB_REPOSITORY}/commits/{commit_sha}/check-runs?per_page={100}"

        for i in range(3):
            try:
                response = requests.get(url, headers=headers, timeout=5)
                response.raise_for_status()
                # assert "next" not in response.links, "Response truncated"

                data = response.json()
                assert data["check_runs"], "?"

                for check in data["check_runs"]:
                    if check["status"] != "completed":
                        print(
                            f"   Check workflow status: Check not completed [{check['name']}]"
                        )
                        return False
                return True
            except Exception as e:
                print(f"ERROR: exception after attempt [{i}]: {e}")
                time.sleep(1)

        return False

    @staticmethod
    def get_pr_url_by_branch(repo, branch):
        get_url_cmd = (
            f"gh pr list --repo {repo} --head {branch} --json url --jq '.[0].url'"
        )
        url = Shell.run(get_url_cmd)
        if not url:
            print(f"ERROR: PR nor found, branch [{branch}]")
        return url


class Shell:
    @classmethod
    def run_strict(cls, command):
        res = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        return res.stdout.strip()

    @classmethod
    def run(cls, command, check=False, dry_run=False):
        if dry_run:
            print(f"Dry-ryn. Would run command [{command}]")
            return ""
        print(f"Run command [{command}]")
        res = ""
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            print(f"stdout: {result.stdout.strip()}")
            res = result.stdout
        else:
            print(
                f"ERROR: stdout: {result.stdout.strip()}, stderr: {result.stderr.strip()}"
            )
            if check:
                assert result.returncode == 0
        return res.strip()

    @classmethod
    def run_as_daemon(cls, command):
        print(f"Run daemon command [{command}]")
        subprocess.Popen(command.split(" "))  # pylint:disable=consider-using-with
        return 0, ""

    @classmethod
    def check(cls, command):
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        return result.returncode == 0


class Utils:
    @staticmethod
    def get_failed_tests_number(description: str) -> Optional[int]:
        description = description.lower()

        pattern = r"fail:\s*(\d+)\s*(?=,|$)"
        match = re.search(pattern, description)
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def is_killed_with_oom():
        if Shell.check(
            "sudo dmesg -T | grep -q -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE'"
        ):
            return True
        return False

    @staticmethod
    def clear_dmesg():
        Shell.run("sudo dmesg --clear ||:")

    @staticmethod
    def check_pr_description(pr_body: str, repo_name: str) -> Tuple[str, str]:
        """The function checks the body to being properly formatted according to
        .github/PULL_REQUEST_TEMPLATE.md, if the first returned string is not empty,
        then there is an error."""
        lines = list(map(lambda x: x.strip(), pr_body.split("\n") if pr_body else []))
        lines = [re.sub(r"\s+", " ", line) for line in lines]

        # Check if body contains "Reverts ClickHouse/ClickHouse#36337"
        if [
            True for line in lines if re.match(rf"\AReverts {repo_name}#[\d]+\Z", line)
        ]:
            return "", LABEL_CATEGORIES["pr-not-for-changelog"][0]

        category = ""
        entry = ""
        description_error = ""

        i = 0
        while i < len(lines):
            if re.match(r"(?i)^[#>*_ ]*change\s*log\s*category", lines[i]):
                i += 1
                if i >= len(lines):
                    break
                # Can have one empty line between header and the category
                # itself. Filter it out.
                if not lines[i]:
                    i += 1
                    if i >= len(lines):
                        break
                category = re.sub(r"^[-*\s]*", "", lines[i])
                i += 1

                # Should not have more than one category. Require empty line
                # after the first found category.
                if i >= len(lines):
                    break
                if lines[i]:
                    second_category = re.sub(r"^[-*\s]*", "", lines[i])
                    description_error = (
                        "More than one changelog category specified: "
                        f"'{category}', '{second_category}'"
                    )
                    return description_error, category

            elif re.match(
                r"(?i)^[#>*_ ]*(short\s*description|change\s*log\s*entry)", lines[i]
            ):
                i += 1
                # Can have one empty line between header and the entry itself.
                # Filter it out.
                if i < len(lines) and not lines[i]:
                    i += 1
                # All following lines until empty one are the changelog entry.
                entry_lines = []
                while i < len(lines) and lines[i]:
                    entry_lines.append(lines[i])
                    i += 1
                entry = " ".join(entry_lines)
                # Don't accept changelog entries like '...'.
                entry = re.sub(r"[#>*_.\- ]", "", entry)
                # Don't accept changelog entries like 'Close #12345'.
                entry = re.sub(r"^[\w\-\s]{0,10}#?\d{5,6}\.?$", "", entry)
            else:
                i += 1

        if not category:
            description_error = "Changelog category is empty"
        # Filter out the PR categories that are not for changelog.
        elif "(changelog entry is not required)" in category:
            pass  # to not check the rest of the conditions
        elif category not in CATEGORY_TO_LABEL:
            description_error, category = f"Category '{category}' is not valid", ""
        elif not entry:
            description_error = f"Changelog entry required for category '{category}'"

        return description_error, category
