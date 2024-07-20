import os
import re
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List, Union, Optional, Tuple


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
    @staticmethod
    def print_in_group(group_name: str, lines: Union[Any, List[Any]]) -> None:
        lines = list(lines)
        print(f"::group::{group_name}")
        for line in lines:
            print(line)
        print("::endgroup::")


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
    def run(cls, command):
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
            res = result.stdout
        return res.strip()

    @classmethod
    def check(cls, command):
        result = subprocess.run(
            command + " 2>&1",
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
