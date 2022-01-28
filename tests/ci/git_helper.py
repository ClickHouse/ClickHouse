#!/usr/bin/env python
import os.path as p
import re
import subprocess
from typing import Optional

TAG_REGEXP = r"^v\d{2}[.][1-9]\d*[.][1-9]\d*[.][1-9]\d*-(testing|prestable|stable|lts)$"
SHA_REGEXP = r"^([0-9]|[a-f]){40}$"


# Py 3.8 removeprefix and removesuffix
def removeprefix(string: str, prefix: str):
    if string.startswith(prefix):
        return string[len(prefix) :]  # noqa: ignore E203, false positive
    return string


def removesuffix(string: str, suffix: str):
    if string.endswith(suffix):
        return string[: -len(suffix)]
    return string


class Runner:
    """lightweight check_output wrapper with stripping last NEW_LINE"""

    def __init__(self, cwd: str = p.dirname(p.realpath(__file__))):
        self.cwd = cwd

    def run(self, cmd: str, cwd: Optional[str] = None) -> str:
        if cwd is None:
            cwd = self.cwd
        return subprocess.check_output(
            cmd, shell=True, cwd=cwd, encoding="utf-8"
        ).strip()


class Git:
    """A small wrapper around subprocess to invoke git commands"""

    def __init__(self):
        runner = Runner()
        rel_root = runner.run("git rev-parse --show-cdup")
        self.root = p.realpath(p.join(runner.cwd, rel_root))
        self._tag_pattern = re.compile(TAG_REGEXP)
        runner.cwd = self.root
        self.run = runner.run
        self.new_branch = ""
        self.branch = ""
        self.sha = ""
        self.sha_short = ""
        self.description = ""
        self.commits_since_tag = 0
        self.update()

    def update(self):
        """Is used to refresh all attributes after updates, e.g. checkout or commit"""
        self.branch = self.run("git branch --show-current")
        self.sha = self.run("git rev-parse HEAD")
        self.sha_short = self.sha[:11]
        # The following command shows the most recent tag in a graph
        # Format should match TAG_REGEXP
        self.latest_tag = self.run("git describe --tags --abbrev=0")
        # Format should be: {latest_tag}-{commits_since_tag}-g{sha_short}
        self.description = self.run("git describe --tags --long")
        self.commits_since_tag = int(
            self.run(f"git rev-list {self.latest_tag}..HEAD --count")
        )

    def _check_tag(self, value: str):
        if value == "":
            return
        if not self._tag_pattern.match(value):
            raise Exception(f"last tag {value} doesn't match the pattern")

    @property
    def latest_tag(self) -> str:
        return self._latest_tag

    @latest_tag.setter
    def latest_tag(self, value: str):
        self._check_tag(value)
        self._latest_tag = value

    @property
    def new_tag(self) -> str:
        return self._new_tag

    @new_tag.setter
    def new_tag(self, value: str):
        self._check_tag(value)
        self._new_tag = value

    @property
    def tweak(self) -> int:
        if not self.latest_tag.endswith("-testing"):
            # When we are on the tag, we still need to have tweak=1 to not
            # break cmake with versions like 12.13.14.0
            return self.commits_since_tag or 1

        version = self.latest_tag.split("-", maxsplit=1)[0]
        return int(version.split(".")[-1]) + self.commits_since_tag
