#!/usr/bin/env python
import argparse
import atexit
import logging
import os
import os.path as p
import re
import subprocess
import tempfile
from typing import Any, List, Literal, Optional

logger = logging.getLogger(__name__)

# ^ and $ match subline in `multiple\nlines`
# \A and \Z match only start and end of the whole string
RELEASE_BRANCH_REGEXP = r"\A\d+[.]\d+\Z"
TAG_REGEXP = (
    r"\Av\d{2}"  # First two digits of major part
    r"([.][1-9]\d*){3}"  # minor.patch.tweak parts
    r"-(new|testing|prestable|stable|lts)\Z"  # suffix with a version type
)
SHA_REGEXP = re.compile(r"\A([0-9]|[a-f]){40}\Z")

CWD = p.dirname(p.realpath(__file__))
TWEAK = 1

with tempfile.NamedTemporaryFile("w", delete=False) as f:
    GIT_KNOWN_HOSTS_FILE = f.name
    GIT_PREFIX = (  # All commits to remote are done as robot-clickhouse
        "git -c user.email=robot-clickhouse@users.noreply.github.com "
        "-c user.name=robot-clickhouse -c commit.gpgsign=false "
        "-c core.sshCommand="
        f"'ssh -o UserKnownHostsFile={GIT_KNOWN_HOSTS_FILE} "
        "-o StrictHostKeyChecking=accept-new'"
    )
    atexit.register(os.remove, f.name)


# Py 3.8 removeprefix and removesuffix
def removeprefix(string: str, prefix: str) -> str:
    if string.startswith(prefix):
        return string[len(prefix) :]  # noqa: ignore E203, false positive
    return string


def removesuffix(string: str, suffix: str) -> str:
    if string.endswith(suffix):
        return string[: -len(suffix)]
    return string


def commit(name: str) -> str:
    if not SHA_REGEXP.match(name):
        raise argparse.ArgumentTypeError(
            "commit hash should contain exactly 40 hex characters"
        )
    return name


def release_branch(name: str) -> str:
    r = re.compile(RELEASE_BRANCH_REGEXP)
    if not r.match(name):
        raise argparse.ArgumentTypeError("release branch should be as 12.1")
    return name


class Runner:
    """lightweight check_output wrapper with stripping last NEW_LINE"""

    def __init__(self, cwd: str = CWD, set_cwd_to_git_root: bool = False):
        self._cwd = cwd
        # delayed set cwd to the repo's root, to not do it at the import stage
        self._git_root = None  # type: Optional[str]
        self._set_cwd_to_git_root = set_cwd_to_git_root

    def run(self, cmd: str, cwd: Optional[str] = None, **kwargs: Any) -> str:
        if cwd is None:
            cwd = self.cwd
        logger.debug("Running command: %s", cmd)
        output = str(
            subprocess.check_output(
                cmd, shell=True, cwd=cwd, encoding="utf-8", **kwargs
            ).strip()
        )
        return output

    @property
    def cwd(self) -> str:
        if self._set_cwd_to_git_root:
            if self._git_root is None:
                self._git_root = p.realpath(
                    p.join(self._cwd, self.run("git rev-parse --show-cdup", self._cwd))
                )
            return self._git_root
        return self._cwd

    @cwd.setter
    def cwd(self, value: str) -> None:
        # Set _cwd only once, then set it to readonly
        if self._cwd != CWD:
            return
        self._cwd = value

    def __call__(self, *args: Any, **kwargs: Any) -> str:
        return self.run(*args, **kwargs)


git_runner = Runner(set_cwd_to_git_root=True)


def is_shallow() -> bool:
    return git_runner.run("git rev-parse --is-shallow-repository") == "true"


def get_tags() -> List[str]:
    if is_shallow():
        raise RuntimeError("attempt to run on a shallow repository")
    return git_runner.run("git tag").split()


class Git:
    """A small wrapper around subprocess to invoke git commands"""

    _tag_pattern = re.compile(TAG_REGEXP)

    def __init__(self, ignore_no_tags: bool = False):
        """
        new_tag is used for special v24.1.1.1-new tags where the previous version is moved to the release branch
        * 66666666666 Some another commit with version 24.8.1.xxxxx-testing, tweak is counted from new_tag = v24.8.1.1-new
        | * 55555555555 (tag: v24.7.1.123123123-stable, branch: 24.7) tweak counted from new_tag = v24.7.1.1-new
        |/
        * 44444444444 (tag: v24.8.1.1-new)
        | * 33333333333 (tag: v24.6.1.123123123-stable, branch: 24.6) tweak counted from new_tag = v24.6.1.1-new
        |/
        * 22222222222 (tag: v24.7.1.1-new)
        | * 11111111111 (tag: v24.5.1.123123123-stable, branch: 24.5) tweak counted from new_tag = v24.4.1.2088-stable
        |/
        * 00000000000 (tag: v24.6.1.1-new)
        * 6d4b31322d1 (tag: v24.4.1.2088-stable)
        * 2c5c589a882 (tag: v24.3.1.2672-lts)
        * 891689a4150 (tag: v24.2.1.2248-stable)
        * 5a024dfc093 (tag: v24.1.1.2048-stable)
        * a2faa65b080 (tag: v23.12.1.1368-stable)
        * 05bc8ef1e02 (tag: v23.11.1.2711-stable)
        """
        self.root = git_runner.cwd
        self._ignore_no_tags = ignore_no_tags
        self.run = git_runner.run
        self.latest_tag = ""
        self.new_tag = ""
        self.branch = ""
        self.sha = ""
        self.sha_short = ""
        self.commits_since_latest = 0
        self.commits_since_new = 0
        self.update()

    def update(self):
        """Is used to refresh all attributes after updates, e.g. checkout or commit"""
        self.sha = self.run("git rev-parse HEAD")
        self.branch = self.run("git branch --show-current") or self.sha
        self.sha_short = self.sha[:11]
        # The following command shows the most recent tag in a graph
        # Format should match TAG_REGEXP
        if self._ignore_no_tags and is_shallow():
            try:
                self._update_tags(True)
            except subprocess.CalledProcessError:
                pass

            return
        self._update_tags()

    def _update_tags(self, suppress_stderr: bool = False) -> None:
        stderr = subprocess.DEVNULL if suppress_stderr else None
        self.latest_tag = self.run("git describe --tags --abbrev=0", stderr=stderr)
        # Format should be: {latest_tag}-{commits_since_tag}-g{sha_short}
        self.commits_since_latest = int(
            self.run(f"git rev-list {self.latest_tag}..HEAD --count")
        )
        if self.latest_tag.endswith("-new"):
            # We won't change the behaviour of the the "latest_tag"
            # So here we set "new_tag" to the previous tag in the graph, that will allow
            # getting alternative "tweak"
            self.new_tag = self.run(
                f"git describe --tags --abbrev=0 --exclude='{self.latest_tag}'",
                stderr=stderr,
            )
            self.commits_since_new = int(
                self.run(f"git rev-list {self.new_tag}..HEAD --count")
            )

    @staticmethod
    def check_tag(value: str) -> None:
        if value == "":
            return
        if not Git._tag_pattern.match(value):
            raise ValueError(f"last tag {value} doesn't match the pattern")

    @property
    def latest_tag(self) -> str:
        return self._latest_tag

    @latest_tag.setter
    def latest_tag(self, value: str) -> None:
        self.check_tag(value)
        self._latest_tag = value

    @property
    def new_tag(self) -> str:
        return self._new_tag

    @new_tag.setter
    def new_tag(self, value: str) -> None:
        self.check_tag(value)
        self._new_tag = value

    @property
    def tweak(self) -> int:
        return self._tweak("latest")

    @property
    def tweak_to_new(self) -> int:
        return self._tweak("new")

    def _tweak(self, tag_type: Literal["latest", "new"]) -> int:
        """Accepts latest or new as a tag_type and returns the tweak number to it"""
        if tag_type == "latest":
            commits = self.commits_since_latest
            tag = self.latest_tag
        else:
            commits = self.commits_since_new
            tag = self.new_tag

        if not tag.endswith("-testing"):
            # When we are on the tag, we still need to have tweak=1 to not
            # break cmake with versions like 12.13.14.0
            if commits:
                return commits
            # We are in a tagged commit or shallow checkout. The tweak should match the
            # current version's value
            version = tag.split("-", maxsplit=1)[0]
            try:
                return int(version.split(".")[-1])
            except ValueError:
                # There are no tags (shallow checkout), or a wrong tag. Return default
                return TWEAK

        version = tag.split("-", maxsplit=1)[0]
        return int(version.split(".")[-1]) + commits
