#!/usr/bin/env python3
"""Debug job: push from the release runner's checked-out tree, at start and end.

Reproduces the CreateRelease push environment that the local
``ci/tests/test_gh_app_commit_check.py`` probe cannot. It runs on the release
runner with ``enable_gh_auth=True`` (so ``gh`` and git authenticate as the
``clickhouse-gh`` App, exactly like the release) and pushes from the
``actions/checkout`` working tree — the same git config, including the checkout
token's http ``extraheader`` — rather than from a pristine temp repo. If that
pre-existing PAT/GITHUB_TOKEN config is what breaks the App-authenticated push,
it shows up here.

Pushes twice — once at the very beginning and once at the end — so config drift
introduced between them (were it to happen) would be visible as a
beginning-passes / end-fails split. Each probe creates a throwaway branch at the
current ``HEAD`` and deletes it immediately, leaving the repository unchanged.
"""

import os
import re

from ci.praktika.git import Git
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

DEFAULT_PROBE_BRANCH = "robot-clickhouse/debug-push-probe"


def _config():
    """Resolve the probe target from workflow inputs, with sensible defaults."""
    repo = Info.get_workflow_input_value("repo") or os.environ.get(
        "GITHUB_REPOSITORY", "ClickHouse/ClickHouse"
    )
    branch = Info.get_workflow_input_value("probe-branch") or DEFAULT_PROBE_BRANCH
    return repo, branch


def _dump_git_auth_state():
    # Origin file + key name only, never values — the extraheader carries a
    # token, so logging values would leak it. This reveals whether an
    # extraheader is set (and where) and thus whether our per-command
    # `-c http.https://github.com/.extraheader=` override targets the right key.
    cfg = Shell.get_output(
        "git config --show-origin --name-only "
        "--get-regexp '^(http|url|credential)\\.' || true",
        verbose=False,
    )
    print(f"auth-related git config (origin + key, no values):\n{cfg or '(none)'}")
    url = Shell.get_output("git remote get-url origin", verbose=False) or ""
    print(f"remote.origin.url: {re.sub(r'://[^@/]*@', '://***@', url)}")


def probe_push():
    """Push HEAD to a throwaway branch as the App, then delete it; return bool."""
    repo, branch = _config()
    _dump_git_auth_state()
    sha = Git.get_commit_sha("HEAD")
    print(f"pushing HEAD [{sha}] to {repo}@{branch}")
    pushed = Git.push(repo, f"{sha}:refs/heads/{branch}", force=True, retries=3)
    deleted = Git.push(repo, f":refs/heads/{branch}", retries=3)
    print(f"pushed={pushed} deleted={deleted}")
    return pushed and deleted


def main():
    stopwatch = Utils.Stopwatch()
    results = [
        Result.from_commands_run(name="Push probe (beginning)", command=probe_push),
        Result.from_commands_run(name="Push probe (end)", command=probe_push),
    ]
    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
