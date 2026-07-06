#!/usr/bin/env python3
"""Debug job: does the release's *tag* push hit GitHub's workflow scan?

Runs on the release runner with ``enable_gh_auth=True`` (so ``gh`` and git
authenticate as the ``clickhouse-gh`` App, exactly like the release) and creates
and pushes throwaway *tags* with the real ``Git.push_tag`` helper — the same
operation as ``push_release_tag`` — each deleted immediately.

Earlier probes pushed a *branch* at the ``cr-work`` tip (which modifies
``.github/workflows``) and were rejected with the ``workflows``-scope timeout;
but the release pushes a *tag* at an existing release commit, not a branch. So
here we tag two existing commits and push the tags:

  * ``master`` — tag at ``origin/master``'s tip (workflows == master).
  * ``cr-work`` — tag at the checked-out ``cr-work`` tip (which does modify
    ``.github/workflows`` vs master).

A tag only points at an already-present commit, so if both pass — even the
cr-work one — tag pushes bypass the workflow scan entirely and the release tag
push is unaffected. If the cr-work tag fails while the master tag passes, the
tagged commit's workflow diff matters even for tags.
"""

import re

from ci.praktika.git import Git
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

REPO = "ClickHouse/ClickHouse"
TAG_PREFIX = "debug-push-probe-tag"
ROBOT_NAME = "robot-clickhouse"
ROBOT_EMAIL = "robot-clickhouse@users.noreply.github.com"


def _dump_git_auth_state():
    # Origin file + key name only, never values — the extraheader carries a token.
    cfg = Shell.get_output(
        "git config --show-origin --name-only "
        "--get-regexp '^(http|url|credential)\\.' || true",
        verbose=False,
    )
    print(f"auth-related git config (origin + key, no values):\n{cfg or '(none)'}")
    url = Shell.get_output("git remote get-url origin", verbose=False) or ""
    print(f"remote.origin.url: {re.sub(r'://[^@/]*@', '://***@', url)}")


def _tag_probe(label, commit_ref):
    """Create+push an annotated tag at commit_ref (like push_release_tag); bool."""
    tag = f"{TAG_PREFIX}-{label}"
    sha = Git.get_commit_sha(commit_ref)
    print(f"=== tag probe [{label}] at {commit_ref} [{sha}] -> {REPO} ===")
    try:
        Git.push_tag(
            REPO,
            tag,
            sha,
            f"debug push probe {label}",
            user_name=ROBOT_NAME,
            user_email=ROBOT_EMAIL,
            retries=3,
        )
        pushed = True
    except Exception as e:  # Git.push_tag raises (strict) on a rejected push
        print(f"tag probe [{label}] push raised: {e}")
        pushed = False
    Git.push(REPO, f":refs/tags/{tag}", retries=3)  # delete the remote tag
    print(f"tag probe [{label}] pushed={pushed}")
    return pushed


def main():
    stopwatch = Utils.Stopwatch()
    _dump_git_auth_state()
    crwork_sha = Git.get_commit_sha("HEAD")
    Shell.check("git fetch --depth=1 origin master", strict=True, verbose=True)
    master_sha = Git.get_commit_sha("FETCH_HEAD")
    results = [
        Result.from_commands_run(
            name="Tag push at master tip",
            command=_tag_probe,
            command_args=["master", master_sha],
        ),
        Result.from_commands_run(
            name="Tag push at cr-work tip",
            command=_tag_probe,
            command_args=["cr-work", crwork_sha],
        ),
    ]
    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
