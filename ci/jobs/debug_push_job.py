#!/usr/bin/env python3
"""Debug job: isolate what makes the release push hit GitHub's workflow scan.

Runs on the release runner with ``enable_gh_auth=True`` (so ``gh`` and git
authenticate as the ``clickhouse-gh`` App, exactly like the release) and pushes
throwaway branches to the real repo, each deleted immediately.

We already know pushing the full-history ``cr-work`` tip (which modifies
``.github/workflows``) is rejected with "Unable to determine if workflow can be
created or updated due to timeout; ``workflows`` scope may be required", while an
orphan commit whose whole tree is a single non-workflow file pushes fine. The
orphan case is not a clean test though — its tree has no ``.github/workflows`` at
all. So here we base every probe on ``origin/master`` (whose ``.github/workflows``
the branch keeps unchanged) and push, over the *full* tree:

  * ``master unchanged``        — new branch at master's tip, zero diff.
  * ``master + non-workflow``   — one commit touching a file OUTSIDE
                                  ``.github/workflows``.

If both pass while the workflow-touching ``cr-work`` tip fails, the trigger is
the workflow-file *diff*, not the tree size or new-branch creation itself.
"""

import os
import re
import shlex

from ci.praktika.git import Git
from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

REPO = os.environ.get("GITHUB_REPOSITORY", "ClickHouse/ClickHouse")
PROBE_BRANCH = "robot-clickhouse/debug-push-probe"
ROBOT_NAME = "robot-clickhouse"
ROBOT_EMAIL = "robot-clickhouse@users.noreply.github.com"


def _dump_git_auth_state():
    # Origin file + key name only, never values — the extraheader carries a
    # token, so logging values would leak it.
    cfg = Shell.get_output(
        "git config --show-origin --name-only "
        "--get-regexp '^(http|url|credential)\\.' || true",
        verbose=False,
    )
    print(f"auth-related git config (origin + key, no values):\n{cfg or '(none)'}")
    url = Shell.get_output("git remote get-url origin", verbose=False) or ""
    print(f"remote.origin.url: {re.sub(r'://[^@/]*@', '://***@', url)}")


def _commit_file(path, content, message):
    full = os.path.join(os.getcwd(), path)
    parent = os.path.dirname(full)
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(full, "w") as f:
        f.write(content)
    Shell.check(f"git add -- {shlex.quote(path)}", strict=True, verbose=True)
    Shell.check(
        f"git -c user.name={ROBOT_NAME} -c user.email={ROBOT_EMAIL}"
        f" -c commit.gpgsign=false commit -m {shlex.quote(message)}",
        strict=True,
        verbose=True,
    )
    return Git.get_commit_sha("HEAD")


def _probe(label, path=None, content=None):
    """Push a branch at origin/master (optionally + a one-file commit); return bool.

    Starts detached at origin/master so the branch's ``.github/workflows`` equals
    master's — only ``path`` (never a workflow file here) differs. Deletes the
    remote branch afterwards.
    """
    branch = f"{PROBE_BRANCH}-{label}"
    print(f"=== probe [{label}] -> {REPO}@{branch} ===")
    Shell.check("git checkout -q --detach FETCH_HEAD", strict=True, verbose=True)
    if path:
        sha = _commit_file(path, content, f"debug push probe: {label}")
    else:
        sha = Git.get_commit_sha("HEAD")
    pushed = Git.push(REPO, f"{sha}:refs/heads/{branch}", force=True, retries=3)
    deleted = Git.push(REPO, f":refs/heads/{branch}", retries=3)
    print(f"probe [{label}] pushed={pushed} deleted={deleted}")
    return pushed


def main():
    stopwatch = Utils.Stopwatch()
    _dump_git_auth_state()
    Shell.check("git fetch --depth=1 origin master", strict=True, verbose=True)
    results = [
        Result.from_commands_run(
            name="Push master unchanged",
            command=_probe,
            command_args=["master-unchanged"],
        ),
        Result.from_commands_run(
            name="Push master + non-workflow file",
            command=_probe,
            command_args=["nonworkflow", "debug_push_probe.txt", "probe\n"],
        ),
    ]
    Result.create_from(results=results, stopwatch=stopwatch).complete_job()


if __name__ == "__main__":
    main()
