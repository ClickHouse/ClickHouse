"""Six-hourly job: bump `contrib/silk` to the tip of silk's `clickhouse-public` branch.

Opens (or refreshes) a single bot pull request. The bot branch is stable so
re-runs update one PR instead of opening a new one every six hours.

Fail-closed: the PR is only touched when the submodule moved to a real new
commit; a failed fetch or checkout never pushes a branch or opens a PR.
"""

import os
import re
import shlex
import sys

from ci.defs.defs import BASE_BRANCH
from praktika.info import Info
from praktika.result import Result
from praktika.utils import Shell

SILK_PATH = "contrib/silk"
SILK_BRANCH = "clickhouse-public"
SILK_REPOSITORY = "ClickHouse/silk"
BOT_BRANCH = "robot/bump-silk"
REPOSITORY = "ClickHouse/ClickHouse"

PR_TITLE = "Bump `silk` to the latest `clickhouse-public`"

TOKENIZED_GITHUB_URL = re.compile(r"(https://x-access-token:)[^@\s]+(@github\.com/)")


def on_current_base_branch():
    """Refuse to run unless HEAD is the current tip of the base branch.

    The workflow is SCHEDULE-triggered, but GitHub also generates a
    `workflow_dispatch` trigger and reruns replay their original ref/sha. The
    branch-name check rejects feature-branch dispatches; fetching the remote base
    branch and comparing its tip with HEAD also rejects reruns of older `master`
    commits. The check runs again after the bump so a run cannot publish if
    `master` advanced while it was working."""
    branch = Info().git_branch
    if branch != BASE_BRANCH:
        print(
            f"Refusing to run: this workflow force-pushes '{BOT_BRANCH}' and manages a"
            f" pull request, so it must run against '{BASE_BRANCH}', not '{branch}'"
            " (e.g. a manual workflow_dispatch or a rerun from another ref).",
            file=sys.stderr,
        )
        return False

    if not Shell.check(
        f"git fetch --no-tags origin {shlex.quote(BASE_BRANCH)}", verbose=True
    ):
        print(
            f"Refusing to run: could not fetch the current '{BASE_BRANCH}' tip.",
            file=sys.stderr,
        )
        return False

    head = Shell.get_output("git rev-parse HEAD").strip()
    current_base = Shell.get_output("git rev-parse FETCH_HEAD").strip()
    if not head or not current_base or head != current_base:
        print(
            f"Refusing to run: HEAD ({head or 'unknown'}) is not the current "
            f"'{BASE_BRANCH}' tip ({current_base or 'unknown'}).",
            file=sys.stderr,
        )
        return False
    return True


def pinned_commit():
    return Shell.get_output(f"git rev-parse HEAD:{SILK_PATH}").strip()


def bump():
    if not Shell.check(
        f"git submodule update --init {SILK_PATH} && "
        f"git -C {SILK_PATH} fetch origin {SILK_BRANCH}",
        verbose=True,
    ):
        return False

    new = Shell.get_output(f"git -C {SILK_PATH} rev-parse FETCH_HEAD").strip()
    if not new:
        print(f"ERROR: failed to resolve the tip of {SILK_BRANCH}")
        return False

    return Shell.check(f"git -C {SILK_PATH} checkout --detach {new}", verbose=True)


def has_changes():
    return bool(Shell.get_output(f"git status --porcelain -- {SILK_PATH}").strip())


def pr_body():
    old = pinned_commit()
    new = Shell.get_output(f"git -C {SILK_PATH} rev-parse HEAD").strip()
    changes = Shell.get_output(
        f"git -C {SILK_PATH} log --no-merges --oneline {old}..{new}"
    ).strip()

    return f"""\
Automated bump of `{SILK_PATH}` from `{old[:11]}` to `{new[:11]}`, the tip of \
[`{SILK_BRANCH}`](https://github.com/{SILK_REPOSITORY}/tree/{SILK_BRANCH}) in \
[{SILK_REPOSITORY}](https://github.com/{SILK_REPOSITORY}).

Changes in `silk` \
([compare](https://github.com/{SILK_REPOSITORY}/compare/{old}...{new})):

```
{changes}
```

This pull request self-updates until it is merged.

### Changelog category (leave one):
- Not for changelog (changelog entry is not required)
"""


def open_or_refresh_pr(body):
    repository = os.getenv("GITHUB_REPOSITORY", "")
    if repository != REPOSITORY:
        print(
            "ERROR: the silk sync workflow must run only in "
            f"{REPOSITORY}, got {repository or '(unset)'}"
        )
        return False

    prepare = [
        'git config user.name "robot-clickhouse"',
        'git config user.email "robot-clickhouse@users.noreply.github.com"',
        f"git checkout -B {BOT_BRANCH}",
        f"git add {SILK_PATH}",
        f"git commit -m {shlex.quote(PR_TITLE)}",
    ]
    if not Shell.check(" && ".join(prepare), verbose=True):
        return False

    repo_url = (
        "https://x-access-token:${token}@github.com/" + shlex.quote(REPOSITORY) + ".git"
    )
    refspec = shlex.quote(f"{BOT_BRANCH}:refs/heads/{BOT_BRANCH}")
    push = (
        'token="$(gh auth token)" && '
        "git -c http.https://github.com/.extraheader= push -f "
        f"{repo_url} {refspec}"
    )
    return_code, stdout, stderr = Shell.get_res_stdout_stderr(push, verbose=False)
    if return_code != 0:
        output = "\n".join(part for part in (stdout, stderr) if part)
        output = TOKENIZED_GITHUB_URL.sub(r"\1***\2", output)
        print("ERROR: failed to push the silk bump bot branch")
        if output:
            print(output)
        return False

    existing = Shell.get_output(
        f"gh pr list --head {BOT_BRANCH} --base {BASE_BRANCH} --state open "
        f"--repo {shlex.quote(REPOSITORY)} "
        "--json number,isCrossRepository "
        "--jq 'map(select(.isCrossRepository == false)) | .[0].number // empty'"
    ).strip()
    if existing:
        print(f"PR #{existing} already open; branch refreshed")
        return Shell.check(
            f"gh pr edit {existing} --repo {shlex.quote(REPOSITORY)} "
            f"--body {shlex.quote(body)}",
            verbose=True,
        )
    return Shell.check(
        f"gh pr create --base {BASE_BRANCH} --head {BOT_BRANCH} "
        f"--repo {shlex.quote(REPOSITORY)} "
        f"--title {shlex.quote(PR_TITLE)} --body {shlex.quote(body)}",
        verbose=True,
    )


if __name__ == "__main__":
    results = [
        Result.from_commands_run(
            name="Run against current master", command=on_current_base_branch
        )
    ]

    if results[-1].is_ok():
        results.append(Result.from_commands_run(name="Bump submodule", command=bump))

    if results[-1].is_ok():
        if has_changes():
            results.append(
                Result.from_commands_run(
                    name="Master is still current", command=on_current_base_branch
                )
            )
            if results[-1].is_ok():
                body = pr_body()
                results.append(
                    Result.from_commands_run(
                        name="Open or refresh PR",
                        command=lambda: open_or_refresh_pr(body),
                    )
                )
        else:
            results.append(
                Result.from_commands_run(
                    name="Open or refresh PR", command=lambda: True
                )
            )
            results[-1].set_info(
                f"No changes; {SILK_PATH} already at the {SILK_BRANCH} tip"
            )

    Result.create_from(results=results).complete_job()
