"""
Copilot-based automated PR code review job.

--pre: review code only (Code Review job, runs at start of CI)
--post: review CI failures (CI Results Review job, runs at end of CI)

Copilot errors are logged as warnings only. Posting the review comment is
done by the job script itself (not copilot) and fails the job if it does not work.

Copilot writes the review to REVIEW_FILE; the job script then posts it via
`ci/praktika/gh.py post-or-update --tag review` so the comment is always posted
as the pre-authenticated app, not the Copilot robot.

The Copilot CLI occasionally hits transient GitHub authorization errors mid-run
("Authorization error, you may need to run /login"). The whole `gh auth` +
`copilot` sequence is wrapped in a retry loop with exponential backoff so a
single transient API failure does not fail the Code Review job.
"""

import os
import shlex
import subprocess
import sys
import tempfile
import time
import traceback

from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result

REVIEW_FILE = "./ci/tmp/copilot_review.md"

# Number of attempts at the full gh-auth + copilot run. The Copilot CLI
# fetches auth state and makes GitHub API calls during execution; either
# layer can hit a transient 5xx / authorization error that no single inner
# subprocess controls. Re-authing and retrying the whole sequence is the
# only reliable way to recover.
COPILOT_MAX_ATTEMPTS = 3


def _reauth_gh():
    """Force re-auth of the main gh context (outside any GH_CONFIG_DIR override).

    Called at job start regardless of current auth status, so the token is
    always fresh when copilot invokes `env -u GH_CONFIG_DIR`.
    """
    from ci.praktika.gh_auth import GHAuth

    GHAuth.auth_from_settings()


def _post_review():
    """Post REVIEW_FILE as a PR comment. Raises on failure, failing the job."""
    subprocess.run(
        [
            sys.executable, "ci/praktika/gh.py",
            "post-or-update", "--tag", "review", "--file", REVIEW_FILE,
        ],
        check=True,
    )


def _run_copilot_once(prompt):
    """Run a single attempt of `gh auth login` + `copilot`.

    Removes any stale REVIEW_FILE first so a successful prior attempt's
    artifact cannot be mistaken for the result of a later failed attempt.
    Returns the praktika ``Result`` of the copilot subprocess.
    """
    if os.path.exists(REVIEW_FILE):
        try:
            os.unlink(REVIEW_FILE)
        except OSError as e:
            print(f"WARNING: Failed to remove stale {REVIEW_FILE}: {e}")

    with tempfile.TemporaryDirectory() as gh_config_dir:
        token = Secret.Config(
            name="/ci/robot-ch-test-poll-copilot", type=Secret.Type.AWS_SSM_PARAMETER
        ).get_value()
        subprocess.run(
            ["gh", "auth", "login", "--with-token"],
            input=token, text=True, check=True,
            env={**os.environ, "GH_CONFIG_DIR": gh_config_dir},
        )
        token = None
        return Result.from_commands_run(
            name="copilot review",
            # --allow-all-tools: run non-interactively
            # --add-dir .: restrict file access to repo root (default,
            #   but explicit; do NOT add --allow-all-paths)
            command=f"GH_CONFIG_DIR={shlex.quote(gh_config_dir)} "
                    f"copilot -p {shlex.quote(prompt)} --allow-all-tools --add-dir . --model gpt-5.3-codex",
            with_info=True,
        )


def _run(prompt):
    """Run Copilot with retries, then post the review comment.

    Each attempt re-fetches the secret, re-auths the temporary
    ``GH_CONFIG_DIR``, and re-runs Copilot. The attempt counts as success
    only if the copilot subprocess exits 0 AND ``REVIEW_FILE`` was written
    with non-empty content. Authorization-style failures inside Copilot do
    not surface as a Python exception — they show up as a non-zero exit
    code and a missing review file — so both have to be checked here.
    """
    last_error = None
    for attempt in range(1, COPILOT_MAX_ATTEMPTS + 1):
        try:
            result = _run_copilot_once(prompt)
            if not result.is_ok():
                last_error = (
                    f"copilot subprocess exited with non-OK status [{result.status}]"
                )
                print(f"WARNING: Copilot attempt {attempt}/{COPILOT_MAX_ATTEMPTS} failed: {last_error}")
            elif not os.path.exists(REVIEW_FILE):
                last_error = f"Copilot did not write {REVIEW_FILE}"
                print(f"WARNING: Copilot attempt {attempt}/{COPILOT_MAX_ATTEMPTS} failed: {last_error}")
            elif os.path.getsize(REVIEW_FILE) == 0:
                last_error = f"{REVIEW_FILE} is empty"
                print(f"WARNING: Copilot attempt {attempt}/{COPILOT_MAX_ATTEMPTS} failed: {last_error}")
            else:
                last_error = None
                break
        except Exception as e:  # noqa: BLE001 — broad catch: any exception is retryable here
            last_error = f"{type(e).__name__}: {e}"
            print(f"WARNING: Copilot attempt {attempt}/{COPILOT_MAX_ATTEMPTS} raised: {last_error}")
            traceback.print_exc()

        if attempt < COPILOT_MAX_ATTEMPTS:
            delay = min(2 ** attempt, 60)
            print(f"Retrying Copilot in {delay}s ...")
            time.sleep(delay)

    if last_error is not None:
        raise RuntimeError(
            f"Copilot review failed after {COPILOT_MAX_ATTEMPTS} attempts: {last_error}"
        )

    # Post the summary from the job script so the job fails loudly if anything is broken.
    _post_review()


def pre():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    _reauth_gh()
    os.makedirs("./ci/tmp", exist_ok=True)
    prompt = f"""\
Follow the Review Instructions in .claude/skills/review/SKILL.md.
Review PR {info.pr_url}. Repo is checked out at PR head.
Post findings as individual inline review comments on specific lines.
Prefix every gh call with `env -u GH_CONFIG_DIR`.

IMPORTANT: to post an inline review comment, write the body to a file and call:
`env -u GH_CONFIG_DIR python3 ci/praktika/gh.py post-pr-line-comment --file <body.md> --commit <sha> --path <file> --line <N> [--side RIGHT|LEFT]`.
This wrapper handles `-F body=@<file>` correctly so the file content is uploaded as the body.
Do NOT call `gh api .../pulls/.../comments` directly: past runs have posted the literal `@<file>`
string as the body when the wrong gh flag was used.
Do NOT use `gh pr review` (that posts a single batched review, not individual line comments).

Before reviewing, fetch all prior discussion on this PR.
Inline review threads come from
`env -u GH_CONFIG_DIR python3 ci/praktika/gh.py list-pr-review-threads --pr {info.pr_number}`
(returns JSON; each thread carries its node `id`, `isResolved`, `path`, `line`, and `comments.nodes`
with author, body, and `databaseId`).
Top-level conversation comes from
`env -u GH_CONFIG_DIR gh api '/repos/{{owner}}/{{repo}}/issues/{info.pr_number}/comments' --paginate`.

Read every reply on every thread before deciding to raise a point.
A reply from the author is not by itself a resolution.
Judge it on its merits: an explanation that holds up, a pointer to a commit that actually fixes
the issue, or a tradeoff you agree with means drop the point.
A handwave, a misunderstanding of what was originally flagged, a 'will fix later' that never
landed, or a claim that contradicts the code as it stands now means the issue is still live.
In that case, push back: reply on the existing thread, restate the concern with the relevant code,
and explain why the previous answer does not resolve it.

To reply on a thread, use the same wrapper with `--reply-to <parent_databaseId>` (the `databaseId`
of the first comment on the thread) and omit `--commit`/`--path`/`--line`:
`env -u GH_CONFIG_DIR python3 ci/praktika/gh.py post-pr-line-comment --file <body.md> --reply-to <parent_databaseId>`.
Do not open a fresh inline comment for a point that already has a thread; reply on the thread instead.
Apply the same judgment to your own prior summary: drop findings that have genuinely been addressed,
keep or sharpen the ones that have not.

After reviewing, mark threads as resolved or unresolved based on the current code.
Only act on threads you created yourself: that means threads where the first comment in
`comments.nodes` was authored by `clickhouse-gh[bot]`.
Never resolve or unresolve threads where the first comment was authored by anyone else
(other reviewers, the PR author, other bots), even when the code suggests the thread should
be resolved or re-opened.
For each bot-authored thread that is not already resolved, if the issue you flagged no longer
holds in the current code, resolve it via
`env -u GH_CONFIG_DIR python3 ci/praktika/gh.py resolve-pr-review-thread --thread-id <thread_node_id>`.
If a bot-authored thread was already resolved (by you or someone else) but the current code still
has the issue, you can re-open it via
`env -u GH_CONFIG_DIR python3 ci/praktika/gh.py unresolve-pr-review-thread --thread-id <thread_node_id>`
and post a follow-up reply explaining what is still wrong.
Verify the state by reading the current code at the thread's path and line, not by trusting
the author's reply.

Write a self-contained summary of ALL findings (regardless of previous summaries) as plain Markdown
to {REVIEW_FILE} using the REQUESTED OUTPUT FORMAT from .claude/skills/review/SKILL.md:
start with `---
#### AI Review`, then use ##### for section headers.
Do NOT post the summary yourself: the job script will post it after you finish.
"""
    _run(prompt)


def post():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    _reauth_gh()
    os.makedirs("./ci/tmp", exist_ok=True)
    ci_report_url = info.get_report_url()

    prompt = f"""\
Fetch CI results: node .claude/tools/fetch_ci_report.js '{ci_report_url}' --failed --links
(use --all, --test <name>, or --download-logs for deeper investigation).
If all checks passed: stop.
Otherwise review the PR {info.pr_url} diff and match each failure to the code changes.
Write a self-contained summary as plain Markdown to {REVIEW_FILE}:
start with `---
### AI Review`, then use #### headers for sections only if needed.
Do NOT post the summary yourself: the job script will post it after you finish.
Do not post inline comments.
"""
    _run(prompt)


if __name__ == "__main__":
    status = Result.Status.OK
    info = ""
    if "--pre" in sys.argv:
        try:
            pre()
        except Exception as e:
            info = f"ERROR: {e}"
            print(info)
            traceback.print_exc()
            status = Result.Status.FAIL
    elif "--post" in sys.argv:
        try:
            post()
        except Exception as e:
            info = f"ERROR: {e}"
            print(info)
            traceback.print_exc()
            status = Result.Status.FAIL
    else:
        print("Usage: copilot_review_job.py --pre | --post")
        sys.exit(1)

    Result.create_from(status=status, info=info).complete_job()
