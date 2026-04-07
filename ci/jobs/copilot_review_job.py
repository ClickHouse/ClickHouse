"""
Copilot-based automated PR code review job.

--pre: review code only (Code Review job, runs at start of CI)
--post: review CI failures (CI Results Review job, runs at end of CI)

Copilot errors are logged as warnings only. Posting the review comment is
done by the job script itself (not copilot) and fails the job if it does not work.

Copilot writes the review to REVIEW_FILE; the job script then posts it via
`ci/praktika/gh.py post-or-update --tag review` so the comment is always posted
as the pre-authenticated app, not the Copilot robot.
"""

import os
import shlex
import subprocess
import sys
import tempfile
import traceback

from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result

REVIEW_FILE = "./ci/tmp/copilot_review.md"


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


def _run(prompt):
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
        Result.from_commands_run(
            name="copilot review",
            # --allow-all-tools: run non-interactively
            # --add-dir .: restrict file access to repo root (default,
            #   but explicit; do NOT add --allow-all-paths)
            command=f"GH_CONFIG_DIR={shlex.quote(gh_config_dir)} "
                    f"copilot -p {shlex.quote(prompt)} --allow-all-tools --add-dir . --model gpt-5.3-codex",
            with_info=True,
        )

    # Post the summary from the job script so the job fails loudly if anything is broken.
    if not os.path.exists(REVIEW_FILE):
        raise RuntimeError(f"Copilot did not write {REVIEW_FILE}")
    if os.path.getsize(REVIEW_FILE) == 0:
        raise RuntimeError(f"{REVIEW_FILE} is empty")
    _post_review()


def pre():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    _reauth_gh()
    os.makedirs("./ci/tmp", exist_ok=True)
    prompt = (
        f"Follow the Review Instructions in .claude/skills/review/SKILL.md. "
        f"Review PR {info.pr_url}. Repo is checked out at PR head. "
        f"Post findings as individual inline comments on specific lines using gh CLI (not a gh review). "
        f"Prefix every gh call with `env -u GH_CONFIG_DIR`. "
        f"IMPORTANT: when posting comments, ALWAYS write the comment body to a temp file first, "
        f"then use `-F body=@<file>` (for gh api) or `--body-file <file>` (for gh pr comment) "
        f"to avoid newlines being escaped to literal backslash-n. Never pass multi-line body text inline. "
        f"Read inline comments already posted by clickhouse-gh[bot]; do not post a new comment if a similar one already exists. "
        f"Write a self-contained summary of ALL findings (regardless of previous summaries) "
        f"as plain Markdown to {REVIEW_FILE} using the REQUESTED OUTPUT FORMAT from .claude/skills/review/SKILL.md — "
        f"start with `---\n#### AI Review`, then use ##### for section headers. "
        f"Do NOT post the summary yourself — the job script will post it after you finish."
    )
    _run(prompt)


def post():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    _reauth_gh()
    os.makedirs("./ci/tmp", exist_ok=True)
    ci_report_url = info.get_report_url()

    prompt = (
        f"Fetch CI results: node .claude/tools/fetch_ci_report.js '{ci_report_url}' --failed --links "
        f"(use --all, --test <name>, or --download-logs for deeper investigation). "
        f"If all checks passed — stop. "
        f"Otherwise review the PR {info.pr_url} diff and match each failure to the code changes. "
        f"Write a self-contained summary as plain Markdown to {REVIEW_FILE} — "
        f"start with `---\n### AI Review`, then use #### headers for sections only if needed. "
        f"Do NOT post the summary yourself — the job script will post it after you finish. "
        f"Do not post inline comments."
    )
    _run(prompt)


if __name__ == "__main__":
    status = Result.Status.SUCCESS
    info = ""
    if "--pre" in sys.argv:
        try:
            pre()
        except Exception as e:
            info = f"ERROR: {e}"
            print(info)
            traceback.print_exc()
            status = Result.Status.FAILED
    elif "--post" in sys.argv:
        try:
            post()
        except Exception as e:
            info = f"ERROR: {e}"
            print(info)
            traceback.print_exc()
            status = Result.Status.FAILED
    else:
        print("Usage: copilot_review_job.py --pre | --post")
        sys.exit(1)

    Result.create_from(status=status, info=info).complete_job()