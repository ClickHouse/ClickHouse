"""
Copilot-based automated PR code review job.

--pre: review code only (Code Review job, runs at start of CI)
--post: review CI failures (CI Results Review job, runs at end of CI)

Always succeeds — copilot errors are logged as warnings only.

Copilot writes the review to REVIEW_FILE and posts it via
`ci/praktika/gh.py post-or-update --tag review` using `env -u GH_CONFIG_DIR`
so the comment is posted as the pre-authenticated app, not the Copilot robot.
"""

import os
import shlex
import subprocess
import sys
import tempfile

from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result

REVIEW_FILE = "./ci/tmp/copilot_review.md"


def _run(prompt):
    with tempfile.TemporaryDirectory() as gh_config_dir:
        try:
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
        except Exception as e:
            print(f"WARNING: copilot review skipped: {e}")
            return

    # Copilot posts the summary itself via ci/praktika/gh.py post-or-update


def pre():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    os.makedirs("./ci/tmp", exist_ok=True)
    prompt = (
        f"Follow the Review Instructions in .claude/skills/review/SKILL.md. "
        f"Review PR {info.pr_url}. Repo is checked out at PR head. "
        f"Post findings as individual inline comments on specific lines using gh CLI (not a gh review). "
        f"Prefix every gh call with `env -u GH_CONFIG_DIR`. "
        f"Read inline comments already posted by clickhouse-gh[bot]; do not post a new comment if a similar one already exists. "
        f"Write a self-contained summary of ALL findings (regardless of previous summaries) "
        f"as plain Markdown to {REVIEW_FILE} using the REQUESTED OUTPUT FORMAT from .claude/skills/review/SKILL.md — "
        f"start with `---\n### AI Review`, then use #### for section headers. "
        f"Post it with: env -u GH_CONFIG_DIR python ci/praktika/gh.py post-or-update --tag review --file {REVIEW_FILE}"
    )
    _run(prompt)


def post():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    os.makedirs("./ci/tmp", exist_ok=True)
    ci_report_url = info.get_report_url()

    prompt = (
        f"Fetch CI results: node .claude/tools/fetch_ci_report.js '{ci_report_url}' --failed --links "
        f"(use --all, --test <name>, or --download-logs for deeper investigation). "
        f"If all checks passed — stop. "
        f"Otherwise review the PR {info.pr_url} diff and match each failure to the code changes. "
        f"Write a self-contained summary as plain Markdown to {REVIEW_FILE} — "
        f"start with `---\n### AI Review`, then use #### headers for sections only if needed. "
        f"Post it with: env -u GH_CONFIG_DIR python ci/praktika/gh.py post-or-update --tag review --file {REVIEW_FILE} "
        f"Do not post inline comments."
    )
    _run(prompt)


if __name__ == "__main__":
    if "--pre" in sys.argv:
        pre()
    elif "--post" in sys.argv:
        post()
    else:
        print("Usage: copilot_review_job.py --pre | --post")
        sys.exit(1)

    Result.create_from(status=Result.Status.SUCCESS).complete_job()
