"""
Copilot-based automated PR code review job.

--pre: review code only (Code Review job, runs at start of CI)
--post: review CI failures (CI Results Review job, runs at end of CI)

Copilot errors are logged as warnings only. Posting the review comment is
done by the job script itself (not copilot) and fails the job if it does not work.

Copilot writes the review to REVIEW_FILE; the job script then posts it via
`python3 -m ci.praktika.gh post-or-update --tag review` so the comment is
always posted as the pre-authenticated app, not the Copilot robot.

The Copilot CLI occasionally hits transient GitHub authorization errors mid-run
("Authorization error, you may need to run /login"). The whole `gh auth` +
`copilot` sequence is wrapped in a retry loop with exponential backoff so a
single transient API failure does not fail the Code Review job.
"""

import os
import random
import shlex
import subprocess
import sys
import tempfile
import time
import traceback
import urllib.parse

from ci.praktika import Secret
from ci.praktika.info import Info
from ci.praktika.result import Result

REVIEW_FILE = "./ci/tmp/copilot_review.md"
GH_PREFIX = "env -u GH_CONFIG_DIR"

# Number of attempts at the full gh-auth + copilot run. The Copilot CLI
# fetches auth state and makes GitHub API calls during execution; either
# layer can hit a transient 5xx / authorization error that no single inner
# subprocess controls. Re-authing and retrying the whole sequence is the
# only reliable way to recover.
COPILOT_MAX_ATTEMPTS = 3

ROBOT_NAMES = [
    "/ci/robot-ch-test-poll-copilot",
    "/ci/robot-ch-test-poll-1-copilot",
]


def _join_prompt(*sections):
    return "\n\n".join(section.rstrip() for section in sections if section).rstrip() + "\n"


def _repo_from_pr_url(pr_url):
    path_parts = urllib.parse.urlparse(pr_url).path.strip("/").split("/")
    if len(path_parts) >= 4 and path_parts[2] == "pull":
        return f"{path_parts[0]}/{path_parts[1]}"
    return ""


def _pr_repository(info):
    return _repo_from_pr_url(info.pr_url) or info.repo_name


def _pre_review_instructions():
    return """\
Review instructions:
- Follow the Review Instructions in .claude/skills/review/SKILL.md.
- Repo is checked out at PR head.
- Post findings as individual inline review comments on specific lines.
"""


def _review_target(info):
    repo_name = _pr_repository(info)
    return f"""\
Review target:
- PR URL: {info.pr_url}
- PR repository: `{repo_name}`
- Always derive the PR repository from the PR URL or the CI event. For ClickHouse reviews this will be either
  `ClickHouse/ClickHouse` or `ClickHouse/ClickHouse-private`. Do not infer the review repository from
  the local checkout remote, because local checkouts may point to a fork.
"""


def _pre_review_tools(pr_number, repo_name):
    return f"""\
Tools:
- Prefix every `gh` call with `{GH_PREFIX}`.
- Pass the explicit PR repository to every helper or `gh` command that accepts it: `--repo {repo_name}`.
- Post a new inline review comment by writing the body to a file and running:
  `{GH_PREFIX} python3 -m ci.praktika.gh post-pr-line-comment --file <body.md> --commit <sha> --path <file> --line <N> --repo {repo_name} [--side RIGHT|LEFT]`.
  This wrapper handles `-F body=@<file>` correctly so the file content is uploaded as the body.
- Do NOT call `gh api .../pulls/.../comments` directly: past runs have posted the literal `@<file>`
  string as the body when the wrong `gh` flag was used.
- Do NOT use `gh pr review`; that posts a single batched review, not individual line comments.
- Fetch inline review threads with:
  `{GH_PREFIX} python3 -m ci.praktika.gh list-pr-review-threads --pr {pr_number} --repo {repo_name}`.
  The command returns JSON; each thread carries its node `id`, `isResolved`, `path`, `line`, and
  `comments.nodes` with author, body, and `databaseId`.
- Fetch top-level conversation with:
  `{GH_PREFIX} gh api '/repos/{repo_name}/issues/{pr_number}/comments' --paginate`.
- Reply on an existing thread with:
  `{GH_PREFIX} python3 -m ci.praktika.gh post-pr-line-comment --file <body.md> --reply-to <parent_databaseId> --repo {repo_name}`.
  Use the `databaseId` of the first comment on the thread as `<parent_databaseId>`, and omit
  `--commit`, `--path`, and `--line`.
- Resolve or unresolve bot-authored review threads with:
  `{GH_PREFIX} python3 -m ci.praktika.gh resolve-pr-review-thread --thread-id <thread_node_id>`
  `{GH_PREFIX} python3 -m ci.praktika.gh unresolve-pr-review-thread --thread-id <thread_node_id>`.
"""


def _pre_review_procedure(pr_url):
    return f"""\
Procedure:
1. In GitHub discussions, "you" are `clickhouse-gh[bot]`.
2. Fetch all prior discussion on this PR before reviewing.
3. Provide a thorough review of {pr_url}. Read the current code and PR diff, not only the discussion.
4. Read every reply on every thread before deciding whether a point is still live. A reply from the author is not
   by itself a resolution. Judge it on its merits: an explanation that holds up, a pointer to a commit
   that actually fixes the issue, or a tradeoff you agree with means drop the point. A handwave, a
   misunderstanding of what was originally flagged, a "will fix later" that never landed, or a claim
   that contradicts the code as it stands now means the issue is still live.
5. Apply the same judgment to your own prior summary: drop findings that have genuinely been addressed,
   keep or sharpen the ones that have not. Verify this by reading the current code at the relevant path
   and line, not by trusting the author's reply.
6. For existing live threads, summarize the issue by default instead of replying on the thread. Reply
   only when the thread is marked as resolved, someone replied saying it is not an issue and you
   believe it still is, or your last comment is older than two days. When replying, restate the concern
   with the relevant code and explain why the previous answer does not resolve it.
7. Resolve or re-open only threads you created yourself: that means threads where the first comment in
   `comments.nodes` was authored by `clickhouse-gh[bot]`. If a bot-authored thread is open and the issue
   no longer holds in the current code, resolve it. If a bot-authored thread was resolved but the
   current code still has the issue, re-open it and post a follow-up reply explaining what is still
   wrong. Never resolve or unresolve threads where the first comment was authored by anyone else.
8. For genuinely new issues that do not already have a thread, post individual inline comments on the
   relevant changed lines. For architectural issues that do not map cleanly to one line, post around
   the most relevant change in the diff.
"""


def _pre_review_output():
    return f"""\
Output:
Write a self-contained summary of ALL findings, regardless of previous summaries, as plain Markdown
to {REVIEW_FILE} using the REQUESTED OUTPUT FORMAT from .claude/skills/review/SKILL.md:
start with `---
#### AI Review`, then use ##### for section headers.
Do NOT post the summary yourself: the job script will post it after you finish.
"""


def _pre_review_prompt(info):
    repo_name = _pr_repository(info)
    return _join_prompt(
        _pre_review_instructions(),
        _review_target(info),
        _pre_review_tools(info.pr_number, repo_name),
        _pre_review_procedure(info.pr_url),
        _pre_review_output(),
    )


def _post_review_tools(ci_report_url):
    return f"""\
Tools:
- Fetch CI results with:
  `node .claude/tools/fetch_ci_report.js '{ci_report_url}' --failed --links`.
- Use `--all`, `--test <name>`, or `--download-logs` for deeper investigation.
"""


def _post_review_procedure(pr_url):
    return f"""\
Procedure:
1. If all checks passed, stop.
2. Otherwise review the PR {pr_url} diff and match each failure to the code changes.
3. Do not post inline comments.
"""


def _post_review_output():
    return f"""\
Output:
Write a self-contained summary as plain Markdown to {REVIEW_FILE}:
start with `---
### AI Review`, then use #### headers for sections only if needed.
Do NOT post the summary yourself: the job script will post it after you finish.
"""


def _post_review_prompt(info, ci_report_url):
    return _join_prompt(
        _review_target(info),
        _post_review_tools(ci_report_url),
        _post_review_procedure(info.pr_url),
        _post_review_output(),
    )


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
            sys.executable, "-m", "ci.praktika.gh",
            "post-or-update", "--tag", "review", "--file", REVIEW_FILE,
        ],
        check=True,
    )


def _run_copilot_once(prompt, robot_name):
    """Run a single attempt of `gh auth login` + `copilot` for one robot.

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
        print(f"Using robot: {robot_name}")
        token = Secret.Config(
            name=robot_name,
            type=Secret.Type.AWS_SSM_PARAMETER,
        ).get_value()
        subprocess.run(
            ["gh", "auth", "login", "--with-token"],
            input=token, text=True, check=True,
            env={**os.environ, "GH_CONFIG_DIR": gh_config_dir},
        )
        token = None
        return Result.from_commands_run(
            name="copilot review",
            # --allow-all: enable all permissions; --allow-all-tools alone hits
            #   a CLI bug where compound shell commands are denied and the gate
            #   then tries to escalate to a human (github/copilot-cli#176, #2971)
            # --no-ask-user: disable ask_user so the agent cannot try to prompt
            #   for permission in a non-interactive session
            # --add-dir .: restrict file access to repo root (default, but explicit)
            # </dev/null: ensure stdin is definitively non-interactive
            command=f"GH_CONFIG_DIR={shlex.quote(gh_config_dir)} "
                    f"copilot -p {shlex.quote(prompt)} --allow-all --no-ask-user "
                    f"--add-dir . --model gpt-5.3-codex --effort xhigh < /dev/null",
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
    robots = ROBOT_NAMES.copy()
    random.shuffle(robots)
    for attempt in range(1, COPILOT_MAX_ATTEMPTS + 1):
        robot_name = robots[(attempt - 1) % len(robots)]
        try:
            result = _run_copilot_once(prompt, robot_name)
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
    prompt = _pre_review_prompt(info)
    _run(prompt)


def post():
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return

    _reauth_gh()
    os.makedirs("./ci/tmp", exist_ok=True)
    ci_report_url = info.get_report_url()

    prompt = _post_review_prompt(info, ci_report_url)
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
