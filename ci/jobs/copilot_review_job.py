"""
AI-based automated PR code review job.

Two backends are supported, selected by flag:

  --codex    OpenAI Codex CLI       (auth: OPENAI_API_KEY from `/ci/llm/openai_api_key`)
  --copilot  GitHub Copilot CLI     (auth: gh robot token from `/ci/robot-ch-test-poll-copilot`)

A run has three stages, and only the middle one involves the agent:

1. Context. The job fetches the PR, its diff, review threads, conversation,
   linked issues, CI status and the previous AI review into `CONTEXT_DIR`
   (`ai_review/context.py`), and a Loom code-index brief for the diff
   (`ai_review/loom.py`) when Loom is configured for the repository.
2. Review. The agent reads the context and the checkout, may query Loom through
   `python3 -m ci.jobs.scripts.ai_review.loom`, and writes its summary, inline
   comments and thread actions as files into `OUTPUT_DIR`. The Codex agent runs
   with no GitHub credentials. An attempt is retried when the agent fails or
   writes no summary; nothing has been posted at that point, so a retry cannot
   duplicate comments.
3. Publish. The job validates the inline comments against the diff and the
   thread actions against the thread ownership rules (`ai_review/publish.py`),
   posts the inline comments as one review, applies the thread actions, and
   posts the summary via `post-or-update --tag review` as the pre-authenticated
   app, with a hidden marker of the reviewed commit for the next run.
"""

import json
import os
import random
import shlex
import sys
import tempfile
import time
import traceback
import urllib.parse

from ci.jobs.scripts.ai_review import context as review_context
from ci.jobs.scripts.ai_review import loom, prompt, publish
from ci.praktika import Secret
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.utils import Shell

WORK_DIR = "./ci/tmp/ai_review"
CONTEXT_DIR = f"{WORK_DIR}/context"
LOOM_DIR = f"{CONTEXT_DIR}/loom"
OUTPUT_DIR = f"{WORK_DIR}/out"
SUMMARY_FILE = f"{OUTPUT_DIR}/summary.md"
PROMPT_FILE = f"{WORK_DIR}/prompt.md"
LOOM_CALL_LOG = f"{WORK_DIR}/loom_calls.jsonl"

MODEL = "gpt-5.4"
REASONING_EFFORT = "xhigh"

# Number of attempts at a full agent run. The agents make model-provider API
# calls during execution, which can hit transient 5xx errors that no single
# inner subprocess controls. Retrying the whole run is the only reliable way
# to recover.
MAX_ATTEMPTS = 3
# Wall-clock limit of one attempt, and the point after which no new attempt is
# started. A hung agent otherwise holds the runner until the job timeout.
ATTEMPT_TIMEOUT_SECONDS = 50 * 60
NO_NEW_ATTEMPT_AFTER_SECONDS = 100 * 60

# Linux limits a single command-line argument to 128 KiB. The Copilot CLI takes
# the prompt as an argument; above this size it is pointed at the prompt file.
_MAX_PROMPT_ARGUMENT = 120_000

# Robot gh tokens the Copilot CLI authenticates against GitHub with. Each
# attempt picks one in a randomised rotation so a single robot's rate limit
# or token issue does not fail every attempt.
ROBOT_NAMES = [
    "/ci/robot-ch-test-poll-copilot",
    "/ci/robot-ch-test-poll-1-copilot",
]

# OpenAI API key for the Codex CLI, written into `$CODEX_HOME/auth.json`
# via `codex login --with-api-key`.
OPENAI_KEY_SECRET = "/ci/llm/openai_api_key"


def _repo_from_pr_url(pr_url):
    path_parts = urllib.parse.urlparse(pr_url).path.strip("/").split("/")
    if len(path_parts) >= 4 and path_parts[2] == "pull":
        return f"{path_parts[0]}/{path_parts[1]}"
    return ""


def _pr_repository(info):
    # Always derive the PR repository from the PR URL or the CI event, never
    # from the local checkout remote, which may point to a fork.
    return _repo_from_pr_url(info.pr_url) or info.repo_name


def _ssm(name):
    return Secret.Config(name=name, type=Secret.Type.AWS_SSM_PARAMETER, region="us-east-1").get_value()


def _reset_output_dir():
    """Remove the previous attempt's outputs so they cannot be mistaken for the
    result of a later failed attempt."""
    Shell.check(f"rm -rf {shlex.quote(OUTPUT_DIR)}", verbose=False)
    os.makedirs(f"{OUTPUT_DIR}/comments", exist_ok=True)
    os.makedirs(f"{OUTPUT_DIR}/replies", exist_ok=True)


def _agent_env(loom_config, extra=None):
    """Environment of the agent process: the job's environment without GitHub
    credentials, plus the Loom configuration for the Loom CLI."""
    env = {k: v for k, v in os.environ.items() if k not in ("GH_TOKEN", "GITHUB_TOKEN", "GH_ENTERPRISE_TOKEN")}
    env.update(loom_config.env())
    env["LOOM_CALL_LOG"] = os.path.abspath(LOOM_CALL_LOG)
    env["PYTHONPATH"] = os.pathsep.join(p for p in (os.getcwd(), env.get("PYTHONPATH", "")) if p)
    env.update(extra or {})
    return env


def _run_copilot_once(loom_config, robot_name):
    """One attempt: `gh auth login` with a robot token (the Copilot CLI's own
    authentication) + `copilot`."""
    with tempfile.TemporaryDirectory() as gh_config_dir:
        print(f"Using robot: {robot_name}")
        Shell.check(
            "gh auth login --with-token", stdin_str=_ssm(robot_name), strict=True, verbose=False,
            env={**os.environ, "GH_CONFIG_DIR": gh_config_dir},
        )
        with open(PROMPT_FILE, "r", encoding="utf-8") as f:
            size = len(f.read().encode())
        prompt_arg = (
            f'"$(cat {shlex.quote(PROMPT_FILE)})"' if size <= _MAX_PROMPT_ARGUMENT
            else shlex.quote(f"Read {PROMPT_FILE} and follow the instructions in it exactly.")
        )
        # --allow-all: enable all permissions; --allow-all-tools alone hits
        #   a CLI bug where compound shell commands are denied and the gate
        #   then tries to escalate to a human (github/copilot-cli#176, #2971)
        # --no-ask-user: disable ask_user so the agent cannot try to prompt
        #   for permission in a non-interactive session
        # --add-dir .: restrict file access to repo root (default, but explicit)
        # </dev/null: ensure stdin is definitively non-interactive
        return Shell.run(
            f"copilot -p {prompt_arg} --allow-all --no-ask-user --add-dir . "
            f"--model {MODEL} --effort {REASONING_EFFORT} < /dev/null",
            timeout=ATTEMPT_TIMEOUT_SECONDS,
            env=_agent_env(loom_config, {"GH_CONFIG_DIR": gh_config_dir}),
        )


def _run_codex_once(loom_config, _robot_name):
    """One attempt: `codex login` + `codex exec`.

    Codex stores credentials in `$CODEX_HOME/auth.json` and does NOT consult
    `OPENAI_API_KEY` directly when invoked — you have to run
    `codex login --with-api-key` first, which reads the key from stdin and
    writes it into `auth.json`. `CODEX_HOME` is scoped to a per-attempt
    temporary directory under `./ci/tmp` (not `/tmp`, which codex refuses
    to use for helper binaries) so the API key never lands on global runner
    state.

    The agent gets no GitHub credentials: `GH_CONFIG_DIR` points at an empty
    directory and `GH_TOKEN` is not passed. Everything it needs from GitHub is
    in the prefetched context, and the job posts its output.
    """
    with tempfile.TemporaryDirectory(dir="./ci/tmp") as codex_home, \
            tempfile.TemporaryDirectory(dir="./ci/tmp") as empty_gh_config:
        Shell.check(
            "codex login --with-api-key", stdin_str=_ssm(OPENAI_KEY_SECRET), strict=True, verbose=False,
            env={**os.environ, "CODEX_HOME": codex_home},
        )
        # -m: same model the Copilot CLI uses, so review quality stays
        #   comparable across backends.
        # -s workspace-write: writable workspace + /tmp + CODEX_HOME,
        #   read-only elsewhere; sufficient for the review output.
        # sandbox_workspace_write.network_access=true: the Loom CLI needs
        #   network.
        # approval_policy=never: codex `exec` is non-interactive,
        #   but the approval policy still applies; "never" lets the
        #   agent execute without blocking on an approval request.
        # --color never: no ANSI codes in the job log.
        # `-` reads the prompt from stdin, which has no argument size limit.
        return Shell.run(
            f"codex exec -m {MODEL} -c 'model_reasoning_effort={REASONING_EFFORT}' "
            f"-s workspace-write -c sandbox_workspace_write.network_access=true "
            f"-c approval_policy=never --color never - < {shlex.quote(PROMPT_FILE)}",
            timeout=ATTEMPT_TIMEOUT_SECONDS,
            env=_agent_env(loom_config, {"CODEX_HOME": codex_home, "GH_CONFIG_DIR": empty_gh_config}),
        )


def _outputs_problem():
    """Why the agent's output cannot be published, or "" when it can."""
    if not os.path.exists(SUMMARY_FILE):
        return f"agent did not write {SUMMARY_FILE}"
    if os.path.getsize(SUMMARY_FILE) == 0:
        return f"{SUMMARY_FILE} is empty"
    for name in ("comments.json", "thread_actions.json"):
        path = f"{OUTPUT_DIR}/{name}"
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    if not isinstance(json.load(f), list):
                        return f"{path} is not a JSON array"
            except ValueError as e:
                return f"{path} is not valid JSON: {e}"
    return ""


def _run_agent(run_once, agent_name, loom_config):
    """Run the agent until it produces publishable output. Raises otherwise."""
    started = time.time()
    last_error = None
    robots = ROBOT_NAMES.copy()
    random.shuffle(robots)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        if attempt > 1 and time.time() - started > NO_NEW_ATTEMPT_AFTER_SECONDS:
            print(f"Not starting attempt {attempt}: {int(time.time() - started)}s already spent")
            break
        _reset_output_dir()
        try:
            exit_code = run_once(loom_config, robots[(attempt - 1) % len(robots)])
            problem = _outputs_problem()
            if exit_code != 0 and problem:
                last_error = f"{agent_name} exited with code {exit_code}: {problem}"
            elif problem:
                last_error = problem
            else:
                if exit_code != 0:
                    # The outputs are complete (they are written last); a
                    # non-zero exit after that is a CLI shutdown issue.
                    print(f"WARNING: {agent_name} exited with code {exit_code} after writing complete output")
                return
        except Exception as e:  # noqa: BLE001 — broad catch: any exception is retryable here
            last_error = f"{type(e).__name__}: {e}"
            traceback.print_exc()
        print(f"WARNING: {agent_name} attempt {attempt}/{MAX_ATTEMPTS} failed: {last_error}")
        if attempt < MAX_ATTEMPTS:
            delay = min(2 ** attempt, 60)
            print(f"Retrying {agent_name} in {delay}s ...")
            time.sleep(delay)
    raise RuntimeError(f"{agent_name} review failed: {last_error}")


def _post_summary(summary, head_sha):
    """Post the summary as the updateable `review` comment. Raises on failure,
    failing the job."""
    body = summary.rstrip() + "\n\n" + review_context.REVIEWED_SHA_MARKER.format(sha=head_sha) + "\n"
    path = f"{WORK_DIR}/summary_to_post.md"
    with open(path, "w", encoding="utf-8") as f:
        f.write(body)
    Shell.check(
        f"{shlex.quote(sys.executable)} -m ci.praktika.gh post-or-update --tag {review_context.REVIEW_COMMENT_TAG} "
        f"--file {shlex.quote(path)}",
        strict=True,
    )


def review(run_once, agent_name):
    info = Info()
    if not info.pr_number:
        print("Not a PR, skipping")
        return []

    repo = _pr_repository(info)
    Shell.check(f"rm -rf {shlex.quote(WORK_DIR)}", verbose=False)
    os.makedirs(WORK_DIR, exist_ok=True)

    ctx = review_context.fetch(CONTEXT_DIR, repo, info.pr_number)

    loom_config = loom.Config.for_repo(repo, info.pr_number, _ssm)
    os.environ["LOOM_CALL_LOG"] = os.path.abspath(LOOM_CALL_LOG)
    brief = loom.write_brief(loom_config, ctx.pr, ctx.files, LOOM_DIR)
    print(f"Loom brief: {'written' if brief else 'not available'}")

    text = prompt.build(
        pr_url=info.pr_url,
        repo=repo,
        context_index=review_context.index_markdown(CONTEXT_DIR),
        incremental=os.path.exists(f"{CONTEXT_DIR}/since_last_review.md"),
        brief=brief,
        overlay=loom_config.pr_overlay,
        output_dir=OUTPUT_DIR,
    )
    with open(PROMPT_FILE, "w", encoding="utf-8") as f:
        f.write(text)

    _run_agent(run_once, agent_name, loom_config)

    # Re-read the threads: the author may have replied or resolved while the
    # agent ran, and thread actions are checked against the current state.
    try:
        threads = GH.list_pr_review_threads(pr=info.pr_number, repo=repo)
    except Exception as e:  # noqa: BLE001
        print(f"WARNING: failed to re-read review threads, using the snapshot: {e}")
        threads = ctx.threads

    with open(SUMMARY_FILE, "r", encoding="utf-8") as f:
        summary = f.read()
    summary = publish.publish(GH, repo, info.pr_number, ctx.head_sha, ctx.files, threads, OUTPUT_DIR, summary)
    _post_summary(summary, ctx.head_sha)

    return [p for p in (PROMPT_FILE, SUMMARY_FILE, LOOM_CALL_LOG) if os.path.exists(p)]


if __name__ == "__main__":
    if "--codex" in sys.argv:
        run_once, agent_name = _run_codex_once, "Codex"
    elif "--copilot" in sys.argv:
        run_once, agent_name = _run_copilot_once, "Copilot"
    else:
        print("Usage: copilot_review_job.py --codex | --copilot")
        sys.exit(1)

    status = Result.Status.OK
    info = ""
    files = []
    try:
        files = review(run_once, agent_name)
    except Exception as e:
        info = f"ERROR: {e}"
        print(info)
        traceback.print_exc()
        status = Result.Status.FAIL

    Result.create_from(status=status, info=info, files=files).complete_job()
