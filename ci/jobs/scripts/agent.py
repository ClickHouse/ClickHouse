"""
Shared helpers for jobs that drive a coding agent (Codex).

Used by the AI code review job and the AI fuzz triage step, so the robot
tokens, the model choice, and the auth sequences live in one place. The
agents run as CLIs today; helpers for calling them through an API belong here too.
"""

import os
import subprocess
import urllib.parse

from ci.praktika import Secret

# Robot gh tokens the AI fuzz triage files and edits issues with, picked at
# random so a single robot's rate limit or token issue does not fail every run.
ROBOT_NAMES = [
    "/ci/robot-ch-test-poll-copilot",
    "/ci/robot-ch-test-poll-1-copilot",
]

# OpenAI API key for the Codex CLI, written into `$CODEX_HOME/auth.json`
# via `codex login --with-api-key`.
OPENAI_KEY_SECRET = "/ci/llm/openai_api_key"

# One model for every agent consumer, so review and triage quality
# stay comparable across jobs.
MODEL = "gpt-6.1-sol"
# `gpt-6.1-sol` refuses some inputs with "This content was flagged for possible
# cybersecurity risk"; the remaining attempts then use `FALLBACK_MODEL`. When it is
# at capacity, only the last attempt does: a same-model retry usually recovers.
FALLBACK_MODEL = "gpt-5.4"
CYBER_RISK_FLAG = "flagged for possible cybersecurity risk"
MODEL_AT_CAPACITY = "Selected model is at capacity"


def refused_as_cyber_risk(model, output):
    """Whether `model` refused the input in `output`, so the next attempt should use `FALLBACK_MODEL`."""
    return model != FALLBACK_MODEL and CYBER_RISK_FLAG in output


def at_capacity(model, output):
    """Whether `model` was at capacity in `output`, so the last attempt may use `FALLBACK_MODEL`."""
    return model != FALLBACK_MODEL and MODEL_AT_CAPACITY in output


def repo_from_pr_url(pr_url):
    """`owner/repo` parsed from a PR URL, or "" when it is not a PR URL."""
    path_parts = urllib.parse.urlparse(pr_url).path.strip("/").split("/")
    if len(path_parts) >= 4 and path_parts[2] == "pull":
        return f"{path_parts[0]}/{path_parts[1]}"
    return ""


def robot_token(robot_name):
    return Secret.Config(
        name=robot_name, type=Secret.Type.AWS_SSM_PARAMETER, region="us-east-1"
    ).get_value()


def openai_api_key():
    return Secret.Config(
        name=OPENAI_KEY_SECRET, type=Secret.Type.AWS_SSM_PARAMETER, region="us-east-1"
    ).get_value()


def codex_login(codex_home):
    """Write the OpenAI key into `$CODEX_HOME/auth.json` via `codex login`.

    Codex does NOT consult `OPENAI_API_KEY` directly when invoked — the key
    must be installed with `codex login --with-api-key`, which reads it from
    stdin. `codex_home` should be a per-attempt temporary directory so the
    key never lands on global runner state.
    """
    subprocess.run(
        ["codex", "login", "--with-api-key"],
        input=openai_api_key(),
        text=True,
        check=True,
        env={**os.environ, "CODEX_HOME": codex_home},
    )
