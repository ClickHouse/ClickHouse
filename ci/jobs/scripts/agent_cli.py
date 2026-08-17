"""
Shared helpers for jobs that drive coding-agent CLIs (Codex, Copilot).

Used by the AI code review job and the AI fuzz triage step, so the robot
rotation, the model choice, and the auth sequences live in one place.
"""

import os
import subprocess
import urllib.parse

from ci.praktika import Secret

# Robot gh tokens. Used by both backends — Copilot authenticates against
# GitHub with one of these directly; Codex needs gh authed so the agent's
# shelled-out `gh` calls succeed. Each attempt picks one in a randomised
# rotation so a single robot's rate limit or token issue does not fail
# every attempt.
ROBOT_NAMES = [
    "/ci/robot-ch-test-poll-copilot",
    "/ci/robot-ch-test-poll-1-copilot",
]

# OpenAI API key for the Codex CLI, written into `$CODEX_HOME/auth.json`
# via `codex login --with-api-key`.
OPENAI_KEY_SECRET = "/ci/llm/openai_api_key"

# One model for every agent-CLI consumer, so review and triage quality
# stay comparable across backends and jobs.
AGENT_MODEL = "gpt-5.4"


def repo_from_pr_url(pr_url):
    """`owner/repo` parsed from a PR URL, or "" when it is not a PR URL."""
    path_parts = urllib.parse.urlparse(pr_url).path.strip("/").split("/")
    if len(path_parts) >= 4 and path_parts[2] == "pull":
        return f"{path_parts[0]}/{path_parts[1]}"
    return ""


def gh_auth_with_robot_token(gh_config_dir, robot_name):
    """Authenticate gh CLI in a scoped GH_CONFIG_DIR using the given robot token."""
    print(f"Using robot: {robot_name}")
    token = Secret.Config(
        name=robot_name, type=Secret.Type.AWS_SSM_PARAMETER
    ).get_value()
    subprocess.run(
        ["gh", "auth", "login", "--with-token"],
        input=token,
        text=True,
        check=True,
        env={**os.environ, "GH_CONFIG_DIR": gh_config_dir},
    )


def codex_login(codex_home):
    """Write the OpenAI key into `$CODEX_HOME/auth.json` via `codex login`.

    Codex does NOT consult `OPENAI_API_KEY` directly when invoked — the key
    must be installed with `codex login --with-api-key`, which reads it from
    stdin. `codex_home` should be a per-attempt temporary directory so the
    key never lands on global runner state.
    """
    openai_key = Secret.Config(
        name=OPENAI_KEY_SECRET, type=Secret.Type.AWS_SSM_PARAMETER
    ).get_value()
    subprocess.run(
        ["codex", "login", "--with-api-key"],
        input=openai_key,
        text=True,
        check=True,
        env={**os.environ, "CODEX_HOME": codex_home},
    )
