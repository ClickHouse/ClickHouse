"""Redaction of secrets in the diagnostics the integration tests log.

Everything the tests print or log ends up in the collected pytest logs, which
are CI artifacts, so the values of the environment variables that hold
credentials (e.g. `CLICKHOUSE_CI_LOGS_PASSWORD`, see helpers/ci_logs_export.py)
are masked before an environment is dumped.
"""

import copy
import re

SECRET_ENV_NAME = re.compile(r"PASSWORD|SECRET|TOKEN|ACCESS_KEY", re.IGNORECASE)
MASK = "[MASKED]"


def is_secret_env_name(name):
    return SECRET_ENV_NAME.search(name) is not None


def redact_env(env):
    """A copy of an environment mapping with the values of secrets masked."""
    return {
        name: MASK if is_secret_env_name(name) else value for name, value in env.items()
    }


def _redact_env_list(env):
    """The same for a docker `Env` list of `NAME=value` strings."""
    redacted = []
    for item in env:
        name, sep, _ = item.partition("=")
        redacted.append(f"{name}={MASK}" if sep and is_secret_env_name(name) else item)
    return redacted


def redact_docker_inspect(info):
    """A copy of the result of docker `inspect_container` or `inspect_image`
    with the values of the secrets in its `Config.Env` (and `ContainerConfig.Env`
    of an image) masked."""
    info = copy.deepcopy(info)
    for key in ("Config", "ContainerConfig"):
        config = info.get(key) if isinstance(info, dict) else None
        if isinstance(config, dict) and isinstance(config.get("Env"), list):
            config["Env"] = _redact_env_list(config["Env"])
    return info
