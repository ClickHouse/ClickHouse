"""Environment-driven configuration for the AI-function end-to-end suite.

Every knob is an environment variable; nothing is stored in the repo. The resolved
`EndpointConfig` is exposed as the session-scoped `cfg` fixture.

Configuration is also the gate: the live modules skip when the resolved target has no
usable credentials, so an unconfigured run reports SKIPPED with a readable reason instead
of an empty collection. See README.md sections 2 and 5.
"""

import dataclasses
import math
import os

# `local` endpoints point at a model server running on the docker host, not in the
# container. The gateway address is only known once the cluster is up, so the target
# carries a placeholder that `EndpointConfig.with_host_gateway` substitutes.
HOST_GATEWAY_PLACEHOLDER = "{host_gateway}"

INTERNAL_BASE = "https://inference-internal.clickhouse.cloud"
LOCAL_BASE = f"http://{HOST_GATEWAY_PLACEHOLDER}:11434"


@dataclasses.dataclass(frozen=True)
class Target:
    """A place to send AI requests, plus what it is capable of.

    `toy_model` marks a stand-in model that cannot be held to a non-trivial instruction
    and whose embedder ignores `dimensions`; the cases listed in README.md section 5 skip
    on such a target. It defaults to False so a target added without considering the flag
    gets full strictness.

    `reports_token_usage` is not about the model: some providers (HuggingFace TEI) omit
    the `usage` object, which leaves the AI token ProfileEvents at zero.
    """

    name: str
    chat_endpoint: str
    embed_endpoint: str
    chat_model: str
    embed_model: str
    embed_dim_model: str
    toy_model: bool = False
    reports_token_usage: bool = True
    # `local` needs no credentials; `internal` is unusable without a key.
    requires_api_key: bool = True


TARGETS = {
    "internal": Target(
        name="internal",
        chat_endpoint=f"{INTERNAL_BASE}/v1/chat/completions",
        embed_endpoint=f"{INTERNAL_BASE}/v1/embeddings",
        chat_model="claude-haiku-4-5",
        embed_model="qwen3-embedding-8b",
        embed_dim_model="text-embedding-3-small",
        toy_model=False,
        reports_token_usage=True,
        requires_api_key=True,
    ),
    "local": Target(
        name="local",
        chat_endpoint=f"{LOCAL_BASE}/v1/chat/completions",
        embed_endpoint=f"{LOCAL_BASE}/v1/embeddings",
        chat_model="qwen2.5:0.5b",
        embed_model="all-minilm",
        embed_dim_model="all-minilm",
        toy_model=True,
        reports_token_usage=True,
        requires_api_key=False,
    ),
}


def _env_str(name, default=""):
    value = os.environ.get(name)
    return default if value is None else value


def _env_int(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return int(value)


def _env_float(name, default):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return float(value)


def _env_bool(name, default=False):
    value = os.environ.get(name)
    if value is None or value == "":
        return default
    return value.strip().lower() in ("1", "true", "yes", "on")


@dataclasses.dataclass(frozen=True)
class EndpointConfig:
    target: Target
    api_key: str
    chat_model: str
    chat_model_alt: str
    embed_model: str
    embed_dim_model: str
    chat_endpoint: str
    embed_endpoint: str
    data_scale: int
    per_call_budget_ms: int
    embed_batch_budget_ms: int
    embed_batch_size: int
    max_api_calls: int
    max_tokens: int
    price_in_per_1m: float
    price_out_per_1m: float
    mock_delay_ms: int
    kill_budget_sec: int
    latency_gate_real: bool
    write_baselines: bool
    compare_to: str

    @property
    def toy_model(self):
        return self.target.toy_model

    @property
    def reports_token_usage(self):
        return self.target.reports_token_usage

    @property
    def live_configured(self):
        """Whether the live (paid) modules can run at all."""
        if self.target.requires_api_key and not self.api_key:
            return False
        return bool(self.chat_endpoint) and bool(self.embed_endpoint)

    @property
    def live_skip_reason(self):
        if self.live_configured:
            return ""
        if self.target.requires_api_key and not self.api_key:
            return (
                f"target '{self.target.name}' needs AI_E2E_API_KEY "
                f"(or set AI_E2E_TARGET=local to use a local model server)"
            )
        return "AI_E2E_CHAT_ENDPOINT / AI_E2E_EMBED_ENDPOINT are empty"

    @property
    def needs_host_gateway(self):
        return HOST_GATEWAY_PLACEHOLDER in self.chat_endpoint + self.embed_endpoint

    def with_host_gateway(self, address):
        """Substitute the docker host address into endpoints that need it."""
        return dataclasses.replace(
            self,
            chat_endpoint=self.chat_endpoint.replace(HOST_GATEWAY_PLACEHOLDER, address),
            embed_endpoint=self.embed_endpoint.replace(
                HOST_GATEWAY_PLACEHOLDER, address
            ),
        )

    @property
    def insecure_endpoint_needed(self):
        """`http://` to a non-loopback host is refused unless the setting is enabled.

        A model server on the docker host is reached by the bridge gateway address, which
        is not loopback, so the setting is required there. The in-container mock is on
        loopback and is exempt (`FunctionBaseAI.cpp`, `isLoopbackHost`).
        """
        for endpoint in (self.chat_endpoint, self.embed_endpoint):
            if endpoint.startswith("http://") and not _is_loopback_url(endpoint):
                return True
        return False


def _is_loopback_url(url):
    host = url.split("//", 1)[-1].split("/", 1)[0].split(":")[0]
    return host == "localhost" or host.startswith("127.")


def resolve():
    """Build the `EndpointConfig` from the environment."""
    target_name = _env_str("AI_E2E_TARGET", "internal")
    if target_name not in TARGETS:
        raise ValueError(
            f"AI_E2E_TARGET='{target_name}' is not one of {sorted(TARGETS)}"
        )
    target = TARGETS[target_name]

    return EndpointConfig(
        target=target,
        api_key=_env_str("AI_E2E_API_KEY"),
        chat_model=_env_str("AI_E2E_CHAT_MODEL", target.chat_model),
        chat_model_alt=_env_str("AI_E2E_CHAT_MODEL_ALT"),
        embed_model=_env_str("AI_E2E_EMBED_MODEL", target.embed_model),
        embed_dim_model=_env_str("AI_E2E_EMBED_DIM_MODEL", target.embed_dim_model),
        chat_endpoint=_env_str("AI_E2E_CHAT_ENDPOINT", target.chat_endpoint),
        embed_endpoint=_env_str("AI_E2E_EMBED_ENDPOINT", target.embed_endpoint),
        data_scale=max(1, _env_int("AI_E2E_DATA_SCALE", 1)),
        per_call_budget_ms=_env_int("AI_E2E_PER_CALL_BUDGET_MS", 15000),
        embed_batch_budget_ms=_env_int("AI_E2E_EMBED_BATCH_BUDGET_MS", 10000),
        # Mirrors the `ai_function_embedding_max_batch_size` default.
        embed_batch_size=_env_int("AI_E2E_EMBED_BATCH_SIZE", 100),
        # Hard ceilings on what the session may spend, in units the run actually
        # measures. Both are enforced from `system.query_log` after every query, so they
        # bound a retry storm too - unlike a pre-run estimate, which can only guess.
        max_api_calls=_env_int("AI_E2E_MAX_API_CALLS", 2000),
        max_tokens=_env_int("AI_E2E_MAX_TOKENS", 2_000_000),
        # Pricing is optional and only decorates the report; it gates nothing.
        price_in_per_1m=_env_float("AI_E2E_PRICE_IN_PER_1M", 0.0),
        price_out_per_1m=_env_float("AI_E2E_PRICE_OUT_PER_1M", 0.0),
        mock_delay_ms=_env_int("AI_E2E_MOCK_DELAY_MS", 200),
        kill_budget_sec=_env_int("AI_E2E_KILL_BUDGET_SEC", 10),
        latency_gate_real=_env_bool("AI_E2E_LATENCY_GATE_REAL"),
        write_baselines=_env_bool("AI_E2E_WRITE_BASELINES"),
        compare_to=_env_str("AI_E2E_COMPARE_TO"),
    )


def request_timeout_sec(cfg):
    return max(1, math.ceil(cfg.per_call_budget_ms / 1000))


class BudgetExceeded(RuntimeError):
    pass


class Budget:
    """Cumulative meter over a session, checked after every query.

    A pre-run estimate cannot bound a run: it has to guess the number of calls, it prices
    each corpus string once even when several suites send it, and it is blind to retries.
    Counting what `system.query_log` reports after each query needs no pricing table, and
    stops a runaway inside the run rather than after it.
    """

    def __init__(self, max_api_calls, max_tokens):
        self.max_api_calls = max_api_calls
        self.max_tokens = max_tokens
        self.api_calls = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.queries = 0

    @property
    def tokens(self):
        return self.input_tokens + self.output_tokens

    def record(self, events):
        """Add one query's usage, and stop the session if it went over."""
        self.queries += 1
        self.api_calls += int(events.get("api_calls") or 0)
        self.input_tokens += int(events.get("input_tokens") or 0)
        self.output_tokens += int(events.get("output_tokens") or 0)
        if self.max_api_calls and self.api_calls > self.max_api_calls:
            raise BudgetExceeded(
                f"{self.api_calls} API calls over {self.queries} queries exceeds "
                f"AI_E2E_MAX_API_CALLS={self.max_api_calls}. Raise the ceiling or lower "
                "AI_E2E_DATA_SCALE."
            )
        if self.max_tokens and self.tokens > self.max_tokens:
            raise BudgetExceeded(
                f"{self.tokens} tokens over {self.queries} queries exceeds "
                f"AI_E2E_MAX_TOKENS={self.max_tokens}."
            )

    def usd(self, price_in_per_1m, price_out_per_1m):
        """Cost, when rates are configured. `None` means unpriced, never zero."""
        if not price_in_per_1m and not price_out_per_1m:
            return None
        return (
            self.input_tokens * price_in_per_1m + self.output_tokens * price_out_per_1m
        ) / 1_000_000

    def summary(self, price_in_per_1m=0.0, price_out_per_1m=0.0):
        cost = self.usd(price_in_per_1m, price_out_per_1m)
        priced = f"${cost:.4f}" if cost is not None else "unpriced (AI_E2E_PRICE_* unset)"
        return (
            f"{self.queries} queries, {self.api_calls}/{self.max_api_calls} API calls, "
            f"{self.tokens}/{self.max_tokens} tokens -> {priced}"
        )
