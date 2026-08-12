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
    max_est_usd: float
    price_in_per_1m: float
    price_out_per_1m: float
    est_output_tokens: int
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
        max_est_usd=_env_float("AI_E2E_MAX_EST_USD", 1.0),
        price_in_per_1m=_env_float("AI_E2E_PRICE_IN_PER_1M", 0.0),
        price_out_per_1m=_env_float("AI_E2E_PRICE_OUT_PER_1M", 0.0),
        est_output_tokens=_env_int("AI_E2E_EST_OUTPUT_TOKENS", 64),
        mock_delay_ms=_env_int("AI_E2E_MOCK_DELAY_MS", 200),
        kill_budget_sec=_env_int("AI_E2E_KILL_BUDGET_SEC", 10),
        latency_gate_real=_env_bool("AI_E2E_LATENCY_GATE_REAL"),
        write_baselines=_env_bool("AI_E2E_WRITE_BASELINES"),
        compare_to=_env_str("AI_E2E_COMPARE_TO"),
    )


def request_timeout_sec(cfg):
    return max(1, math.ceil(cfg.per_call_budget_ms / 1000))


@dataclasses.dataclass
class SpendEstimate:
    chat_calls: int
    input_tokens: int
    output_tokens: int
    usd: float
    priced: bool

    def format(self):
        cost = f"${self.usd:.4f}" if self.priced else "unpriced (AI_E2E_PRICE_* unset)"
        return (
            f"{self.chat_calls} chat calls, ~{self.input_tokens} input tokens, "
            f"~{self.output_tokens} output tokens -> {cost}"
        )


def estimate_spend(cfg, texts, chat_calls):
    """Estimate the cost of a run.

    Input tokens are approximated as len(text)/4. Output is `AI_E2E_EST_OUTPUT_TOKENS`
    per chat call rather than `max_tokens`, which overestimates these corpora by more
    than an order of magnitude and would abort runs costing cents. Unpriced is reported
    as unpriced, never as zero.
    """
    input_tokens = sum(max(1, len(text) // 4) for text in texts)
    output_tokens = chat_calls * cfg.est_output_tokens
    priced = cfg.price_in_per_1m > 0 or cfg.price_out_per_1m > 0
    usd = (
        input_tokens * cfg.price_in_per_1m + output_tokens * cfg.price_out_per_1m
    ) / 1_000_000
    return SpendEstimate(chat_calls, input_tokens, output_tokens, usd, priced)
