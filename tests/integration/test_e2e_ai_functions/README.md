# AI functions — end-to-end suite

Manually-run tests for the AI functions against a **real** OpenAI-compatible endpoint, plus an
architecture-latency suite driven by a delay-injecting mock. Nothing here runs in CI: the live half
spends money, and the latency half asserts wall-clock and wants an unshared host.

Anything checkable for free and deterministically lives in `tests/integration/test_ai_functions/`
instead, where CI runs it on every pull request — the per-query-shape API call counts, embedding batch
counts, timeout and retry attempt counts, and the quota-scope check.

## Layout

```
conftest.py              cluster, config-defined named collections, preflight, key redaction
config.py                env -> EndpointConfig, target capabilities, the spend meter
corpus.py                deterministic corpora, one per workload
asserts.py               shared assertions, ProfileEvents reader, report writer
latency_mock_server.py   threaded mock: injected delay, in-flight gauge, connection counting
test_basic_e2e.py        every function end to end, within a time budget
test_params.py           does the endpoint honor dimensions, max_tokens, system_prompt, ...
test_concurrency.py      isolation and liveness under concurrent queries
test_latency_arch.py     architecture latency against the mock; connection reuse
test_latency_real.py     real-endpoint p50/p95 and batch throughput, reported
configs/                 named collections, values read from the environment
```

## Configuration

Everything goes through `ci/local.env` (gitignored). praktika runs the job inside a container and
passes only that file, so an exported shell variable never reaches pytest; `--param` both echoes its
value into the job log and cannot carry a value containing a space.

```
# ci/local.env
PYTEST_ADDOPTS=-m e2e -o timeout=3600
AI_E2E_TARGET=internal
AI_E2E_API_KEY=…
```

`PYTEST_ADDOPTS` applies to **every** integration run from the checkout, so comment it out when
running anything else — otherwise a normal run collects zero tests and reports success.

| Variable | Default | Meaning |
|---|---|---|
| `AI_E2E_TARGET` | `internal` | `internal` (the inference gateway) or `local` (an OpenAI-compatible model server) |
| `AI_E2E_API_KEY` | unset | Live modules skip with a readable reason when it is missing |
| `AI_E2E_CHAT_ENDPOINT`, `AI_E2E_EMBED_ENDPOINT` | per target | Full URLs |
| `AI_E2E_CHAT_MODEL`, `AI_E2E_EMBED_MODEL`, `AI_E2E_EMBED_DIM_MODEL` | per target | Models. `AI_E2E_CHAT_MODEL_ALT` enables the model-override case |
| `AI_E2E_DATA_SCALE` | `1` | Multiplier on the scaling corpora |
| `AI_E2E_MAX_API_CALLS` | `2000` | Hard ceiling, counted from `system.query_log` after every query; the session stops when passed |
| `AI_E2E_MAX_TOKENS` | `2000000` | Same, for input + output tokens. `0` disables either ceiling |
| `AI_E2E_PRICE_IN_PER_1M`, `AI_E2E_PRICE_OUT_PER_1M` | `0` | Optional; decorates the end-of-run spend line, gates nothing |
| `AI_E2E_PER_CALL_BUDGET_MS` | `15000` | Per-call time budget; also sets `ai_function_request_timeout_sec` |
| `AI_E2E_MOCK_DELAY_MS` | `200` | Delay the latency mock injects |
| `AI_E2E_COMPARE_TO` | unset | A previous `tmp/ai_e2e_latency_arch.json`, for a before/after timing table |
| `AI_E2E_LATENCY_GATE_REAL` | `0` | Makes the real-endpoint suite assert against `AI_E2E_COMPARE_TO` instead of only reporting |

## Running

```bash
# architecture latency: free, no endpoint, no key, ~4 minutes, wants an unshared host
python -m ci.praktika run "integration" \
    --test "test_e2e_ai_functions/test_latency_arch.py" --path ../ch-build/programs/clickhouse

# the live half (spends money)
python -m ci.praktika run "integration" --path ../ch-build/programs/clickhouse \
    --test "test_e2e_ai_functions/test_basic_e2e.py" "test_e2e_ai_functions/test_params.py" \
           "test_e2e_ai_functions/test_concurrency.py"
```

`--path` is mandatory: the job otherwise defaults to `ci/tmp/clickhouse`, usually a stale binary from
an unrelated build. Name files rather than the directory — a directory selector can put the same path
on pytest's command line once per file in it. At larger `AI_E2E_DATA_SCALE`, raise `--session-timeout`
as well as the per-test `-o timeout`.

For a before/after timing comparison, keep `tmp/ai_e2e_latency_arch.json` from the first run and point
`AI_E2E_COMPARE_TO` at it for the second. Wall-clock and CPU numbers are never committed: two runs on
the same host minutes apart measured 161.2 and 75.4 µs per row, so only a same-host, same-session diff
means anything.

## Guards

**Spend** is metered, not estimated: every query's `AIAPICalls` and token counts are read back from
`system.query_log` and accumulated, and the session stops the moment a ceiling is passed. That bounds
a retry storm, which a pre-run estimate cannot.

**Secrets.** The named collections are defined in `configs/ai_e2e_collections.xml` using `from_env`
with `hide_in_preprocessed`, so the key is absent from the config on disk, from the preprocessed copy,
from `database/named_collections/`, and from query text. `conftest.py` installs a logging filter
*before* the cluster is constructed — `ClickHouseCluster.__init__` dumps the whole environment at
DEBUG — and scrubs the `.env` files the harness writes. Treat a failed run's `logs.tar.gz` as
sensitive regardless.

**Time.** Cases assert on `query_duration_ms` from `system.query_log` rather than client wall-clock,
and `ai_function_request_timeout_sec` bounds a stalled socket read.

## Notes for whoever picks this up

- `test_latency_arch.py` carries two recorded constants, `BASELINE_MAX_IN_FLIGHT_8T` and
  `BASELINE_EFFECTIVE_CONCURRENCY_8T`, measured on master @ `529df9d151c`. They are a stream-count and
  a dimensionless ratio, which is why they can be constants at all; update them together with the
  measurement that produced them.
- `test_b2_6_kill_query_latency` is an expected failure: the AI row loop has no cancellation
  checkpoint, so `KILL QUERY` cannot interrupt it. It flips to a pass by itself when that is fixed.
- The `local` target is a smoke path, not equivalent coverage — cases needing a capable model skip on
  it.
- Suite A deliberately does not re-test `NULL`/empty handling, quota skip and throw, or
  `ai_function_throw_on_error`: the mock suite pins them for free, and no endpoint can influence them.
