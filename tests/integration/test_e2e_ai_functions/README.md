# AI functions — end-to-end test suite (design)

Design for a manually-run suite that exercises the ClickHouse AI functions against a real
OpenAI-compatible endpoint, plus a latency suite that measures the *implementation's* architecture
rather than the endpoint's speed. Nothing here runs in CI.

Documents:

| Doc | Contents |
|---|---|
| `README.md` (this file) | Scope, decisions, layout, configuration, how to run, spend/time guards, implementation order |
| [`01-correctness.md`](01-correctness.md) | Suite A: end-to-end per function, parameter/setting honoring, concurrency |
| [`02-latency.md`](02-latency.md) | Suite B: latency and architecture metrics, controlled-latency mock endpoint, lazy-evaluation baselines |

## 1. Scope

Covered: `aiGenerate`, `aiClassify`, `aiExtract`, `aiTranslate`, `aiEmbed`, `aiSimilarity` against an
OpenAI-compatible endpoint (`provider = 'openai'`). Anthropic is out of scope for the first iteration;
§8 lists what has to change to add it.

Not covered here, because it is already covered:

| Layer | Location | What it covers |
|---|---|---|
| SQL | `tests/queries/0_stateless/03300_ai_functions.sql`, `04142`, `04492`, `04614`, `04628` | Argument validation, return types, named-collection resolution, error codes, settings defaults. No HTTP. |
| Mock HTTP | `tests/integration/test_ai_functions/` | Row loop, retries, quotas, `NULL`/empty handling, malformed-response rejection, request headers and body, and the **API-call-count invariants** for each query shape (filter, `LIMIT`, `PREWHERE`, short-circuit, dedup, CSE) plus the per-query quota scope. Single-threaded mock, no real model, runs in CI. |

This suite adds only what those two cannot answer:

1. **Does every function work end to end against a real model, within a bounded time?** (Suite A1)
2. **Does the endpoint honor the parameters and settings we send?** (Suite A2)
3. **Is concurrent use correct — no mixed, dropped, or cross-attributed results?** (Suite A3)
4. **Does a code change improve or regress the architecture's latency?** (Suite B)

Anything a mock can decide is left to the mock suite. In particular `NULL`/empty-operand handling,
quota skip and throw behavior, `ai_function_throw_on_error`, and embedding batch counts are *not*
re-tested here: no endpoint can influence them, and paying a provider to re-confirm them is waste.

## 2. Decisions

| Decision | Choice |
|---|---|
| Location | `tests/integration/test_e2e_ai_functions/`, matching the existing `test_e2e_catalogs`, `test_e2e_iceberg_engine`, `test_e2e_iceberg_tf` suites |
| Harness | Python + pytest |
| CI exclusion | `pytestmark = pytest.mark.e2e` **in every test module**, deselected by `pytest.ini`'s existing `addopts = -m 'not long_run and not e2e'`. No change to shared config |
| Latency method | Controlled-latency local mock is the gating signal; the real endpoint is a secondary, reported measurement |
| Assertions | Deterministic where the implementation decides the outcome; explicitly marked report-only where the model or the endpoint decides it. No LLM judge, no accuracy thresholds |
| Cost | The suite may spend money. A pre-run token estimate and a hard `AI_E2E_MAX_EST_USD` cap gate the run |
| Data size | `AI_E2E_DATA_SCALE` multiplier on the loop corpora (default `1` = smallest useful run) |
| Targets | `internal` (ClickHouse inference gateway) and `local` (OpenAI-compatible local model, for testing the test), distinguished by two flags: `toy_model` and `reports_token_usage` |

One consequence of configuring through `ci/local.env`: `PYTEST_ADDOPTS=-m e2e` applies to **every**
integration run from that checkout, so an ordinary `test_ai_functions` run would collect zero tests and
report success. Keep that line commented out except when running this suite.

**The marker keeps the suite out of CI.** `pytest.ini` already deselects `e2e`, so no shared config
changes. `pytestmark` must be in each `test_*.py` — pytest ignores `pytestmark` in `conftest.py`, and
`ci/jobs/integration_test_job.py:430` collects every `test_*/test*.py`, so a conftest-only marker
would let CI run the whole suite against the real endpoint with no key. Nothing in CI passes
`-m e2e`, which is exactly what `test_e2e_catalogs` and the two iceberg suites already rely on.

**Missing configuration is a skip, not a failure.** The live modules carry a second module-level mark:

```python
pytestmark = [pytest.mark.e2e, requires_live_endpoint]   # test_basic_e2e, test_params, test_concurrency
pytestmark = pytest.mark.e2e                             # test_latency_arch — mock only, no config needed
```

where `requires_live_endpoint` is a `skipif` whose reason names the missing variable, the same pattern
as `helpers/catalog_manager_iceberg_s3.py`'s "Missing Iceberg S3 e2e settings: …" and
`test_e2e_iceberg_tf.py`'s `_skip_by_backend` fixture. There is no separate enable flag: the
configuration *is* the gate. An unconfigured run reports readable SKIPPED entries instead of an empty
collection, and `test_latency_arch.py` needs no configuration at all, so the free architecture suite
runs with nothing but `-m e2e`.

Sharing the `e2e` marker with the three existing suites means a blanket `-m e2e` run includes this
one. That is fine: it *is* an e2e suite, and without an endpoint configured the live modules skip with
a clear reason. Selection is by path in any case — naming one file with `--test` runs one file, and
`-m` only re-enables what the path selector already picked.

## 3. Implementation facts the design depends on

Verified in the current tree. Re-check when the implementation changes — several assertions exist
precisely to detect these changing.

| Fact | Location |
|---|---|
| Chat functions issue one HTTP request per row, strictly serial within a block; no batching, no concurrency knob | `src/Functions/FunctionBaseAI.cpp:517` |
| Embedding functions batch up to `ai_function_embedding_max_batch_size` (default 100) inputs per request; batches are issued serially | `src/Functions/FunctionBaseAI.cpp:392` |
| The five AI `ProfileEvents` are incremented **after** the row loop, so a query that throws records none of them | `src/Functions/FunctionBaseAI.cpp:591` |
| `aiEmbed` and `aiSimilarity` declare `isDeterministic() = true`; the four text functions inherit `false` | `src/Functions/aiEmbed.cpp:87`, `src/Functions/aiSimilarity.cpp:113`, `src/Functions/FunctionBaseAI.h:58` |
| Identical texts are not deduplicated before embedding | `src/Functions/aiEmbed.cpp:164-171` |
| The per-row loop has no cancellation or timeout checkpoint | `src/Functions/FunctionBaseAI.cpp:517` |
| A non-`https` scheme is rejected for non-loopback hosts unless `ai_function_allow_insecure_endpoint = 1`; `RemoteHostFilter::checkURL` runs first | `src/Functions/FunctionBaseAI.cpp:279-290` |
| The quota tracker is constructed per `executeImpl` call, so per block and per stream - **not** per query, despite the `..._per_query` setting names. Measured: a limit of 10 on an 8-part table produced 30 calls | `src/Functions/FunctionBaseAI.cpp:499`, `aiEmbed.cpp:135`, `aiSimilarity.cpp:162` |
| Retries are per request; `ai_function_max_retries` defaults to 0 | `src/Functions/FunctionBaseAI.cpp:537` |
| `ai_function_request_timeout_sec` sets `receive_timeout` only — a per-socket-read timeout, not a bound on total request time | `src/Functions/FunctionBaseAI.cpp:506` |
| Requests go through the server-wide keep-alive pool (`makeHTTPSession(HTTPConnectionGroupType::HTTP, …)`), so connections survive across queries | `src/Functions/AI/OpenAIProvider.cpp:60,145` |
| `ProfileEvents`: `AIAPICalls`, `AIInputTokens`, `AIOutputTokens`, `AIRowsProcessed`, `AIRowsSkipped` | `src/Common/ProfileEvents.cpp:1562-1566` |
| The existing mock server is a single-threaded `http.server.HTTPServer`: it serializes requests and cannot measure concurrency | `tests/integration/test_ai_functions/mock_ai_server.py` |

The quota scope is the one fact here with a user-visible consequence rather than a
testing consequence. `ai_function_max_api_calls_per_query` is documented as "the maximum number of
HTTP requests that AI functions may dispatch per query" and is the *only* bound available for
providers that omit token usage, so it is the last line of defence against a runaway query's spend.
Because the tracker is a stack local with no shared state, every block and every stream starts with a
fresh allowance, and the effective ceiling scales with the data instead of bounding it.
`test_structural.py::test_api_call_quota_scope` measures this and is marked `xfail`: it flips to a
pass by itself if the tracker is ever hoisted to query scope.

The determinism asymmetry matters more than it looks: common-subexpression elimination, constant
folding, and any future memoization are available to the embedding functions and structurally
unavailable to the chat functions. Suite B's laziness scenarios are split accordingly.

Signatures and parameters:

| Function | Signature | Own params (map keys) | Return |
|---|---|---|---|
| `aiGenerate` | `(text[, params])` | `temperature` (0.7), `system_prompt` | `String`, `Nullable(String)` if `text` is nullable |
| `aiClassify` | `(text, categories[, params])` | `temperature` (0.0) | same |
| `aiExtract` | `(text, instruction_or_schema[, params])` | `temperature` (0.0) | same, JSON payload |
| `aiTranslate` | `(text, target_language[, params])` | `temperature` (0.3), `instructions` | same |
| `aiEmbed` | `(text, model[, params])` | `dimensions` (0 = native) | `Array(Float32)` |
| `aiSimilarity` | `(text1, text2, model[, params])` | `dimensions` (0 = native) | `Nullable(Float32)` |

The four text functions also accept `credentials`, `model`, `max_tokens` (1024). The embedding
functions accept `credentials` and `dimensions` only; `model` is a required positional argument and is
rejected in the named collection. `params` is a const `Map(String, String)`: **every** value must be a
string literal, including numbers (`map('dimensions', '256')`, not `map('dimensions', 256)`, which has
no common value supertype and fails analysis).

## 4. Layout

```
tests/integration/test_e2e_ai_functions/
    __init__.py
    README.md                  # this design; becomes the run doc on implementation
    01-correctness.md
    02-latency.md
    conftest.py                # cluster, named collections, preflight, corpus, requires_live_endpoint
    config.py                  # env -> EndpointConfig, target capabilities, spend estimate and cap
    corpus.py                  # deterministic corpora, one per workload
    asserts.py                 # shared assertions, ProfileEvents reader, mock /stats reader
    latency_mock_server.py     # threaded, delay-injecting, concurrency-instrumented mock
    test_basic_e2e.py          # Suite A1
    test_params.py             # Suite A2
    test_concurrency.py        # Suite A3
    test_structural.py         # Suite B: exact-integer cases at D=0 that need this suite's mock
                               # (connection counting, injected delay). The call-count and
                               # laziness invariants live in test_ai_functions/ instead, so CI
                               # validates them on every PR
    test_latency_arch.py       # Suite B: timing cases (B1 matrix, laziness time pass, cancellation)
    test_latency_real.py       # Suite B3 (real endpoint; reporting)
    baselines/
        laziness.json          # committed: AI call counts per query shape, gated by test_structural.py
        arch.json              # committed: integers and dimensionless ratios only. Host-dependent
                               # timings are NOT here - they land in tmp/ai_e2e_latency_arch.json and
                               # are diffed run-local via AI_E2E_COMPARE_TO (02-latency.md section 8)
```

One concern per file: the harness runs xdist with `--dist=loadfile`
(`ci/jobs/integration_test_job.py:1082`), so tests in one file land on one worker and run
sequentially, while *different* files in this directory run concurrently on different workers. Three
consequences:

- Every timing-sensitive test lives in `test_latency_arch.py` / `test_latency_real.py`, and nothing
  else shares those files.
- **`test_structural.py` is deliberately separate from `test_latency_arch.py`.** Its cases run the mock
  at `delay_ms = 0` and assert only exact integers — `AIAPICalls` per query shape, batch counts — so
  they take seconds, tolerate a shared host, and can run in parallel with anything. That is what makes
  them cheap enough to run on every AI-touching PR while the timing matrix runs on a schedule (§9).
- Register only the timing files as sequential in `ci/jobs/scripts/integration_tests_configs.py`, so a
  local full-suite run does not put another cluster on the host while latency is being measured:

  ```python
  TC("test_e2e_ai_functions/test_latency_arch.py", True, "timing-sensitive; must not share the host"),
  TC("test_e2e_ai_functions/test_latency_real.py", True, "timing-sensitive; must not share the host"),
  ```

  Prefixes are matched with `startswith` against paths like `test_e2e_ai_functions/test_params.py`
  (`integration_tests_configs.py:585-590`), so file-level entries work — `LLVM_COVERAGE_SKIP_PREFIXES`
  already uses one (`test_storage_s3_queue/test_6.py`). Naming the files rather than the directory keeps
  `test_structural.py` and Suite A in the parallel phase. The `TEST_CONFIGS` sanity check requires each
  prefix to match a collected test file, so add these entries together with the files, not before.

Preflight (§A0 of `01-correctness.md`) is a **session-scoped autouse fixture in `conftest.py`**, not a
test module: module order is duration-derived under both schedules, so no test file can be guaranteed
to run first.

## 5. Configuration

Environment variables, resolved in `config.py`. No secret is stored in the repo.

| Variable | Default | Meaning |
|---|---|---|
| `AI_E2E_TARGET` | `internal` | `internal` \| `local` |
| `AI_E2E_CHAT_ENDPOINT` | per target | Full chat-completions URL |
| `AI_E2E_EMBED_ENDPOINT` | per target | Full embeddings URL |
| `AI_E2E_API_KEY` | unset | Bearer key; empty is valid for a local model |
| `AI_E2E_CHAT_MODEL` | per target | Model for the four text functions |
| `AI_E2E_CHAT_MODEL_ALT` | unset | Second chat model for the `model`-override precedence case (A2-6); that case skips when unset |
| `AI_E2E_EMBED_MODEL` | per target | Embedding model, native dimension |
| `AI_E2E_EMBED_DIM_MODEL` | per target | Embedding model that honors `dimensions` |
| `AI_E2E_DATA_SCALE` | `1` | Multiplier on the loop corpora |
| `AI_E2E_MAX_EST_USD` | `1.0` | The session aborts before spending if the pre-run estimate exceeds this |
| `AI_E2E_PRICE_IN_PER_1M`, `AI_E2E_PRICE_OUT_PER_1M` | `0` | USD per 1M tokens, used only for the estimate |
| `AI_E2E_EST_OUTPUT_TOKENS` | `64` | Output tokens assumed per chat call in the estimate |
| `AI_E2E_PER_CALL_BUDGET_MS` | `15000` | Per-chat-call time budget |
| `AI_E2E_EMBED_BATCH_BUDGET_MS` | `10000` | Per-embedding-request time budget |
| `AI_E2E_MOCK_DELAY_MS` | `200` | Injected per-request delay for Suite B |
| `AI_E2E_KILL_BUDGET_SEC` | `10` | Time within which `KILL QUERY` must terminate an in-flight AI query (B2-6) |
| `AI_E2E_LATENCY_GATE_REAL` | `0` | `1` makes Suite B3 assert instead of only reporting |
| `AI_E2E_WRITE_BASELINES` | `0` | `1` regenerates `baselines/*.json` from this run instead of asserting against them |
| `AI_E2E_COMPARE_TO` | unset | Path to a previous Suite B results JSON; makes the run print a before/after table |

Targets carry exactly **two flags**, because the `local` target exists to test the test on a model that
cannot do everything the gateway can. A case whose flag is absent skips with a clear reason instead of
failing:

```python
INTERNAL = Target(
    chat="https://inference-internal.clickhouse.cloud/v1/chat/completions",
    embed="https://inference-internal.clickhouse.cloud/v1/embeddings",
    chat_model="claude-haiku-4-5",
    embed_model="qwen3-embedding-8b",           # native dimension
    embed_dim_model="text-embedding-3-small",   # honors `dimensions`
    toy_model=False, reports_token_usage=True,
)
LOCAL = Target(  # ollama, llama.cpp server, vLLM, …
    chat="http://{host_gateway}:11434/v1/chat/completions",
    embed="http://{host_gateway}:11434/v1/embeddings",
    chat_model="qwen2.5:0.5b",
    embed_model="all-minilm", embed_dim_model="all-minilm",
    toy_model=True,             # a 0.5B model and a fixed-384 embedder
    reports_token_usage=True,
)
```

| Flag | Meaning | Gates |
|---|---|---|
| `toy_model` | The model is a stand-in: it cannot be held to a non-trivial instruction, and its embedder ignores `dimensions`. Defaults to `False`, so a target added without thinking about the flag gets full strictness | A1-1, A1-3, A2-1, A2-3, A2-7, A2-8 |
| `reports_token_usage` | The provider returns a `usage` object — not about the model. HuggingFace TEI omits it | Every `AIInputTokens > 0` assertion, and the spend estimate |

Two flags rather than one per capability, deliberately: every additional axis is another way to
*silently remove coverage on the real endpoint* — a stray value turns an assertion into a skip and the
run still reports green. One model axis is coarse (it lumps instruction-following together with
`dimensions` support) but its failure mode is visible, since `internal` sets `toy_model=False` and any
run that skips those six cases has obviously mis-resolved its target. Note that if the limitation ever
turns out to be the *endpoint* rather than the model — a gateway route that drops `dimensions`, say —
the name stops describing the cause, and the flag should be renamed rather than stretched.

Consequently **`local` is a smoke path, not equivalent coverage**: it exercises the plumbing, the
response parsing, and the shape assertions, and skips the cases that need a capable model. Use it to
develop the suite and to check that an assertion is not too strict, not to certify a change.

There is no `deterministic_at_temp0` flag: A2-5 is report-only, so nothing branches on it.
`OpenAIProvider` sends no `seed` (`src/Functions/AI/OpenAIProvider.cpp:31-54`) and the gateway fronts
several backends, so byte-identical output at `temperature = 0` is not a contract for any target.

Two environment details that otherwise cost an afternoon:

- **Reaching a local model.** The model server runs on the host, so `localhost` inside the ClickHouse
  container is the container. Resolve the docker gateway from `/proc/net/route` inside the container
  (`iproute2` is not guaranteed in the test image) and substitute it into **both** the chat and the
  embed URL. That host is not loopback, so an `http://` endpoint also needs
  `ai_function_allow_insecure_endpoint = 1` in the query settings. `latency_mock_server.py` runs
  *inside* the container, is on loopback, and needs no such setting.
- **Reaching the internal gateway.** The container needs egress, DNS, and CA certificates. The
  preflight fixture curls the endpoint from inside the container and fails with that diagnosis instead
  of letting six function tests fail with a timeout.

### Secrets

Three channels carry the key out of the run if you let them, all inside the tree the job packages on
failure (`tests/integration/<suite>/` at `ci/jobs/integration_test_job.py:1232`,
`test_*/_instances*/*/configs/` at `:1225`, and the error hook's `test_*/_instances*/` at `:648`):

| Channel | Leak |
|---|---|
| praktika `--param` | Echoes every `KEY=VALUE` it sets into the job log (`:663`). It also cannot carry a value containing a space: praktika forwards it unquoted and the job's parser rejects the tail |
| the ambient shell environment | Not a leak - it simply does not work. praktika runs the job inside the `clickhouse/integration-tests-runner` container (`ci/praktika/runner.py:441`) and passes only `PYTHONUNBUFFERED`, `PYTHONPATH` and `--env-file ci/local.env` |
| `CREATE NAMED COLLECTION` | Persists the key to `database/named_collections/*.sql` |
| `env_variables=` on `add_instance` | Written verbatim to the host-side `_instances-gwN/.env` (`helpers/cluster.py:3688`, `:158-163`) and logged at DEBUG (`:158`) |

So the key never travels by DDL. The collections are **config-defined**, with the value pulled from
the environment and the element withheld from the preprocessed copy:

```xml
<!-- configs/ai_e2e_collections.xml, passed via main_configs= -->
<clickhouse><named_collections>
    <ai_e2e_chat>
        <provider>openai</provider>
        <endpoint from_env="AI_E2E_CHAT_ENDPOINT"/>
        <model from_env="AI_E2E_CHAT_MODEL"/>
        <api_key from_env="AI_E2E_API_KEY" hide_in_preprocessed="true"/>
    </ai_e2e_chat>
    <ai_e2e_embed>
        <provider>openai</provider>
        <endpoint from_env="AI_E2E_EMBED_ENDPOINT"/>
        <api_key from_env="AI_E2E_API_KEY" hide_in_preprocessed="true"/>
    </ai_e2e_embed>
</named_collections></clickhouse>
```

An unset variable is not an error: `from_env` logs a warning and leaves the element as written
(`ConfigProcessor.cpp:488-501`), so the fixture always passes all four variables - empty when the
suite is unconfigured - and `requires_live_endpoint` is what actually gates a run.
`ConfigProcessor` substitutes `from_env` at load (`src/Common/Config/ConfigProcessor.cpp:602-620`),
and `savePreprocessedConfig` writes the output of `hideElements`, which drops every element carrying
`hide_in_preprocessed="true"` (`:304-315`, `:941-957`, `:1011`). The file that lands in the
bind-mounted `configs/` directory therefore holds only variable *names*, the preprocessed copy omits
the `api_key` element entirely, no DDL exists to persist, and query text carries only
`map('credentials','ai_e2e_chat')`.

That leaves the env var itself, which `ClickHouseCluster` writes to `_instances-gwN/.env`. The
fixture rewrites that one line immediately after `cluster.start()` — docker-compose reads the file
only at `up` time, so the running container keeps its environment — and again on teardown, so no
failure path can package it.

**The channel is `ci/local.env`**, a gitignored (`.gitignore:2`) file that praktika passes into the
job container as `--env-file`; only its path appears on the docker command line, never its contents.

```
# ci/local.env
PYTEST_ADDOPTS=-m e2e
AI_E2E_API_KEY=…
AI_E2E_TARGET=internal
```

Even with all of the above, treat a failed run's `logs.tar.gz` as sensitive until you have looked at
it. One channel the `.env` scrub cannot reach: `helpers/cluster.py` logs the whole `env_variables`
dict at DEBUG (`:159`) and `pytest.ini` sets `log_level = DEBUG`, so the fixture installs a logging
filter that redacts the key from captured records.

For reference, `tests/integration/test_e2e_catalogs/test.py:198` hands real cloud credentials to the
server through plain `env_variables=` with none of this, which is safe today only because nothing in
CI ever selects those tests. §9 covers what changes when this suite does run in CI.

## 6. Running

Everything is configured through `ci/local.env` (see Secrets above). At minimum it must carry
`PYTEST_ADDOPTS=-m e2e`, since `pytest.ini` deselects the marker by default - without it a run
collects zero tests and reports success.

```bash
# structural counts only: free, no endpoint, no key, seconds
python -m ci.praktika run "integration" \
    --test "test_e2e_ai_functions/test_structural.py" --path ./build/programs/clickhouse

# timing matrix: free, no key, minutes, wants an unshared host
python -m ci.praktika run "integration" \
    --test "test_e2e_ai_functions/test_latency_arch.py" --path ./build/programs/clickhouse

# the live half (spends money)
python -m ci.praktika run "integration" --path ./build/programs/clickhouse \
    --test "test_e2e_ai_functions/test_basic_e2e.py" "test_e2e_ai_functions/test_params.py" \
           "test_e2e_ai_functions/test_concurrency.py"

# a larger corpus: put AI_E2E_DATA_SCALE=8 in ci/local.env and raise the timeouts
python -m ci.praktika run "integration" \
    --test "test_e2e_ai_functions/test_basic_e2e.py" --path ./build/programs/clickhouse \
    --session-timeout 7200
```

Three things to get right when running:

- **`--path` is mandatory.** The job otherwise defaults to `ci/tmp/clickhouse`, usually a stale binary
  from an unrelated build.
- **Name files explicitly, not the directory.** `get_parallel_sequential_tests_to_run`
  (`ci/jobs/integration_test_job.py:517-524`) appends the selector once per matching test file and
  `quote_tests` does not dedupe, so a directory selector may put the same path on pytest's command
  line once per file in it. Verify with `--collect-only` before spending money on a directory-wide run.
- **Raise both timeouts for large scales.** The sequential phase defaults to 3600 s
  (`ci/jobs/integration_test_job.py:1025`) and `pytest.ini:5` sets a **per-test** limit of 900 s. At
  `AI_E2E_DATA_SCALE=8` a single Suite A case can exceed 900 s long before its own budget assertion
  fires, so raise the per-test limit too: `PYTEST_ADDOPTS=-m e2e -o timeout=3600` in `ci/local.env`.

## 7. Guards

**Spend.** `config.py` estimates input tokens from the resolved corpus (`len(text)/4`) and output
tokens as `AI_E2E_EST_OUTPUT_TOKENS` per chat call — not `max_tokens`, which overestimates these
corpora by more than an order of magnitude and would abort a run costing cents. It prices them with
`AI_E2E_PRICE_*`, prints the estimate per suite, and aborts when the total exceeds
`AI_E2E_MAX_EST_USD`. Pricing left at `0` prints token counts and does not gate. Suite B's mock
experiments cost nothing and are excluded.

**Time.** Two real layers plus a backstop:

1. Each test asserts on `query_duration_ms` from `system.query_log`, so client and harness overhead
   stay out of the budget. Budgets come from `AI_E2E_PER_CALL_BUDGET_MS` (milliseconds).
2. `ai_function_request_timeout_sec` is set to `ceil(AI_E2E_PER_CALL_BUDGET_MS / 1000)`. This bounds a
   *stalled socket read*, not total request time (`FunctionBaseAI.cpp:506`): an endpoint that trickles
   bytes slowly is not caught by it.
3. `--session-timeout` and the subprocess kill are the backstop. Every `node.query` call from a thread
   passes an explicit `timeout=`, since `helpers/client.py` defaults to `None` and would otherwise
   block until the harness kills pytest.

**Isolation and reproducibility.** Every query carries a unique `query_id` and reads its own
`ProfileEvents` after `SYSTEM FLUSH LOGS`. Every query that asserts an exact call count also pins
`max_block_size ≥ rows`, `max_threads = 1`, and a single part — quotas and embedding batches are
per block (`FunctionBaseAI.cpp:499`, `:392`), so counts are only well-defined under that discipline.

## 8. Adding Anthropic later

- `config.py` gains `AI_E2E_PROVIDER` (`openai` | `anthropic`) plus per-provider endpoint and model
  defaults; named collections are created with the resolved provider.
- The embedding half of every suite is provider-gated: Anthropic has no embeddings API, so `aiEmbed`
  and `aiSimilarity` throw `NOT_IMPLEMENTED`.
- Structured output uses tool-use instead of `response_format`. The `aiClassify` and `aiExtract`
  assertions stay identical; the report records which provider produced them.
- `latency_mock_server.py` gains a `/v1/messages` path with the Anthropic response shape.

## 9. Future: running this from CI

Not built. This section records the mechanism options, what the suite already provides, and the one
place where the choice would feed back into the suite's own design.

### The template: Keeper stress

The closest analogue in the repo is an expensive job that is *not* label-driven. It has four parts:

1. **One shared `Job.Config`** (`ci/defs/job_configs.py:1262`) reused by three workflows, with the mode
   decided inside the job script from `Info().pr_number` and `Info().workflow_name` — PR runs a short
   subset, nightly runs the full matrix.
2. **Path-triggered in the PR workflow.** Added with a shortened timeout
   (`ci/workflows/pull_request.py:204-206`) and skipped by `filter_job.py:216` unless changed files
   touch `src/Coordination`, `tests/stress/keeper`, `programs/keeper-bench`, or the job script.
3. **`digest_config=Job.CacheDigestConfig(include_paths=[…])`**, so an unchanged digest reuses the
   cached result instead of paying again.
4. **Two `SCHEDULE` workflows** (`nightly_keeper.py`, `nightly_keeper_faults.py`) with
   `cron_schedules`, plus `post_hooks=["…/ingest_keeper_metrics.py"]` pushing metrics into the CI
   database — which is how Keeper trends are tracked over time rather than in committed files.

### Options

| Mechanism | Trigger | Precedent | Assessment |
|---|---|---|---|
| Path-triggered PR job | Changed files touch `src/Functions/ai*`, `src/Functions/AI/`, `tests/integration/test_e2e_ai_functions/` | Keeper (`filter_job.py:216`) | Best fit. AI-function PRs are rare, so "always run on AI PRs" costs little and needs no human action — a label's failure mode is that nobody remembers it |
| Digest cache | Job re-runs only when its `include_paths` digest changes | Keeper, compatibility jobs | Complementary, not an alternative. Same paths as the path trigger, so a rebase or unrelated push reuses the previous result |
| Scheduled cron on `master` | `Workflow.Event.SCHEDULE` + `cron_schedules` | `nightly_keeper.py` | Good for the free mock half at any cadence; weekly is enough for the paid half to catch endpoint drift |
| `workflow_dispatch` with inputs | Maintainer runs it, passing ref and scale | `create_release.py` (`Event.DISPATCH`, `dispatch_inputs`) | Simplest "run it now at scale 8" path, and the only one where data size is chosen at trigger time |
| Label-gated | `ci-ai-e2e` plus a push or re-run | the `ci-*` labels | Works, but needs a second action to start: the PR workflow is not subscribed to `labeled` events (`.github/workflows/pull_request.yml` has no `types:`, so GitHub's default `opened, synchronize, reopened` applies, and praktika has no labeled variant) |
| Merge-queue only | Once per merge attempt | `merge_queue.py` | Cheapest per merge, worst feedback: you learn at merge time |
| PR comment command | `/ai-e2e` in a comment | none | Needs an `issue_comment` workflow. Not worth it |

Two mechanisms worth taking regardless of trigger:

- **`set_allow_failure()`** (as `build_profile_diff_job` does, `pull_request.py:195`). A job depending on
  an external paid endpoint must never block a merge — gateway downtime turning into a red required
  check is how a suite like this gets deleted a year later.
- **Metrics into CIDB via `post_hooks`**, the way `ingest_keeper_metrics.py` does. See "Design coupling"
  below: this is the one option that changes the suite.

### Where a failure would show up

This decides the division of labour below, so it comes first. For a scheduled workflow the inventory is:

| Channel | Reach |
|---|---|
| The Actions run and the praktika HTML report (`enable_report=True`) | Pull, not push — nobody looks unprompted |
| GitHub's scheduled-failure email | Goes to whoever last modified the cron in the workflow file |
| CIDB (`enable_cidb=True`, as `nightly_keeper.py` sets) | Rows queryable on play.clickhouse.com; how Keeper stress results are actually consumed |
| The flaky-issue machinery — `ci/praktika/issue.py` fed by the Hourly workflow's "Collect flaky tests" job (`ci/workflows/hourly.py:16`), consumed by `ci/jobs/scripts/check_ci.py` | Turns recurring CIDB failures into labelled GitHub issues, but under `flaky test` framing rather than "regression" |

There is **no alert when a scheduled workflow fails** — `ci/workflows/hourly.py:5` says so in the repo
itself (`# TODO: add alert on workflow failure`). `team_notifications.py` is PR-comment-only and does not
apply. So a weekly pass/fail gate would fail silently for weeks.

### Recommended composition

Split by what can be made visible, not by what is cheap:

- **PR-triggered and blocking: `test_structural.py`.** Exact integers at `delay_ms = 0`, seconds to run.
  A regression appears as a red check on the PR that caused it — unmissable by construction, needing no
  alerting infrastructure. This also means `baselines/laziness.json` is validated on every AI-touching
  PR, which is what stops it going stale.
- **Weekly on `master`: `test_latency_arch.py` as a data feed, not a gate.** Ingest the timing metrics
  into CIDB from a `post_hook`, exactly as `ingest_keeper_metrics.py` does. Nobody has to chase a red
  check; the numbers accumulate and get queried when a performance change is on the table — the way the
  Keeper stress dashboard is used today. Mark the job `set_allow_failure()` so an infrastructure blip
  cannot wedge `master`.
- **Paid Suite A: dispatch or label, until the key handling below is proven.**

Mechanically, copy Keeper: one `Job.Config` whose script selects scope from
`Info().pr_number`/`workflow_name`, path-triggered via `filter_job.py`, digest-scoped to the AI paths.
Keep a `ci-ai-e2e` label as an *override* that forces the job on when the path filter did not fire — a PR
that only touches the planner, say, but whose laziness numbers you want.

If you later want the weekly run to shout rather than accumulate, the honest options are a `post_hook`
that opens or updates a GitHub issue through the `praktika.gh` primitives, or a Slack path that does not
exist in this repo today. Neither is inherited for free.

### Prerequisites, which are not about the trigger

- The key comes from praktika's secret store (`Secret.Config(..., Secret.Type.AWS_SSM_PARAMETER)`, read
  in-job with `info.get_secret`) — never from `--param`, which echoes values it sets
  (`ci/jobs/integration_test_job.py:663`).
- Use a dedicated, revocable CI key rather than a human's, so a leak costs a rotation. This is the
  primary control: the only one that does not depend on a mechanism no other suite has used.
- Add a masking rule for the key pattern in the suite's `configs/` — the mechanism
  `tests/integration/test_mask_sensitive_info/` exists to validate.
- Consider having that job not attach `_instances*` artifacts at all. The `from_env` +
  `hide_in_preprocessed` layout and the `.env` scrub (§5) already keep the key off disk, but a CI-run
  job is when an unproven mechanism is worth double-covering.

### What the suite already provides

No test-code change is needed when a trigger is built:

- Halves are separate files, so one job can run exactly the free part or exactly the paid part.
- All configuration is environment variables, so a job supplies it without touching test code.
- Missing credentials are a clean SKIPPED with a reason, never an unauthenticated call — so an
  unconfigured automatic run is quiet rather than red.
- Results are written as JSON with provenance (git SHA, target, model, scale, `nproc`), so a run nobody
  is watching is still interpretable afterwards.
- Nothing in the live half depends on a developer-local artifact; `AI_E2E_COMPARE_TO` is used only by
  the mock latency half.

### Design coupling

Two things feed back into the suite; everything else — path filters, digest configs, cron schedules,
dispatch inputs, allow-failure — is job configuration outside this directory and implies no change to the
tests.

1. **The file split.** Separating `test_structural.py` from `test_latency_arch.py` exists so the
   exact-integer cases can run on a PR while the timing matrix runs on a schedule (§4). Without it, a
   PR-triggered job would drag 12-20 minutes of timing work along, and the timing files' sequential
   scheduling would be forced onto the cheap cases.
2. **Metrics to CIDB from a `post_hook`.** This splits regression detection in two: `laziness.json` stays
   a committed baseline because `test_structural.py` gates it on every AI PR, while the *timing* numbers
   in `arch.json` are better as a CIDB time series — "compare against history" rather than "compare
   against a committed file", which removes the staleness problem §10 describes and most of the
   re-blessing ritual in `02-latency.md` §8. The commitment is a stable result-JSON schema, which the
   human-facing reports already need.

## 10. Assumptions and known limitations

- The internal gateway stays OpenAI-compatible and reachable from a docker container on a developer
  machine.
- The gateway does not rate-limit a single virtual key (per the 2026-06-18 findings in
  `~/mystuff/ai-funcs-scripts/ai-endpoint-test/FINDINGS.md`), so Suite A3's concurrency is bounded by
  ClickHouse. A3 records 429 and 5xx counts so a change there is visible instead of being read as a
  ClickHouse bug.
- The June 2026 audit recorded that the gateway silently drops `response_format: json_schema` on all
  routes. **That no longer reproduces.** The first full run of A2-11 pushed the model off-schema on
  purpose and the schema held: an unsatisfiable `aiExtract` schema came back as
  `{"blood_type": null, "isbn": null}` - exactly the requested keys with null values, which is the
  signature of enforcement rather than prompt compliance - and `aiClassify` over gibberish stayed
  inside its enum. The probe stays report-only so a regression shows up as a diff between runs.
- **Baselines can go stale.** The suite is manual by design, so `baselines/laziness.json` and
  `baselines/arch.json` are only validated when someone runs it. Mitigations: each baseline file
  records the git SHA and target it was generated on; a run whose SHA differs by more than a
  configurable distance prints a staleness warning; and regeneration is a deliberate
  `AI_E2E_WRITE_BASELINES=1` run whose diff must be reviewed, never a hand edit. §9 describes the
  structural fix — PR-gating `test_structural.py` and moving the timing numbers to CIDB — which retires
  most of this limitation once a trigger exists.

Open questions, answered by the first run of Suite B rather than by reading code:

- Do AI calls already run concurrently *across pipeline streams* (several parts, `max_threads > 1`)?
  `executeImpl` is serial within a block, but nothing stops several streams from calling it at once.
  Experiment B1 measures this directly.
- How long does `KILL QUERY` take on an in-flight AI query? There is no cancellation checkpoint in the
  row loop, so a block should be uninterruptible until it completes. B2-6 measures it.

## 11. Implementation order

1. `config.py`, `conftest.py` (including `requires_live_endpoint` and the preflight fixture), `corpus.py`,
   `asserts.py`. Get gating, the two target flags, and the counting discipline right before any assertion
   exists.
2. `test_basic_e2e.py` (A1) — highest value; catches anything that breaks against a real model.
3. `latency_mock_server.py`, then `test_structural.py` (exact integers, the laziness table) and
   `test_latency_arch.py` (B1 and the timing cases) — free to run, and the part that pays off on every
   later performance change. `test_structural.py` first: it is the cheapest and the one a CI job can
   eventually gate on.
4. `test_params.py` (A2).
5. `test_concurrency.py` (A3).
6. `test_latency_real.py` (B3) and the baseline files.

Steps 1–3 are the minimum useful suite.
