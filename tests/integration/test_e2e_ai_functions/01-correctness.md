# Suite A — correctness against a real endpoint

Companion to [`README.md`](README.md). Suite A answers three questions a mock cannot: does every
function work end to end against a real model in bounded time (A1), does the endpoint honor what we
send (A2), and is concurrent use correct (A3).

Assertions come in two kinds, marked per case:

- **Implementation-decided** (asserted): `ProfileEvents` counts, vector dimensions, result cardinality
  and row alignment, JSON validity, value ranges, exception classes.
- **Model-decided** (asserted only where the corpus makes non-compliance implausible, otherwise
  report-only): whether an output is the right label, contains the right substring, or obeys an
  instruction. The gateway drops `response_format: json_schema`
  (`~/mystuff/ai-funcs-scripts/ai-endpoint-test/FINDINGS.md` #1), so nothing but the prompt constrains
  these — the design says so out loud instead of calling them deterministic.

Model-decided assertions are additionally skipped on a target marked `toy_model`, so the `local`
target skips them rather than failing. See README §5 for the two flags and why there are only two.

## A0 — preflight

A **session-scoped autouse fixture in `conftest.py`**, not a test module: module order is
duration-derived under both xdist schedules, so no test file can be guaranteed to run first. It raises
on failure, aborting the session once with one diagnosis.

| Id | Check | Failure message must say |
|---|---|---|
| A0-1 | `curl -sS -o /dev/null -w '%{http_code}' <chat endpoint>` from inside the ClickHouse container | Whether it is DNS, TLS/CA, egress, or auth |
| A0-2 | One-row `aiGenerate` through the created named collection | The resolved endpoint, model, and target |
| A0-3 | `SELECT value FROM system.build_options WHERE name = 'GIT_HASH'` matches `git rev-parse HEAD` | Which commit the binary was built from, and which one the tree is at |
| A0-4 | Spend estimate for the selected suites and scale is under `AI_E2E_MAX_EST_USD` | Estimate, cap, and which suite dominates |

A0-3 compares **git hashes**, not `version()`. The harness copies whatever `--path` names into the
container, so comparing the server's version to that binary is a tautology; and the version string
changes only on a release bump, so it matches for every commit in a cycle - including the stale binary
a failed `ninja` leaves behind, which is the trap this check exists for. It warns rather than fails,
since running an older binary on purpose is legitimate.

## Shared fixtures

Each Suite A module starts with:

```python
pytestmark = [pytest.mark.e2e, requires_live_endpoint]
```

`requires_live_endpoint` is a `skipif` defined in `conftest.py`: it fires when the resolved target has
no usable credentials (no `AI_E2E_API_KEY` for `internal`; a `local` target needs none), and its reason
names the missing variable. There is no separate enable flag — configuration is the gate, and an
unconfigured run reports SKIPPED with a readable reason. See README §2.

| Fixture | Scope | Provides |
|---|---|---|
| `cfg` | session | `EndpointConfig`: endpoints, models, key, the two target flags, scale, budgets |
| `preflight` | session, autouse | A0 |
| `started_cluster` | session | Started cluster; named collections; corpus tables; drops everything on teardown. Session scope avoids five container restarts |
| `corpus` | session | Per-workload row lists; only the loop corpora scale |
| `q` | function | `q(sql, settings=None, counting=False) -> (rows, profile_events, duration_ms)`: unique `query_id`, run, `SYSTEM FLUSH LOGS`, read `system.query_log` |

Named collections `ai_e2e_chat` and `ai_e2e_embed` are **config-defined**, not created by DDL:
`configs/ai_e2e_collections.xml` passed via `main_configs=`, with every value read through `from_env`
and `api_key` additionally marked `hide_in_preprocessed="true"`. See README §5 for the file and for
why DDL is not used — it would persist the key into the instance directory that a failed run packages
into `logs.tar.gz`. The fixture's remaining job is to scrub `_instances-gwN/.env` right after
`cluster.start()`.

Two collections, not three: the embedding model is chosen by the *query*, so `aiEmbed` with a native
model and with a `dimensions`-capable model share one collection. `model` must not appear in an
embedding collection — it is rejected.

Suite B's mock collections (`ai_e2e_mock_chat`, `ai_e2e_mock_embed`) stay plain DDL: their `api_key`
is the literal `'mock-key'`, so none of this applies.

Every query sets `allow_experimental_ai_functions = 1` and
`ai_function_request_timeout_sec = ceil(AI_E2E_PER_CALL_BUDGET_MS / 1000)`.

### Counting discipline

`q(..., counting=True)` sets `max_block_size` ≥ row count, `max_threads = 1` and
`preferred_block_size_bytes = 0`, and the fixture creates corpus tables with a single part
(`OPTIMIZE TABLE … FINAL` after load). The byte-size preference matters as much as the row count: it
defaults to 1000000 and can split a block *below* `max_block_size` on its own.

What depends on this is narrower than it first looks. For the four text functions
`AIAPICalls == rows` regardless of blocking, since the loop issues one request per row
(`FunctionBaseAI.cpp:517`). It is the **embedding batch counts** (`:392`) and the **quota-bounded**
counts (`:499`) that are per block, so a second block restarts both.

`assert_ai_usage` is parameterized by function kind, because `api_calls` means rows for chat and
batches for embeddings:

```python
assert_ai_usage(pe, kind="chat",  rows=n)              # api_calls == n
assert_ai_usage(pe, kind="embed", rows=n, batch=100)   # api_calls == ceil(live_rows / batch)
```

`AIInputTokens > 0` is asserted only when the target sets `reports_token_usage` (HuggingFace TEI omits
`usage`).

### Time budget

```python
def budget_ms(rows, kind):
    per   = cfg.per_call_budget_ms if kind == "chat" else cfg.embed_batch_budget_ms
    units = rows if kind == "chat" else math.ceil(rows / cfg.embed_batch_size)
    return int(per * units * 1.5) + 5000
```

The budget assumes today's serial architecture. If a change adds parallelism the budget becomes loose
but never wrong; tightening it is Suite B's job, not A1's. A1 only asserts "not pathologically slow",
which is the request's "taking a really long time is effectively a correctness issue".

## Corpus

`corpus.py` builds every corpus deterministically from a fixed seed list. Two flavors:

- **Pinned rows** — used by semantic assertions. Fixed text, verbatim, **do not scale**: duplicating
  the same 12 sentences buys no coverage and only costs money.
- **Loop rows** — used by counting, timing, and concurrency assertions. Sized by
  `AI_E2E_DATA_SCALE` and suffixed ` (ref NNNNNN)` so no two prompts are byte-identical, which defeats
  endpoint-side response caching.

| Workload | Base size | Scales | Row shape | Checkable part |
|---|---|---|---|---|
| `arith` | 8 | yes | `"What is {a} + {b}? Reply with the number only."` | Exact sum as a substring (model-decided, but non-compliance implausible) |
| `classify` | 12 | no | Unambiguous sentiment sentences with a gold label in `['positive','negative','neutral']` | Output ∈ categories; equals gold |
| `extract` | 8 | no | `"{name} lives in {city} and is {age} years old. Order {n}."` | JSON keys exactly `{name, city}`; values equal the injected ones |
| `translate` | 8 | no | `"Order {n} shipped to Berlin on Monday."` → French | Output ≠ source, contains `{n}` |
| `embed_pairs` | 6 triplets | no | `(anchor, paraphrase, unrelated)` | `cos(a,p) > cos(a,u) + 0.05` |
| `embed_bulk` | 40 | yes | Distinct short sentences | Vector dimension, count, `ProfileEvents` |

The `extract` instruction is `'{"name": "the person name", "city": "the city they live in"}'`; the
injected `age` and order number are decoys that must not appear in the output key set.

## A1 — end to end, per function

One query per case over the named corpus, `ORDER BY id`, asserted row by row. Counting cases use
`counting=True`.

| Id | Query | Implementation-decided | Model-decided |
|---|---|---|---|
| A1-1 | `aiGenerate(prompt, map('credentials','ai_e2e_chat'))` over `arith` | `api_calls = n`, `rows_processed = n`, `rows_skipped = 0`, tokens > 0, within budget, cardinality `n` | Each result contains the expected sum |
| A1-2 | `aiClassify(text, ['positive','negative','neutral'], …)` over `classify` | counts as above | Output ∈ categories after trim; equals gold label |
| A1-3 | `aiExtract(text, '{"name":…,"city":…}', …)` over `extract` | counts; every row `isValidJSON` | `JSONExtractKeys` = `{name, city}`; values equal injected; no `age` |
| A1-4 | `aiExtract(text, 'Extract the city name', …)` | counts; non-empty | Contains the injected city |
| A1-5 | `aiTranslate(text, 'French', …)` over `translate` | counts; non-empty; ≠ source | Contains the order number |
| A1-6 | `aiEmbed(text, '<AI_E2E_EMBED_MODEL>', map('credentials','ai_e2e_embed'))` over `embed_bulk` | All `length()` equal and > 0 (native dimension recorded); all values finite; `L2Norm > 0`; `api_calls = ceil(n/100)`; `rows_processed = n` | — |
| A1-7 | `aiSimilarity` over `embed_pairs` plus a self-pair | Every score ∈ [-1, 1]; self-pair ≥ 0.999; `api_calls` = batch count over live operands | `cos(anchor, paraphrase) > cos(anchor, unrelated) + 0.05` |
| A1-8 | All six functions in one `SELECT` over 4 rows | Each column's shape assertion; `api_calls` = sum of per-function expectations (4 × 4 chat + 1 embed batch + 1 similarity batch) | Per-column semantic checks |

`NULL`/empty handling and the `d00280af28a` operand-skipping behavior are **not** re-tested here — the
mock suite pins them for free (`test_ai_functions/test.py:1203`, `:1144`, `:664`, `:244`).

A1-8 is the cross-function case: one query, one context, six functions, shared provider creation.

## A2 — the endpoint honors parameters and settings

The cases that justify hitting a real endpoint. Everything endpoint-independent (quota skip/throw,
`throw_on_error`, batch counts) is left to the mock suite.

| Id | Parameter | Method | Assertion | Kind |
|---|---|---|---|---|
| A2-1 | `dimensions` | `aiEmbed(text, AI_E2E_EMBED_DIM_MODEL, map('credentials','ai_e2e_embed','dimensions','<d>'))` for `d ∈ {256, 512, 1024}` | `length() = d` for every row and every `d` | asserted; skipped on a `toy_model` target |
| A2-2 | `dimensions`, unsupported model | Same with the native-only model | Record whether the endpoint errors, ignores, or honors | report-only |
| A2-3 | `dimensions` in `aiSimilarity` | `d = 256` vs native over `embed_pairs` | Both orderings hold; both scores ∈ [-1, 1] | asserted / model-decided |
| A2-4 | `max_tokens` | `'Write 400 words about columnar storage.'` with `max_tokens ∈ {'16','256'}` | `length(short) < length(long)`, both non-empty. `AIOutputTokens ≤ 16` recorded but not asserted: reasoning-capable routes can report more, and `FINDINGS.md` #3 shows the gateway's handling is loose | asserted / report-only split |
| A2-5 | `temperature = '0'` | Same constrained prompt, three separate queries | Record whether all three are byte-identical | report-only — no `seed` is sent (`OpenAIProvider.cpp:31-54`) and the gateway fronts several backends |
| A2-6 | `model` override reaches the wire | `map(…,'model','definitely-not-a-model')`, then `AI_E2E_CHAT_MODEL_ALT` | Bogus: throws, message names the model or carries the 400 body. Alt: succeeds | asserted (alt skipped if unset) |
| A2-7 | `system_prompt` (`aiGenerate`) | `'Reply with exactly the word BANANA and nothing else.'` | Output contains `BANANA` | model-decided |
| A2-8 | `instructions` (`aiTranslate`) | `'Prefix your answer with "TR:".'` | Output starts with `TR:` | model-decided |
| A2-9 | `ai_function_embedding_max_batch_size` | `b ∈ {1, 3, 100}` over 12 rows, `counting=True` | `api_calls = ceil(12/b)`; vectors agree across `b` within `1 - cos ≤ 1e-4` position by position | asserted; the tolerance is measured on the chosen model before it becomes a gate |
| A2-10 | Structured-output enforcement | Input matching no category (`aiClassify` over gibberish); an `aiExtract` schema the text cannot satisfy | Record whether the response stays inside the enum / schema | report-only — the single most valuable endpoint-capability signal (`FINDINGS.md` #1) |

A2-9 is the one place the same input must produce the same vector. If the endpoint is non-deterministic
at that level, the suite says so, which matters before anyone builds a vector index on `aiEmbed`.

Deliberately in the mock suite instead, where they are exact rather than flaky
([`02-latency.md`](02-latency.md) §5): `ai_function_request_timeout_sec`, `ai_function_max_retries` and
backoff, connection reuse.

## A3 — concurrency

Two properties: **isolation** (no result, count, or vector crosses a query boundary) and **liveness**
(nothing deadlocks or serializes unexpectedly). All cases use `ThreadPoolExecutor` with
`k = min(4 × AI_E2E_DATA_SCALE, 16)` separate `node.query` calls, each passing an explicit `timeout=`.

| Id | Shape | Assertions |
|---|---|---|
| A3-1 | `k` concurrent one-row `aiGenerate`, query `i` prompting `'Reply with exactly the token TOKQ{i}.'` | Result `i` contains `TOKQ{i}` and no other `TOKQ`; all `k` succeed |
| A3-2 | `k` concurrent queries, one per function, each over its own corpus | Each satisfies its A1 assertions; per-`query_id` `ProfileEvents` match that function's expectation exactly (counters are per query, not global) |
| A3-3 | `k` concurrent multi-row `aiGenerate` over disjoint `arith` slices, `ORDER BY id` | Every row's answer matches its own input; no foreign row appears; per-query cardinality exact |
| A3-4 | `k` concurrent `aiEmbed` over disjoint slices vs a reference computed serially beforehand | `1 - cos(concurrent, reference) ≤ 1e-4` per position — catches batch-index mixing under load |
| A3-5 | Two concurrent queries, each `ai_function_max_api_calls_per_query = 3` over 3 rows | Both complete fully, each `api_calls = 3`, `rows_skipped = 0` — proves cross-query isolation of the counter (its scope is per block, per `FunctionBaseAI.cpp:499`) |
| A3-6 | Intra-query parallelism: `MergeTree` with 8 parts, `max_threads = 8`, one `aiClassify` over all rows | Every label correct; `api_calls = n` exactly (no duplicate or dropped calls when several streams call `executeImpl` concurrently); `rows_skipped = 0`. Not a counting case — the point is that the total is right despite multiple blocks |
| A3-7 | Endpoint health under concurrency | Report-only: per-query duration p50/p95, 429 and 5xx counts surfaced as exceptions, retries used. Fails only if A3-1..A3-6 fail |

A3-6 is the thread-safety test of the function object and the provider, and the cheapest existing probe
of whether cross-stream parallelism already happens: run it at `max_threads = 1` and `8` on the same
data and report the ratio. Suite B turns that observation into a measurement.

## Report

Each module appends to `<temp>/ai_e2e_results.json` and renders a markdown block from it — JSON is the
persisted form, markdown is generated, so nothing parses a rendered table:

```
## Suite A (target=internal, chat=claude-haiku-4-5, embed=qwen3-embedding-8b, scale=1)
| case | rows | api_calls | in_tok | out_tok | duration_ms | budget_ms | verdict |
|------|-----:|----------:|-------:|--------:|------------:|----------:|---------|
| A1-1 aiGenerate/arith    |  8 |  8 |  312 | 24 | 4102 | 185000 | ok |
| A1-6 aiEmbed/bulk        | 40 |  1 | 1180 |  0 |  612 |  20000 | ok, native dim 4096 |
| A2-2 dimensions/native   |  4 |  1 |    … |  … |    … |      … | endpoint ignores `dimensions` |
| A2-5 temperature 0       |  3 |  3 |    … |  … |    … |      … | 2/3 identical — not deterministic |
| A2-10 schema enforcement |  4 |  4 |    … |  … |    … |      … | out-of-enum output leaked |
| A3-7 concurrency health  | 32 | 32 |    … |  … | p50 891 / p95 1733 | — | 0×429, 0×5xx |
```

The `verdict` column carries the report-only findings, so an endpoint capability change shows up as a
diff between runs rather than as silence.
