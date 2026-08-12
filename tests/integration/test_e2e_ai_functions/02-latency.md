# Suite B — latency and architecture

Companion to [`README.md`](README.md). Suite B answers one question: **did this code change make the
AI-function implementation faster or slower, for architectural reasons?**

It is not a latency test of the endpoint or the network. Those dominate wall-clock and are outside our
control, so the design pushes them out of the measurement instead of averaging over them.

## 1. Principle

A real endpoint's per-call latency `D` varies by model, region, load, and time of day — the competitive
benchmark ([ClickHouse/ai#2437](https://github.com/ClickHouse/ai/pull/2437)) measured ClickHouse
`generate` p50 at 2921 ms on Haiku and 1097 ms on Llama-3.1-8B, a 2.7× swing from the model alone. No
threshold on wall-clock survives that.

What is stable is the relationship between `D` and total query time. Today, for the four text
functions:

```
T ≈ rows × D          (one request per row, serial within a block)
```

`src/Functions/FunctionBaseAI.cpp:517`. Embedding functions give `T ≈ ceil(rows / batch) × D_batch`.
Every architectural improvement worth making changes the *shape* of that formula, not the value of `D`:

| Change | Effect on the shape |
|---|---|
| Parallel requests within a block | `T ≈ rows × D / P` |
| Batching several prompts per chat request | `T ≈ ceil(rows / B) × D_B`, and `AIAPICalls` drops |
| Lazy evaluation (AI functions last, after filters and limits) | `rows` becomes `rows_surviving_filters` |
| Deduplicating identical inputs | `rows` becomes `distinct(rows)` per block |
| Connection reuse / cheaper prompt construction | reduces the constant, invisible while `D` dominates |

So Suite B measures the shape with `D` **known and injected**, and treats the real endpoint as a
secondary sanity check. The primary signal is free, deterministic, and needs no key.

The bet has a boundary, stated up front: injecting `D` is sound for exact integers (M1, M2, M5) and for
ratios where `D` dominates (M3). It is **not** sound as `D → 0`, where a Python mock's own per-request
cost is the same order as ClickHouse's per-row cost. M4 therefore does not use wall-clock at all.

## 2. Metrics

| Metric | Source | Detects | Noise |
|---|---|---|---|
| **M1 API calls** `AIAPICalls` per query | `system.query_log` `ProfileEvents` | Lazy evaluation, batching, dedup, duplicate evaluation, wasted retries | none — exact integer |
| **M2 Max in-flight** | Mock `/stats` | Parallelism, and its degree | none — exact integer |
| **M3 Effective concurrency** `C = (expected_calls × D) / T` | Injected `D` + `query_duration_ms` | Parallelism achieved end to end | low, `D` dominates |
| **M4 Per-row CPU cost** | `ProfileEvents['UserTimeMicroseconds'] / rows` at `D = 0` | Prompt construction, JSON handling, allocation | moderate — repeat and take the median |
| **M5 Connection efficiency** `requests / connections` | Mock `/stats` | Connection-reuse regressions (each new TLS handshake is real latency against a real endpoint) | none — exact integers |
| **M6 Cancellation latency** | `KILL QUERY` to terminal state | Missing cancellation checkpoints in the row loop | low |
| **M7 Real p50/p95, rows/s** | Real endpoint | Absolute numbers for a PR description; cross-check that M3 survives contact with reality | high — reported, asserted only with `AI_E2E_LATENCY_GATE_REAL=1` |

M1 is the most valuable metric here: an exact integer, free to obtain, and it moves on exactly the
changes the request cares about. M2 and M3 measure parallelism by independent means — disagreement
between them is itself a finding, usually a queue.

Two traps this design works around:

- **`ProfileEvents` are lost on failure.** All five AI counters are incremented after the row loop
  (`FunctionBaseAI.cpp:591`), so a query that throws records none of them. Any case that expects an
  exception reads the mock's `/stats` request count instead of M1.
- **Connections are pooled server-wide.** `OpenAIProvider` uses
  `makeHTTPSession(HTTPConnectionGroupType::HTTP, …)` (`OpenAIProvider.cpp:60,145`), so sockets survive
  across queries and resetting the mock's counters does not close them. An absolute "connections
  opened = 1" is not reproducible; M5 is a ratio, and each case that cares binds the mock on a fresh
  port so it starts cold.

Every latency query pins `max_block_size` and `max_threads` explicitly, and the tables state the part
count. Without that, block splitting makes M1 and M2 unreproducible across machines.

**Which file a case lives in follows from its metric**, because the two halves get different triggers
(README §9):

| File | Cases | Property | Runtime |
|---|---|---|---|
| `test_structural.py` | M1 and M5 cases at `delay_ms = 0`: B2-2, B2-3's counts, B2-5's count pass, B2-10 and B2-11's request counts | Exact integers; tolerates a shared host; parallel-safe | seconds |
| `test_latency_arch.py` | Everything with a wall-clock or in-flight assertion: B1, B2-1, B2-3's timing relation, B2-4, B2-6, B2-7, B2-8, B2-9, B2-5's time pass | Needs an unshared host; runs sequentially | 12–20 min |

The split is not cosmetic: it is what lets the deterministic half run on every AI-touching PR as a
blocking check while the timing half runs weekly as a CIDB data feed.

## 3. The controlled-latency mock

`latency_mock_server.py`, a new file: the existing
`tests/integration/test_ai_functions/mock_ai_server.py` is a single-threaded `HTTPServer` and would
serialize every request, reporting `max_in_flight = 1` no matter what ClickHouse does. Leave it alone;
it is correct for what it tests.

Requirements:

- `ThreadingHTTPServer`, `daemon_threads = True`, `protocol_version = "HTTP/1.1"` with correct
  `Content-Length` so keep-alive works — otherwise M5 measures the mock.
- Paths `/v1/chat/completions` and `/v1/embeddings`, response shapes identical to the existing mock
  (`choices[].message.content`, `usage`, `data[].index`), so `OpenAIProvider` parses them unchanged.
- Configurable via `POST /config`, effective until `POST /reset`:

| Key | Meaning |
|---|---|
| `delay_ms` | Fixed sleep before responding. `time.sleep` releases the GIL, so threads scale fine at `delay_ms > 0` |
| `jitter_ms` | Uniform jitter, default `0` |
| `max_concurrency` | Simulated endpoint limit; behavior beyond it set by `over_limit` |
| `over_limit` | `429` (immediate rejection) or `queue` |
| `output_tokens` | Length of generated content, so `max_tokens` behavior is testable |
| `embedding_dim` | Vector size from `/v1/embeddings` |
| `echo_token` | Echo a token derived from the request body, for cross-talk checks |

- `GET /stats` returns, since the last reset, **excluding the control paths** (`/config`, `/reset`,
  `/stats`), which would otherwise inflate every counter:

```json
{"requests": 256, "by_path": {"/v1/chat/completions": 256},
 "max_in_flight": 1, "mean_in_flight": 1.0, "connections": 1,
 "over_limit_rejections": 0, "first_request_ns": …, "last_response_ns": …}
```

`max_in_flight` counts **accepted** requests only — a 429-rejected request never enters the gauge, so
M2 can never exceed `max_concurrency`. `connections` counts distinct TCP connections
(`BaseHTTPRequestHandler.setup`).

- Named collections for Suite B, created by its fixture on the mock's loopback port:

```sql
CREATE NAMED COLLECTION ai_e2e_mock_chat AS
    provider = 'openai', endpoint = 'http://localhost:<port>/v1/chat/completions',
    model = 'mock-model', api_key = 'mock-key';
CREATE NAMED COLLECTION ai_e2e_mock_embed AS
    provider = 'openai', endpoint = 'http://localhost:<port>/v1/embeddings', api_key = 'mock-key';
```

Loopback is exempt from the `https` check, so `ai_function_allow_insecure_endpoint` is not needed.
Cases that need a cold connection pool get a fresh port.

## 4. B1 — characterization (report, no assertions)

The matrix that says what the implementation currently does. Run before and after a change; the report
is the artifact.

Fixed: `rows = 64 × AI_E2E_DATA_SCALE`, `delay_ms = 200`, `output_tokens = 32`.

| Axis | Values |
|---|---|
| Function | `aiGenerate` (chat), `aiEmbed` (batched) |
| `max_threads` | 1, 4, 8 |
| Table parts | 1, 8 |
| `max_block_size` | 8, 64, `rows` |
| `ai_function_embedding_max_batch_size` (embed only) | 1, 16, 100 |

Reported per cell: `T`, M1, M2, M3, M5, `T / (rows × D)`. The interesting cells:

- `max_threads = 8`, 8 parts, `max_block_size = 8`: if M2 > 1, cross-stream parallelism already exists
  and `max_threads` is the current throughput lever. This settles the README's open question in one run.
- `max_block_size = rows` vs `8`: shows whether block splitting is what produces any parallelism, and
  therefore whether an in-function thread pool would add anything on top.
- Embed at `batch = 1` vs `100`: quantifies the batching win chat does not have.

## 5. B2 — gating assertions (mock)

| Id | Setup | Assertion | Catches |
|---|---|---|---|
| B2-1 | `aiGenerate`, 32 rows, `D = 200 ms`, `max_threads = 1`, one part, `max_block_size = 32` | `T ≤ 32 × D × 1.4` (one-sided) | Extra hidden requests, per-row overhead blowups, an accidental sleep. One-sided on purpose: serial execution makes `T ≥ 32 × D`, so a lower bound could only fire on the parallelism B2-4 exists to reward |
| B2-2 | Same, fresh mock port | M1 `= 32`; M5 `= 32 / 1` (one connection, 32 requests) | Duplicate calls; connection-reuse regression |
| B2-3 | `aiEmbed`, 32 rows, batch ∈ {1, 8, 32}, one block | `T ≈ M1 × D` within 40% | Batching that stops overlapping with the time it should take. The call *count* is already pinned free in the mock suite (`test_ai_functions/test.py:710`); the timing relation is not |
| B2-4 | `aiGenerate`, 32 rows, `max_threads = 8`, 8 parts, `max_block_size = 4` | M2 `≥ baselines/arch.json["max_in_flight_8t"]`; M3 `≥ baseline × 0.8` | **Loss of parallelism.** The blessed baseline is whatever B1 finds today; the day a change adds parallelism, the new value is blessed and can never silently regress |
| B2-5 | Laziness scenarios, §6 | Per scenario, M1 `≤ baseline`; `= ideal` where baseline already equals ideal | Any change that makes evaluation less lazy |
| B2-6 | `aiGenerate`, 4 rows, `D = 15000 ms`, one block, `KILL QUERY` after 2 s | Query reaches a terminal state within `AI_E2E_KILL_BUDGET_SEC` | Missing cancellation checkpoint. Expected to fail today — see §8. Few rows × large `D` so the case is short and the margin wide |
| B2-7 | `max_concurrency = 2`, `over_limit = 429`, `ai_function_max_retries = 3`, 16 rows, `max_threads = 8`, 8 parts, `max_block_size = 2` | All 16 rows correct; query succeeds; `over_limit_rejections` reported | Correctness against a throttling endpoint. Gated on rows and rejections, not on M2 — with serial execution M2 is 1 and any `M2 ≤ 2` bound is vacuous |
| B2-8 | Same, `ai_function_max_retries = 0` | Query throws; message carries the status; mock `/stats` shows ≥ 1 rejection | Retry configuration silently swallowing throttling |
| B2-9 | `D = 0`, 512 rows, `output_tokens = 4`, repeated 5× | Median M4 `≤ baselines/arch.json["cpu_us_per_row"] × 1.5`, where M4 is CPU time per row from `ProfileEvents`, **not** wall-clock | Per-row CPU regressions in prompt building and response parsing, without measuring the mock's own cost |
| B2-10 | `ai_function_request_timeout_sec = 1`, `D = 3000 ms`, 2 rows, retries `0` | Query throws a timeout within ~2× the timeout; mock `/stats` `requests = 1` | Timeout not honored per request. Reads `/stats`, not M1: the query throws, so no `ProfileEvents` are recorded |
| B2-11 | Same, `ai_function_max_retries = 2`, `ai_function_retry_initial_delay_ms = 100` | `/stats` `requests = 3`; elapsed consistent with 100 + 200 ms backoff | Retry count and backoff, deterministically — impossible against a real endpoint |

B2-4 is the "did this change add parallelism" test; B2-5 is the "did the planner get AI-aware" test.
Both compare against a committed baseline, so an improvement is a deliberate, reviewable diff.

## 6. Laziness scenarios

`t` is a `MergeTree` table, one part, `N = 256 × AI_E2E_DATA_SCALE` rows, `max_block_size = 64`,
`max_threads = 1`. Two passes:

- **Count pass** at `D = 0`, in `test_structural.py`: M1 per scenario. Free, instant, exact — and the
  pass a PR-triggered job gates on, which is what keeps `baselines/laziness.json` from going stale.
- **Time pass** at `D = 200 ms` for L1, L2, L4, L10, in `test_latency_arch.py`: `T` next to M1, so the
  report shows the *latency* a lazy evaluator would save, not only the call count. The request asks the
  test to "show an improvement (or a regression)", and a unitless count does not. At today's non-lazy
  counts each of these takes ~51 s, which is why they are not on the PR path.

Ideals are **per block**, because nothing in the engine is global to a query: with `max_block_size = 64`
and `N = 256` there are 4 blocks, so a per-block optimization can at best reach `4 × ideal_per_block`.

Chat scenarios use `chat(x)` = `aiClassify(x, ['positive','negative','neutral'],
map('credentials','ai_e2e_mock_chat'))`. Dedup and CSE scenarios use `emb(x)` =
`aiEmbed(x, 'mock-model', map('credentials','ai_e2e_mock_embed'))`, because `aiEmbed` declares
`isDeterministic() = true` (`aiEmbed.cpp:87`) while the chat functions inherit `false`
(`FunctionBaseAI.h:58`) — memoization and CSE are structurally available to one and not the other.
Running both is the point: it documents the asymmetry.

| Id | Query | Ideal M1 |
|---|---|---|
| L1 | `SELECT chat(x) FROM t WHERE id % 8 = 0` | `N/8` |
| L2 | `SELECT chat(x) FROM t LIMIT 5` | 5 |
| L3 | `SELECT chat(x) FROM t ORDER BY id LIMIT 5` | 5 |
| L4 | `SELECT count() FROM t WHERE id % 8 = 0 AND chat(x) = 'positive'` | `N/8` (AI predicate last) |
| L5 | `SELECT if(id % 8 = 0, chat(x), '') FROM t`, `short_circuit_function_evaluation = 'force_enable'` | `N/8` |
| L6 | `SELECT emb(x) FROM t` where `x` has 4 distinct values | 4 per block → 16 |
| L7 | `SELECT emb(x) FROM (SELECT DISTINCT x FROM t)` — control for L6 | 1 batch |
| L8 | `WITH emb(x) AS a SELECT a, arraySum(a) FROM t` | `N` rows in `ceil(N/batch)` calls per block (regression: double) |
| L9 | `SELECT chat(x) FROM t WHERE chat(x) = 'positive'` | `N` (regression: `2N`) |
| L10 | `SELECT chat(x) FROM t PREWHERE id % 8 = 0` | `N/8` |

`baselines/laziness.json` records what the implementation does today:

```json
{
  "_comment": "Generated by AI_E2E_WRITE_BASELINES=1. Bless changes only with the code change causing them.",
  "git_sha": "…", "generated_on": "…", "N": 256, "max_block_size": 64,
  "scenarios": {"L1": {"ideal": 32, "current": 32}, "L2": {"ideal": 5, "current": 64}}
}
```

The test asserts `current_run ≤ baseline.current` and reports `ideal / current` per scenario plus a
suite mean. An optimizer change that makes AI functions lazy shows up as `current` values collapsing
toward `ideal`, blessed in the same commit. That diff is the evidence the change worked.

Never hand-fill `current`: generate with `AI_E2E_WRITE_BASELINES=1` and review the diff.

## 7. B3 — real endpoint (report; asserted only on request)

Mirrors the competitive benchmark's method so numbers are comparable to
[ClickHouse/ai#2437](https://github.com/ClickHouse/ai/pull/2437):

1. **Warm up** — two discarded single calls per function.
2. **Single-call latency** — `n = 20 × AI_E2E_DATA_SCALE` one-row queries per function, sequential;
   report p50/p95 of `query_duration_ms`.
3. **Batch throughput** — one query over `64 × AI_E2E_DATA_SCALE` rows per function, forcing per-row
   evaluation with `WHERE NOT ignore(<ai call>)` so nothing can prune the projection; verify M1 `= rows`.
   Report `s/row`, rows/s, tokens/s.
4. **Effective concurrency against the real endpoint** — `C = rows × p50_single / T_batch`, using the
   p50 measured in the same session as `D`. This is M3 with a measured rather than injected `D`: the one
   number that says whether parallelism added in the implementation survives contact with the endpoint.
   An endpoint connection limit shows up as `C` plateauing below the mock's `C`.
5. **Token cost** — per-query tokens, priced by `AI_E2E_PRICE_*` if set; unpriced is reported as
   unpriced, never as zero.

Results go to `<temp>/ai_e2e_latency_real.json` with target, model, scale, git SHA, and `nproc`, and are
**not committed**: cross-session comparison of real-endpoint numbers is not meaningful and a committed
number would become a false gate.

## 8. What goes in the baselines, and what does not

A metric is committed only if it is the same on every machine. That splits the suite cleanly:

| Metric | Committed? | Why |
|---|---|---|
| Laziness counts, batch counts, calls per row | **yes** | Exact integers, fixed by the implementation and the pinned settings. Identical on a laptop, a c6a.4xlarge, or ARM |
| `max_in_flight_8t` | yes | Near-structural: how many streams the pipeline builds, given pinned `max_threads` and part count |
| `effective_concurrency_8t` | yes | Dimensionless, and ~80% of `T` is the injected delay, so the host contributes little |
| `requests_per_connection` | yes | Ratio of two exact integers |
| Wall-clock durations | **no** | Host speed, load, thermals |
| `cpu_us_per_row` | **no** | Pure host and build property |
| Real-endpoint p50/p95 | **no** | Endpoint, model routing, region, time of day |

**Host-dependent numbers are compared run-local instead**: the same host, the same session, one binary
against another, via `AI_E2E_COMPARE_TO`. That is what this repository already does for performance -
`ci/jobs/scripts/perf/compare.sh` runs a `left` and a `right` binary on one runner and reports the
delta, and none of the 540 performance tests commits an absolute number. Same-host before/after
cancels host variance exactly, which pinning an instance type cannot: two identical instance types
still differ by noisy neighbours, CPU stepping and thermals.

The evidence for drawing the line here is direct. Two runs of B2-9 on the *same* machine minutes apart
measured `cpu_us_per_row` at 161.2 and 75.4 - a 2.1x swing from background load alone. A committed
threshold would have been either permanently useless or a false alarm, while the run-local diff is
meaningful in both runs.

A fixed cloud instance (the ClickBench approach) would only help if you wanted absolute numbers
comparable across months, and even then it would stabilise the small term: for the paid half the
dominant variance is the endpoint, not the host. ClickBench can pin a host because its measurement is
entirely local; this one is not.

Staleness is handled differently for the two halves:

- `laziness.json` and the integer keys of `arch.json` are gated by `test_structural.py`, which is cheap
  enough to run on every AI-touching PR (README §9). A drifted baseline shows up as a failed check on the
  PR that drifted it, so it cannot rot quietly.
- The timing keys are better as a **CIDB time series** than as a committed file: the weekly
  `test_latency_arch.py` run ingests them via a `post_hook`, the way `ingest_keeper_metrics.py` does, and
  a regression is found by comparing against history rather than against a number somebody blessed once.

Until that ingestion exists, each baseline file records the git SHA it was generated on and a run whose
SHA is further than a configurable distance prints a staleness warning, so a stale baseline is visible
rather than silently re-blessed.

## 9. Expected findings on the first run

Recorded so they are not mistaken for suite bugs:

- **B2-4's baseline is whatever B1 measures**, quite possibly `max_in_flight = 1`. Either result is a
  valid baseline; the point is that it becomes pinned.
- **B2-6 is expected to fail.** The row loop has no cancellation checkpoint
  (`FunctionBaseAI.cpp:517`), so 4 rows at `D = 15 s` should be uninterruptible for ~60 s. Land it as a
  **non-strict** `xfail` (a timing-budget assertion can flip, and `strict=True` would turn an
  unexpected pass into an error) with a link to the tracking issue, filed with the suite.
- **The laziness table was measured, and it overturned the prediction this section used to carry.**
  On `master` at `529df9d151c`:

  | Scenario | current | ideal | |
  |---|--:|--:|---|
  | L1 `WHERE id % 8 = 0` | 32 | 32 | already lazy |
  | L2 `LIMIT 5` | 5 | 5 | already lazy |
  | L3 `ORDER BY id LIMIT 5` | 5 | 5 | already lazy |
  | L4 AI predicate in `AND` | 32 | 32 | AI predicate evaluated last |
  | L5 short-circuit `if` | 32 | 32 | already lazy |
  | L10 `PREWHERE` | 32 | 32 | already lazy |
  | L7 `DISTINCT` control | 4 | 4 | — |
  | L8 CSE via `WITH` | 256 | 256 | CSE works |
  | L9e CSE across `WHERE` and `SELECT` | 256 | 256 | CSE works |
  | **L6 dedup, 4 distinct values in 256 rows** | **256** | **16** | **the only gap** |

  The planner is **already lazy** for filters, `LIMIT`, `PREWHERE`, short-circuit evaluation and
  common subexpressions: `LIMIT 5` issues 5 AI calls, not one per row of the first block. For those
  shapes the "AI-aware optimizer" the original request imagined is already built, and the table's
  job becomes protecting it rather than motivating it.

  The one real gap is **memoization of identical inputs**: L6 spends 256 calls where 16 would do, a
  16x waste on duplicate-heavy data. It is legal to fix for the embedding functions, which declare
  `isDeterministic() = true` (`aiEmbed.cpp:87`), and structurally impossible for the chat functions,
  which do not. L7 is the control — the same data through `SELECT DISTINCT` first costs 4 calls.

## 10. First measured run

`master` @ `529df9d151c`, 32-core host, mock `D = 200 ms`, 32 rows.

**Cross-stream parallelism exists, but only with multiple parts, and it is weak.**

| Config | calls | ms | max in-flight | C_eff |
|---|--:|--:|--:|--:|
| `max_threads=1`, 1 part | 32 | 7675 | 1 | 0.83 |
| `max_threads=8`, **1 part** | 32 | 7677 | **1** | 0.83 |
| `max_threads=8`, **8 parts** | 32 | **4979** | **3** | **1.29** |

`max_threads` alone changes nothing: one part is one stream, so execution is strictly serial. Only a
multi-part table overlaps requests, and then just three of them despite eight threads and 32 cores.
Block size (8 vs 32) is irrelevant. So the "one request per row, serial" model in section 1 is exactly
right for a single-part table - the state every table reaches after a merge - and a freshly-inserted
multi-part table gets a modest speedup by accident. `baselines/arch.json` pins
`max_in_flight_8t = 3`, `effective_concurrency_8t = 1.29`, `cpu_us_per_row = 161.2`.

`C_eff = 0.83` in the serial case means the query takes ~20% *longer* than `rows x D`, i.e. ~40 ms per
row of overhead - consistent with the mock's own ~33 ms per request, and the reason M4 uses CPU
counters rather than wall-clock.

**Against the real endpoint** (`claude-haiku-4-5`, `qwen3-embedding-8b`, `n = 10` singles, 32-row
batches), the same shape appears with a measured `D`:

| | p50 | p95 | | batch | calls | s/row | rows/s | C_eff |
|---|--:|--:|---|---|--:|--:|--:|--:|
| aiGenerate | 496 ms | 515 ms | | aiGenerate | 32 | 0.573 | 1.74 | **0.86** |
| aiClassify | 624 ms | 686 ms | | aiEmbed | **1** | 0.022 | 46.31 | **7.76** |
| aiExtract | 817 ms | 1164 ms | | | | | | |
| aiTranslate | 596 ms | 907 ms | | | | | | |
| aiEmbed | 168 ms | 216 ms | | | | | | |
| aiSimilarity | 274 ms | 366 ms | | | | | | |

Chat batch throughput is `single-call latency x rows` to within 15%. Embed reaches 7.76 effective
concurrency on the same endpoint purely by packing 32 texts into one request, making it **27x faster
per row than chat**. That is the whole argument for chat batching or request parallelism, measured
rather than asserted.

**`KILL QUERY` cannot interrupt the row loop** - B2-6 xfailed as predicted.

## 11. How a PR author uses this

```bash
# 1. before the change (mock only, free, ~4 minutes)
git checkout master && ninja -C ../ch-build clickhouse
python -m ci.praktika run "integration" \
  --test "test_e2e_ai_functions/test_latency_arch.py" --path ../ch-build/programs/clickhouse
cp tmp/ai_e2e_latency_arch.json tmp/before.json

# 2. after the change, comparing against it
git checkout my-branch && ninja -C ../ch-build clickhouse
echo "AI_E2E_COMPARE_TO=$PWD/tmp/before.json" >> ci/local.env
python -m ci.praktika run "integration" \
  --test "test_e2e_ai_functions/test_latency_arch.py" --path ../ch-build/programs/clickhouse
```

`AI_E2E_COMPARE_TO` takes the JSON, not a rendered table, and the before/after markdown is generated
from it. The path must be absolute: pytest runs with cwd `tests/integration/`. Configuration goes
through `ci/local.env`, because the job runs inside a container that inherits nothing else
(README section 5).

The structural counts need no before/after: they are committed, so one run on any machine says whether
they moved.

The generated block goes into the PR description:

```
## Suite B — architecture latency (mock, D=200ms, 64 rows, scale=1)
| metric                          | before | after | Δ      |
|---------------------------------|-------:|------:|-------:|
| T, max_threads=1 (ms)           |  13011 | 13024 |  +0.1% |
| T, max_threads=8, 8 parts (ms)  |  13008 |  1702 | -86.9% |
| max in-flight, 8 threads        |      1 |     8 |    +7  |
| effective concurrency           |    1.0 |   7.6 |  +6.6  |
| AIAPICalls (must not change)    |     64 |    64 |      0 |
| requests per connection         |     64 |     8 |   -56  |
| CPU µs/row (D=0, median of 5)   |    412 |   438 |  +6.3% |
| laziness mean (ideal/current)   |   0.28 |  0.28 |      0 |
| L1 time at D=200ms (ms)         |  51200 | 51190 |   0.0% |
```

`AIAPICalls` unchanged next to a large `T` drop is the signature of a genuine parallelism win rather
than accidentally skipped work. That pairing is why M1 is asserted in the same test as M3. The
`requests per connection` drop in this example is the expected cost of parallelism — worth seeing, not
a regression.
