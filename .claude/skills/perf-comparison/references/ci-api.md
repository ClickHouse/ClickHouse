# performance.ci API reference for the skill

Base URL:

```bash
BASE="https://performance.ci.clickhouse.com/api/v1"
```

## Run discovery

Find runs for a PR:

```bash
PR=104350
curl -fsS "$BASE/runs?q=$PR" | jq .
```

Important fields in `items[]`:

- `identity.runId`
- `identity.prNumber`
- `identity.oldSha`
- `identity.newSha`
- `identity.runTime`
- `arches`
- `changedQueries`
- `slowdownQueries`
- `speedupQueries`
- `unstableQueries`

Always filter by `identity.prNumber == PR`; search can return more than intended.


Caveats observed during testing:

- `unstableQueries[]` can contain high-noise/high-threshold rows even when the summary card says `Unstable queries: 0`. Treat them as flakiness/noise context, not as changed rows unless they are also over threshold.
- `totals.intersectingFiles` may be omitted when `fileSet=intersecting` has no files; use `len(files)` as the zero-intersection fallback.
- `selectedRunPoint.value` in trend responses can be `0` as a marker; use query detail old/new values for the actual PR measurement.
- Flamegraph endpoints may return 404 even when a query is changed; absence of flamegraph data is not evidence either way.

## Master status counts through ClickHouse Play

Use this when you need 30-day master status counts for every changed row: how often the same test/query appeared `slower`, `faster`, or `unstable` on master. The dashboard history API is richer for charts/trends; `default.checks` gives direct status counts.

Helper:

```bash
python3 scripts/perf_api.py master-checks --pr "$PR" --limit 50
```

Equivalent HTTP query pattern:

```bash
curl -fsS 'https://play.clickhouse.com/?user=explorer' --data-binary @- <<'SQL'
SELECT
    replaceRegexpOne(test_name, '::(new|old)$', '') AS test,
    countIf(test_status = 'slower') AS slower_count,
    countIf(test_status = 'faster') AS faster_count,
    countIf(test_status = 'unstable') AS unstable_count,
    count() AS total_runs
FROM default.checks
WHERE pull_request_number = 0
  AND check_name LIKE '%Performance%arm%'
  AND check_start_time >= now() - INTERVAL 30 DAY
  AND test_name IN ('fixed_hash_table_parallel_merge #1::new')
GROUP BY test
FORMAT JSONEachRow
SQL
```

Use these counts for classifications: `new in PR`, `rarely on master`, `flaky/slower on master`, `unstable on master`, `new improvement`, or `fixes known master regression`.

## TSV/raw artifact inventory

When available, raw `all-query-metrics.tsv` / `fetch_perf_report.py --tsv` output gives per-shard rows with:

- `old`/`new` (`left`/`right` in raw files),
- `diff`,
- `times_change`,
- `stat_threshold`,
- `test`,
- `query_index`,
- query display text,
- and, in `fetch_perf_report.py --tsv`, `arch`, `shard`, `is_changed`, `is_unstable`, `direction`.

Use this helper only when dashboard/API cannot provide required artifact-level data. It supports both named TSV output from `fetch_perf_report.py --tsv` and headerless raw `all-query-metrics.tsv` rows with the current CI column order.

```bash
python3 scripts/perf_api.py tsv-inventory --tsv pr_${PR}_amd.tsv pr_${PR}_arm.tsv --limit 20
```

TSV rows are artifact fallback evidence; do not use them to override dashboard classification.

## Artifact/log fallback

Use artifacts when dashboard/API cannot provide root-cause data:

- `logs.tar.zst` / `job.log.zst`: job context, server configs, warnings/errors, machine contention.
- `left/server.log` and `right/server.log`: exact old/new binary revision, build ID, startup settings, query execution timing, background activity.
- `left-trace-log.tsv` / `right-trace-log.tsv`: raw `system.trace_log` samples by query id for CPU/Real/Memory stacks.
- `analyze/tmp/{test}_{queryN}.tsv`: per-run raw profile events, useful when dashboard only shows summary metrics.
- prebuilt `.svg` flamegraphs: useful if the dashboard flamegraph-diff endpoint is missing or incomplete.

Prefer dashboard flamegraph/query APIs first. Fall back to artifacts when you need raw logs, raw traces, exact build metadata, or a query/run that dashboard did not profile.

## Run overview

```bash
curl -fsS "$BASE/runs/$RUN_ID?metrics=client_time,real_time,cpu_time,memory" | jq .
```

Use:

- `slowdowns[]`
- `speedups[]`
- `unstableQueries[]`
- `testSummaries[]`
- `assets[]`
- `reports[]`

`ComparisonRow` fields:

- `test`
- `queryIndex`
- `queryDisplayName`
- `metric`
- `arch`
- `oldValue`
- `newValue`
- `diffPercent`
- `statThreshold`
- `direction`
- `severity`
- `confidence`

Note: current API represents `diffPercent` and `statThreshold` as fractions in observed examples (`0.232` means about `+23.2%`). Display both safely if uncertain.

## Confidence

Run-level confidence:

```bash
curl -fsS "$BASE/runs/$RUN_ID/confidence?metrics=client_time,real_time,cpu_time,memory" | jq .
```

Test-level confidence:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/confidence?metrics=client_time,real_time,cpu_time,memory" | jq .
```

Use confidence as human-readable evidence, not raw module codes:

- Always report `confidence.tier` and `confidence.reason` when present.
- For module details, use `modules[].title`, `modules[].status`, `modules[].interpretation`, and useful `modules[].rows[]` facts.
- Do **not** write unexplained strings like `M1:downgrade, M2:neutral`; that is dashboard-internal shorthand, not a reviewer-facing conclusion.
- Be careful with speedups: current tier names/reasons can be slowdown-oriented. Do not print `confirmed_regression` for a speedup row; call it a stable speedup/change only if the surrounding evidence supports that.
- Example useful wording: `tier=noise; reason=change is smaller than recent-history adaptive threshold; History Adaptive Threshold downgraded because observed 47.2% < adaptive 94.8%.`

## Test and query detail

Test detail:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST?metrics=client_time,real_time,cpu_time,memory" | jq .
```

Query detail:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/queries/$QUERY_INDEX?metrics=client_time,real_time,cpu_time,memory" | jq .
```

Use query detail to collect:

- `queryText`
- all metric rows for the query
- `flamegraphAssets`
- report links

## Trend and history

Trend near selected run:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/trend?metric=$METRIC&queryIndex=$QUERY_INDEX&arch=$ARCH" | jq .
```

Longer history:

```bash
curl -fsS "$BASE/history/$TEST/$QUERY_INDEX?metric=$METRIC&arch=$ARCH" | jq .
```

Use:

- `points[]` / `centerTrend[]`: normal spread and selected run context.
- `changePoints[]`: known historical jumps.
- `periodComparisons[]`: large historical changes.

Required summary shape:

- state the time period covered by the points;
- show percentiles, not only min/max: p05/p25/p50/p75/p95;
- show a recent-window spread, e.g. last 30 points with its own time span;
- compare selected old/new values to p50 and p95;
- de-duplicate change points and list date/direction/sha for the latest relevant entries.

Interpretation:

- selected run far outside recent p95 + repeated PR runs: stronger evidence.
- selected run inside recent spread: likely noise/unstable.
- known recent master change point: PR may be inheriting master movement, not causing it.

## Coverage / PR overlap

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/coverage?fileSet=intersecting" | jq .
```

Other useful `fileSet` values:

```bash
fileSet=pr
fileSet=covered
fileSet=all
```

Use:

- `coverageAvailable`
- `message`
- `totals.touchedFiles`
- `totals.coveredFiles`
- `totals.intersectingFiles`
- `files[].path`
- `files[].status`
- `files[].patch`
- `files[].coverageRanges`

Do not require coverage to prove causality. Use it to decide if a PR/test relation is plausible.

## Flamegraphs

Reviewer-facing links should point to the UI query page, which contains the Flamegraphs card:

```text
https://performance.ci.clickhouse.com/runs/$RUN_ID/tests/$TEST/queries/$QUERY_INDEX
```

Use raw APIs only when you need machine-readable stacks/deltas.

Collapsed stacks for one side:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/queries/$QUERY_INDEX/flamegraph?metric=$METRIC&arch=$ARCH&side=candidate&traceType=CPU" | jq -r .collapsed
```

Differential flamegraph data:

```bash
curl -fsS "$BASE/runs/$RUN_ID/tests/$TEST/queries/$QUERY_INDEX/flamegraph-diff?metric=$METRIC&arch=$ARCH&traceType=CPU" | jq .
```

Trace types:

- `CPU`: compute time.
- `REAL_TIME`: wall-clock time; useful for waits, locks, I/O, scheduler noise.
- `MEMORY`: memory-related samples if available.

Flamegraph diff fields:

- `stack`
- `baselineSamples`
- `candidateSamples`

Suggested summary:

| Leaf/subsystem | Baseline samples | Candidate samples | Delta | Interpretation |
|---|---:|---:|---:|---|

Do not claim root cause solely because a frame has samples. It must match the metric/query and PR change plausibility.

Reviewer-facing flamegraph evidence must include:

- exact UI query-page link first;
- raw API link only as backup if useful;
- top sample deltas in a table;
- a short interpretation tied to the query, or `no frames returned` / `not available` if absent.
