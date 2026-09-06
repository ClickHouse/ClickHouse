# PromQL-to-SQL lowering notes

This is a development note for maintainers working on ClickHouse's PromQL lowering for `TimeSeries` tables. It documents the current lowering model and measured optimization patterns for changes to this converter.

The measurements cited below came from an external PromQL-to-ClickHouse-SQL transpiler that emitted SQL and executed it in ClickHouse. They are evidence that these physical SQL patterns can improve ClickHouse execution, not a substitute for in-tree validation. When changing this converter, use the patterns as design references and validate the emitted SQL with PromQL tests plus `EXPLAIN` or `system.query_log`.

## Current lowering model

PromQL requests enter ClickHouse through the Prometheus HTTP API or the `prometheusQuery` table function. The query is parsed into `PrometheusQueryTree`, then lowered to ClickHouse SQL AST by this directory.

Main concepts in this lowering path:

- `NodeEvaluationRangeGetter` computes each PromQL node's evaluation `start_time`, `end_time`, `step`, and lookback/range `window`. It accounts for query-range parameters, instant queries, offsets, `@` modifiers, range selectors, and subquery step alignment.
- `fromSelector.cpp` turns instant and range selectors into bounded `timeSeriesSelector(...)` reads. Selector reads are already time-bounded by the node evaluation range and lookback/window.
- `SQLQueryPiece` carries the lowered expression, result type, storage shape, time grid, scalar values, and metric-name state between lowering steps.
- `SelectQueryBuilder` creates the SQL AST fragments. Lowering code should prefer explicit AST construction over stringly SQL where practical.
- `timeSeries*ToGrid` aggregate functions are the main bridge between raw samples and PromQL's per-evaluation-step vector or matrix results.
- `finalizeSQL.cpp` converts the final `SQLQueryPiece` into the table-function result shape expected by SQL and HTTP API callers.

Correct PromQL semantics are the first constraint. A physical optimization is only safe when it preserves staleness/lookback behavior, label-set identity, metric-name dropping rules, duplicate-labelset errors, scalar/vector cardinality behavior, `NaN` handling, and subquery alignment.

## Measured optimization patterns for PromQL lowering

Each subsection describes a baseline physical pattern, a measured-better or explored physical pattern, the external-transpiler result, and what to validate when this converter implements, extends, or refactors that pattern. Some patterns are already used by current code paths and some remain candidates for future work; check the source before making an implementation or performance claim. Percentages are p50 latency or `system.query_log` counter deltas from targeted external-transpiler runs.

### Use native range-grid aggregates instead of row-expanded windows

Targets PromQL range-function expressions such as:

```promql
rate(http_requests_total[5m])
sum by (job) (rate(http_requests_total[5m]))
```

These expressions start from samples in a lookback window for every evaluation timestamp.

Baseline pseudocode:

```sql
SELECT
    series_id,
    eval_ts,
    rateFromSamples(groupArray((sample_ts, value))) AS value
FROM samples
INNER JOIN evaluation_timestamps
    ON sample_ts > eval_ts - lookback AND sample_ts <= eval_ts
GROUP BY series_id, eval_ts
```

Measured-better pseudocode:

```sql
SELECT
    series_id,
    arrayJoin(timeSeriesRateToGrid(start_ts, end_ts, step, lookback, sample_ts, value)) AS point
FROM samples
GROUP BY series_id
```

Measured result from the external transpiler:

- For `query_range` over one day with 5-minute steps, `rate(http_requests_total[5m])` p50 improved by `47.9%` and `50.2%` in two measured runs.
- For `query_range` over seven days with 1-hour steps, `sum by (job) (rate(http_requests_total[1h]))` p50 improved by `56.5%` and `58.2%`.

In-tree ClickHouse validation: range-function tests for lookback boundaries, staleness, counter resets, offsets, and subquery alignment; `EXPLAIN` or `system.query_log` evidence showing less expression work, memory, or pipeline work for the generated SQL.

### Cancel exact repeated-vector averages and unit fractions

Targets PromQL binary expressions that repeat the same vector operand inside an exact average or unit-fraction expression:

```promql
(rate(http_requests_total[5m]) + rate(http_requests_total[5m])) / 2
(rate(http_requests_total[5m]) + rate(http_requests_total[5m]) + rate(http_requests_total[5m])) * (1 / 3)
```

Baseline pseudocode:

```sql
WITH
    left_vector AS lower(rate(http_requests_total[5m])),
    right_vector AS lower(rate(http_requests_total[5m]))
SELECT
    match_labels,
    (left_vector.value + right_vector.value) / 2 AS value
FROM left_vector
INNER JOIN right_vector USING match_key
```

Measured-better pseudocode:

```sql
WITH vector AS lower(rate(http_requests_total[5m]))
SELECT
    labels,
    value AS value
FROM vector
```

This target is safe only when the duplicated expression has the same label-set and metric-name behavior that PromQL would produce after binary matching and the arithmetic is an exact average or unit fraction. Do not generalize this evidence to arbitrary rewrites such as `x + x -> x * 2`; that explored form improved latency but did not reduce the work counters.

Measured result from the external transpiler:

- Repeated-expression cancellation reduced `FunctionExecute/query` by about `26%` to `40%` for exact-average expressions like the examples above.
- The same expressions had p50 improvements of about `55%` to `89%`.
- Some repeated-expression variants also reduced `SelectedRows` or `ReadCompressedBytes` by about `33%` to `37.5%` after the duplicated scan work disappeared.

In-tree ClickHouse validation: tests for `on`/`ignoring`, bool comparisons, duplicate-labelset errors, metric-name dropping, and set-operator presence semantics; `system.query_log` evidence such as lower `FunctionExecute`, `SelectedRows`, or `ReadCompressedBytes` when the duplicate work is removed.

### Aggregate directly over selector windows when the function is exact

Targets PromQL range-function expressions where the function can be computed exactly from the raw selector window:

```promql
rate(http_requests_total[5m])
increase(http_requests_total[5m])
```

Some range functions can be computed from the raw selector window without first materializing a full per-step vector grid.

Baseline pseudocode:

```sql
WITH vector_grid AS lower(selector[window])
SELECT
    labels,
    eval_ts,
    aggregateWindow(vector_grid.points_for_eval_ts) AS value
FROM vector_grid
GROUP BY labels, eval_ts
```

Measured-better pseudocode:

```sql
SELECT
    labels,
    eval_ts,
    aggregateWindow(sample_ts, value) AS value
FROM bounded_selector_samples
GROUP BY labels, eval_ts
```

Measured result from the external transpiler:

- Direct selector-window aggregate shapes reduced `FunctionExecute/query` by about `13%` to `21%` for `rate(...)` expressions like the examples above.
- p50 improved by about `80%`.
- `SelectedRows` and `SelectedBytes` moved by about `11%` in the measured accepted shape.

In-tree ClickHouse validation: exact boundary tests for inclusive/exclusive window edges, `@` modifiers, offsets, sparse samples, stale markers, and one-sample windows; query-log counters showing the generated SQL does less scan or expression work.

### Use direct long-window `rate` aggregate kernels

Targets PromQL instant queries that compute `rate` over a long enough window, including histogram inputs built from bucket rates:

```promql
rate(http_requests_total[1h])
histogram_quantile(0.9, sum by (le) (rate(http_request_duration_seconds_bucket[1h])))
```

Baseline pseudocode:

```sql
WITH vector_grid AS lower(rate(metric[lookback]))
SELECT labels, value_at_eval_ts
FROM vector_grid
```

Measured-better pseudocode:

```sql
SELECT
    labels,
    directRateAggregate(sample_ts, value) AS value
FROM bounded_selector_samples
GROUP BY labels
```

The measured-better target uses a direct aggregate kernel for long-window `rate` instead of building the generic range grid first.

Measured result from the external transpiler:

- A guarded direct-rate aggregate improved instant `rate(http_requests_total[1h])` p50 by about `14%`.
- Histogram-quantile inputs such as `histogram_quantile(0.9, sum by (le) (rate(http_request_duration_seconds_bucket[1h])))` improved p50 by `12.3%` and `11.7%` in two measured cases.
- `FunctionExecute/query` moved only `0.5%` to `1.5%` on those histogram expressions, so this was mainly a latency improvement in the external measurements.

In-tree ClickHouse validation: tests for counter resets, one-sample windows, sparse windows, stale markers, and especially short windows. The external attempt had to disable the direct shortcut for `rate(...[15s])` before re-enabling it behind a long-window guard.

### Match latest samples with an ASOF-style staleness shape

Targets PromQL instant-vector expressions and aggregations over instant vectors:

```promql
http_requests_total{job="api"}
sum by (job) (http_requests_total)
```

Instant-vector lowering needs the latest sample at or before each evaluation timestamp, then must reject stale markers according to Prometheus rules.

Baseline pseudocode:

```sql
SELECT
    labels,
    eval_ts,
    argMax(value, sample_ts) AS value
FROM samples
INNER JOIN evaluation_timestamps
    ON sample_ts <= eval_ts AND sample_ts >= eval_ts - lookback
GROUP BY labels, eval_ts
HAVING NOT isStaleNaN(value)
```

Measured-better pseudocode:

```sql
SELECT
    labels,
    eval_ts,
    value
FROM evaluation_timestamps
ASOF JOIN samples_by_series
    ON samples_by_series.sample_ts <= evaluation_timestamps.eval_ts
WHERE sample_ts >= eval_ts - lookback
  AND NOT isStaleNaN(value)
```

Measured result from the external transpiler:

- ASOF-style staleness matching reduced `FunctionExecute/query` by `19.2%`.
- p50 improved by `94.0%` and `94.6%` in two measured runs.
- `SelectedRows` moved by `19%`; `ReadCompressedBytes` moved by `15%`.

In-tree ClickHouse validation: staleness and lookback tests for selectors, offsets, range endpoints, `NaN` values, and empty series; `EXPLAIN` and query-log counters showing the intended join and reduced work.

### Coalesce classic histogram buckets before quantile interpolation

Targets PromQL classic-histogram quantile expressions over `_bucket` float series distinguished by an `le` label:

```promql
histogram_quantile(0.9, sum by (job, le) (rate(http_request_duration_seconds_bucket[5m])))
```

Baseline pseudocode:

```sql
SELECT
    job,
    eval_ts,
    prometheusHistogramQuantile(
        0.9,
        groupArray((toFloat64(le), value))
    ) AS value
FROM expanded_bucket_rows
GROUP BY job, eval_ts
```

Measured-better pseudocode:

```sql
WITH bucket_rows AS
(
    SELECT
        job,
        eval_ts,
        toFloat64(le) AS upper_bound,
        sum(value) AS bucket_value
    FROM bucket_samples
    GROUP BY job, eval_ts, upper_bound
)
SELECT
    job,
    eval_ts,
    prometheusHistogramQuantile(0.9, groupArray((upper_bound, bucket_value))) AS value
FROM bucket_rows
GROUP BY job, eval_ts
```

The measured-better target parses and groups bucket bounds once per output label set and timestamp, then runs Prometheus-compatible interpolation over the coalesced buckets.

Measured result from the external transpiler:

- Direct histogram coalescing improved p50 by about `14.6%` to `19.7%` on histogram-quantile expressions like the example above.
- `FunctionExecute/query` moved by about `0.4%` to `1.9%` in those runs, so the observed win was mostly latency/shape rather than a large total function-count reduction.

In-tree ClickHouse validation: tests for missing or invalid `le`, duplicate bucket bounds, `+Inf`, zero-count buckets, monotonicity correction, grouping labels, and metric-name dropping; generated SQL and query-log counters for bucket parsing, grouping, or function work.

### Sort timestamp/value tuples directly for range windows

Targets PromQL range functions whose per-series calculation needs samples ordered by timestamp:

```promql
rate(http_requests_total[5m])
delta(memory_usage_bytes[1h])
```

Range functions often sort samples inside each series/window before applying the PromQL calculation.

Baseline pseudocode:

```sql
SELECT
    series_id,
    arraySort(groupArray(sample_ts)) AS timestamps,
    reorderValuesBySortedTimestamps(groupArray(value), timestamps) AS values
FROM samples
GROUP BY series_id
```

Measured-better pseudocode:

```sql
SELECT
    series_id,
    arraySort(groupArray((sample_ts, value))) AS points
FROM samples
GROUP BY series_id
```

The measured-better target sorts aligned `(timestamp, value)` tuples once, then reads tuple elements from the sorted array.

Measured result from the external transpiler:

- Tuple-native sorting for a range-rate path reduced `FunctionExecute/query` by `11.67%` and `11.63%` in two measured runs.
- p50 improved by `1.9%` and `1.4%`.

In-tree ClickHouse validation: range-function correctness tests for duplicate timestamps, sparse windows, and counter resets; `system.query_log` counters showing lower function work or sort/array work for the generated SQL.

## Explored patterns with weak or mixed external-transpiler measurements

These patterns did not show a clear positive result in generated SQL, but they may still be useful inside ClickHouse if the in-tree converter can express them with less overhead or at a lower level than generated SQL can.

### Reuse one range source instead of scanning the same selector twice

Targets PromQL expressions that repeat the same range-function input in multiple operands:

```promql
rate(http_requests_total[5m]) + rate(http_requests_total[5m])
rate(http_requests_total[5m]) >= bool rate(http_requests_total[5m])
```

Repeated range expressions can sometimes be evaluated from one raw source and reused for both operands.

Baseline pseudocode:

```sql
WITH
    left_range AS lower(rate(metric[5m])),
    right_range AS lower(rate(metric[5m]))
SELECT combine(left_range, right_range)
```

Explored pseudocode:

```sql
WITH range_source AS readSamples(metric, required_time_range)
SELECT combine(
    evaluateRate(range_source),
    evaluateRate(range_source))
```

External measurement result:

- Early normalized range self-reuse attempts reduced `SelectedRows` and `SelectedBytes` by about `5%` to `7%`, but `FunctionExecute/query` did not improve enough to accept the generated-SQL change.
- A later one-source self-join form improved p50 by about `51%`, but `FunctionExecute/query` regressed and a separate `query_range` expression for `rate(http_requests_total[5m])` regressed by about `12%`.

In-tree ClickHouse validation: duplicate-labelset and vector-matching tests, plus query-log counters proving the reuse does not add more join or function work than it removes.

### Aggregate grouped range selectors directly

Targets PromQL aggregations over range-function results:

```promql
sum by (job) (rate(http_requests_total[5m]))
avg by (instance) (avg_over_time(memory_usage_bytes[1h]))
```

Grouped range queries may look cheaper if they aggregate raw selector rows before building the final vector grid.

Baseline pseudocode:

```sql
WITH vector_grid AS lower(rate(metric[5m]))
SELECT labels, eval_ts, sum(value)
FROM vector_grid
GROUP BY labels, eval_ts
```

Explored pseudocode:

```sql
SELECT grouping_labels, eval_ts, aggregateWindow(sample_ts, value)
FROM bounded_selector_samples
GROUP BY grouping_labels, eval_ts
```

External measurement result:

- A direct raw-sample grouped range selector attempt improved p50 by `6.0%` and `7.6%` in two measured runs.
- `FunctionExecute/query` slightly regressed by `0.61%` and `0.48%`, and `ReadCompressedBytes` did not move clearly.

In-tree ClickHouse validation: grouping-label, metric-name dropping, stale-marker, and window-boundary tests; query-log evidence showing whether the in-tree shape reduces scan, memory, or expression work.

### Bucket non-overlapping range windows explicitly

Targets PromQL range queries where the query-range `step` is greater than or equal to the range selector window:

```promql
rate(http_requests_total[5m])      # query_range step >= 5m
avg_over_time(memory_usage_bytes[1h])  # query_range step >= 1h
```

When evaluation windows do not overlap, samples can be assigned to one output bucket without the generic window join shape.

Baseline pseudocode:

```sql
SELECT series_id, eval_ts, aggregateWindow(samples)
FROM samples
INNER JOIN evaluation_timestamps
    ON sample_ts > eval_ts - lookback AND sample_ts <= eval_ts
GROUP BY series_id, eval_ts
```

Explored pseudocode:

```sql
SELECT
    series_id,
    bucketForTimestamp(sample_ts, start_ts, step) AS eval_ts,
    aggregateWindow(sample_ts, value)
FROM samples
WHERE lookback <= step
GROUP BY series_id, eval_ts
```

External measurement result:

- Guarded non-overlap bucketization reduced `FunctionExecute/query` by about `2.0%` to `3.6%`.
- p50 moved only within about `1.2%`, which was treated as noise in the external transpiler.

In-tree ClickHouse validation: tests for step/window equality, offset, `@`, range start/end inclusivity, and sparse windows; `EXPLAIN` or query-log counters showing the bucketed form does less work than the generic join form.

## How to use this evidence in this converter

Use the measured patterns as design and review references, not as SQL to paste into this codebase.

- The external transpiler emits SQL strings. This converter builds ClickHouse SQL AST while carrying `SQLQueryPiece` result type, storage shape, time-grid, label, scalar, and metric-name state. The same physical idea needs an in-tree representation, not a pasted SQL template.
- Each pattern touches high-risk PromQL behavior: lookback and staleness, duplicate label-set errors, vector matching, counter resets, subquery alignment, or histogram bucket semantics. Keep each rewrite scoped enough that the semantic tests explain why it is safe.
- The percentages above came from SQL generated outside this converter. Claim an in-tree win only after this converter emits the intended SQL and `EXPLAIN` or `system.query_log` shows the expected ClickHouse-side change.
- Some patterns need dedicated helper functions or `timeSeries*ToGrid` variants before the converter can express the measured-better target cleanly.

When extending an implemented pattern or adding a missing one, keep the change scoped to that pattern and its semantic tests, then add the ClickHouse-side evidence for the generated SQL.

## Optimization principles for this converter

- Start from the PromQL semantic invariant, then choose the physical SQL shape.
- Reuse existing range/window logic. Special function paths that read raw selectors should not duplicate off-by-one, lookback, offset, `@`, or subquery alignment rules unless they also own tests for those boundaries.
- Keep unsupported or uncertain shapes visible. Do not silently lower an unsupported semantic case to a faster approximate shape.
- Make physical choices reviewable in plan structs, helper functions, or localized lowering code before changing broad renderer behavior.
- Treat shorter SQL as insufficient evidence. If ClickHouse rewrites two SQL shapes to the same `EXPLAIN SYNTAX` or reads the same rows with the same ProfileEvents, the optimization is probably cosmetic.
- Keep correctness and optimization commits separable when that improves review and rollback.

## Validation checklist for optimization changes

Before describing a PromQL lowering change as an optimization, collect evidence that matches the claim:

- Correctness: focused integration tests and relevant compliance coverage still pass.
- Shape: generated SQL/AST or `EXPLAIN` output shows the intended physical change.
- Storage pruning: `SelectedRows`, `SelectedBytes`, marks, or read bytes move when the claim is reduced scanning.
- Expression work: `FunctionExecute`, array/sort counters, or pipeline stages move when the claim is less computation.
- Memory: query-log memory or `MemoryTrackerUsage` moves when the claim is lower peak memory.
- Latency: report benchmark conditions and caveats; do not rely on small wall-clock deltas without lower-variance counters.
