# PromQL-to-SQL lowering notes

This is a development note for maintainers working on ClickHouse's PromQL lowering for `TimeSeries` tables. Some items below may be implemented in this PR, in later PRs, or removed once they are no longer useful. The goal is to keep reviewable thoughts near the code while the PromQL surface is still expanding, not to define a permanent optimization roadmap.

## Current lowering model

PromQL requests enter ClickHouse through the Prometheus HTTP API or the `prometheusQuery` table function. The query is parsed into `PrometheusQueryTree`, then lowered to ClickHouse SQL AST by this directory.

Main concepts in this lowering path:

- `NodeEvaluationRangeGetter` computes each PromQL node's evaluation `start_time`, `end_time`, `step`, and lookback/range `window`. It accounts for query-range parameters, instant queries, offsets, `@` modifiers, range selectors, and subquery step alignment.
- `fromSelector.cpp` turns instant and range selectors into bounded `timeSeriesSelector(...)` reads. Selector reads are already time-bounded by the node evaluation range and lookback/window.
- `SQLQueryPiece` carries the lowered expression, result type, storage shape, time grid, scalar values, and metric-name state between lowering steps.
- `SelectQueryBuilder` creates the SQL AST fragments. Lowering code should prefer explicit AST construction over stringly SQL where practical.
- `timeSeries*ToGrid` aggregate functions are the main bridge between raw samples and PromQL's per-evaluation-step vector or matrix results.
- `finalizeSQL.cpp` converts the final `SQLQueryPiece` into the table-function result shape expected by SQL and HTTP API callers.

Correct PromQL semantics are the first constraint. A physical optimization is only safe when it preserves staleness/lookback behavior, label-set identity, metric-name dropping rules, duplicate-labelset errors, scalar/vector cardinality behavior, NaN handling, and subquery alignment.

## Optimization principles for this converter

- Prefer deterministic, query-family-specific physical shapes over a broad PromQL cost-based optimizer.
- Reuse existing range/window logic. Special function paths that read raw selectors should not duplicate off-by-one, lookback, offset, `@`, or subquery alignment rules unless they also own tests for those boundaries.
- Keep unsupported or uncertain shapes visible. Do not silently route an unsupported semantic case to a faster approximate shape.
- Make physical choices reviewable in plan structs, helper functions, or localized lowering code before changing broad renderer behavior.
- Treat shorter SQL as insufficient evidence. If ClickHouse rewrites two SQL shapes to the same `EXPLAIN SYNTAX` or reads the same rows with the same ProfileEvents, the optimization is probably cosmetic.
- Keep correctness and optimization commits separable when that improves review and rollback.

## Candidate follow-ups

| Area | ClickHouse direction | Semantic guardrail | Evidence before claiming a win |
| --- | --- | --- | --- |
| Selector-bound consistency for specialized lowerings | `fromSelector.cpp` already bounds `timeSeriesSelector(...)` reads. Special paths such as direct `timestamp(<instant-selector>)` should share or mirror that logic rather than inventing their own scan interval. | Include every raw sample PromQL could select for each evaluation timestamp, including lookback, range selectors, subqueries, offsets, `@`, and inclusive/exclusive boundaries. | Boundary tests plus generated SQL/AST showing the expected selector interval. If a change claims less scanning, show `SelectedRows`/`SelectedBytes` movement. |
| Range and subquery materialization | Prefer raw-data or streaming `timeSeries*ToGrid` aggregate shapes over building large vector grids when the aggregate is semantically exact. | Preserve per-step windows, staleness, extrapolation, subquery step alignment, and sparse output. | Range/subquery integration tests, `EXPLAIN PIPELINE` or SQL-shape evidence, memory/ProfileEvents on long-range fixtures. |
| Shared helper construction | Centralize repeated AST fragments for time grids, selector reads, group keys, scalar parameters, and label extraction where repeated construction risks divergence or duplicated executor work. | Helpers must not hide semantic differences between instant vectors, range vectors, scalar grids, and raw-data inputs. | Staged diff should make the shared invariant obvious. Runtime claims need `EXPLAIN SYNTAX` and ProfileEvents such as `FunctionExecute`. |
| Label and group reduction | Drop metric names and unused labels as soon as PromQL rules allow; avoid carrying full tag/group values through aggregation or joins when the final labels are already known. | Preserve output labels, duplicate-labelset detection, `by`/`without`, `on`/`ignoring`, and `group_left`/`group_right` behavior. | Label-focused tests plus reduced expression work or memory in ProfileEvents. |
| Vector matching joins | For arithmetic, comparison, and set operators, precompute match keys once, use existence-filter shapes where exact, and keep duplicate/cardinality checks deterministic. | Must preserve PromQL cardinality errors, bool comparison behavior, metric-name rules, and set-operator presence semantics. | Tests for vector matching modifiers and duplicate cases, plus join row counts or pipeline/ProfileEvents for performance claims. |
| Limit and top-k aggregations | Special-case empty-output parameters such as `k = 0`; prefer partial top-N shapes where exact instead of full sorts. | Preserve NaN ordering, tie behavior, grouping labels, and output order expectations where the API requires them. | Edge tests for `k`, NaN, and grouping; `EXPLAIN`/ProfileEvents showing less sort or array work. |
| Classic histogram quantile | Aggregate bucket rows early, avoid repeated `le` parsing/group reconstruction, and keep monotonicity correction in one helper path. | Preserve Prometheus bucket ordering, missing/invalid `le`, duplicate bucket, zero-count, and monotonicity edge behavior. | Histogram integration/compliance tests plus reduced grouping/parsing work in SQL or ProfileEvents. |
| Explainability | Where practical, expose the selected PromQL lowering shape and important rejection reasons through existing explain paths. | Explain output must not change normal query results or hide unsupported semantic cases. | Explain-output tests or examples tied to a query family. |

## Validation checklist for optimization PRs

Before describing a PromQL lowering change as an optimization, collect evidence that matches the claim:

- Correctness: focused integration tests and relevant compliance coverage still pass.
- Shape: generated SQL/AST or `EXPLAIN` output shows the intended physical change.
- Storage pruning: `SelectedRows`, `SelectedBytes`, marks, or read bytes move when the claim is reduced scanning.
- Expression work: `FunctionExecute`, array/sort counters, or pipeline stages move when the claim is less computation.
- Memory: query-log memory or `MemoryTrackerUsage` moves when the claim is lower peak memory.
- Latency: report benchmark conditions and caveats; do not rely on small wall-clock deltas without lower-variance counters.
