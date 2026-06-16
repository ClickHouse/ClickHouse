-- Tags: no-fasttest, no-parallel-replicas

-- Fused TopN aggregation (Mode 1, sorted-input early termination): correctness vs the standard
-- pipeline, and plan-shape checks for the gates. The cardinality gate reads column uniq
-- statistics, so keep statistics enabled regardless of any randomized test settings.
SET allow_experimental_statistics = 1, allow_statistics_optimize = 1, use_statistics = 1;

DROP TABLE IF EXISTS t_topn;
-- ORDER BY start_time makes start_time the first sorting-key column (the ORDER BY aggregate's
-- argument). Per group (trace_id = number % 1000) the max/min start_time is unique, so a
-- single-column ORDER BY on the aggregate is deterministic. service_name depends only on
-- trace_id, so it is constant within a group, keeping any(service_name) deterministic across
-- pipelines. auto_statistics_types = 'uniq' provides the NDV the gate needs (materialized on merge).
CREATE TABLE t_topn (trace_id String, start_time DateTime, service_name String, value UInt64)
ENGINE = MergeTree ORDER BY start_time SETTINGS auto_statistics_types = 'uniq';
INSERT INTO t_topn SELECT
    'trace_' || toString(number % 1000),
    toDateTime('2024-01-01') + number,
    'service_' || toString((number % 1000) % 5),
    number
FROM numbers(10000);
OPTIMIZE TABLE t_topn FINAL;

-- Correctness: the optimized result must equal the standard pipeline.
-- max as the ORDER BY aggregate, with any/argMax companions on the same argument.
SELECT '-- max DESC + any/argMax: optimized';
SELECT trace_id, max(start_time) AS m, any(service_name) AS s, argMax(value, start_time) AS v
FROM t_topn GROUP BY trace_id ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1;
SELECT '-- max DESC + any/argMax: reference';
SELECT trace_id, max(start_time) AS m, any(service_name) AS s, argMax(value, start_time) AS v
FROM t_topn GROUP BY trace_id ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 0;

-- min as the ORDER BY aggregate, with an argMin companion.
SELECT '-- min ASC + argMin: optimized';
SELECT trace_id, min(start_time) AS m, argMin(value, start_time) AS v
FROM t_topn GROUP BY trace_id ORDER BY m ASC LIMIT 5 SETTINGS optimize_topn_aggregation = 1;
SELECT '-- min ASC + argMin: reference';
SELECT trace_id, min(start_time) AS m, argMin(value, start_time) AS v
FROM t_topn GROUP BY trace_id ORDER BY m ASC LIMIT 5 SETTINGS optimize_topn_aggregation = 0;

-- Plan shape. Eligible query: Mode 1 activates, and the LimitStep is kept above it (so
-- rows_before_limit_at_least is still reported).
SELECT '-- eligible: TopNAggregating present, Limit kept';
SELECT countIf(explain LIKE '%TopNAggregating%') > 0, countIf(explain LIKE '%Limit%') > 0
FROM (EXPLAIN actions = 1 SELECT trace_id, max(start_time) AS m FROM t_topn GROUP BY trace_id ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1);

-- Cardinality gate: skipped when LIMIT is large relative to ndv (1000 groups, LIMIT 800 > ndv * 0.5),
-- or when the ratio is 0.
SELECT '-- large limit (ratio gate): not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT trace_id, max(start_time) AS m FROM t_topn GROUP BY trace_id ORDER BY m DESC LIMIT 800 SETTINGS optimize_topn_aggregation = 1) WHERE explain LIKE '%TopNAggregating%';
SELECT '-- max_ndv_ratio = 0: not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT trace_id, max(start_time) AS m FROM t_topn GROUP BY trace_id ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1, topn_aggregation_max_ndv_ratio = 0) WHERE explain LIKE '%TopNAggregating%';

-- Correctness gates that early termination would break: each enforces a limit the standard pipeline
-- applies but the fused path would skip, so Mode 1 must not activate.
--   max_rows_to_group_by (AggregatingStep must still throw at the group limit),
--   max_rows_to_sort (SortingStep must still throw at the sort limit),
--   always_read_till_end (exact_rows_before_limit).
SELECT '-- max_rows_to_group_by: not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT trace_id, max(start_time) AS m FROM t_topn GROUP BY trace_id ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1, max_rows_to_group_by = 100, group_by_overflow_mode = 'throw') WHERE explain LIKE '%TopNAggregating%';
SELECT '-- max_rows_to_sort: not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT trace_id, max(start_time) AS m FROM t_topn GROUP BY trace_id ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1, max_rows_to_sort = 1, sort_overflow_mode = 'throw') WHERE explain LIKE '%TopNAggregating%';
SELECT '-- exact_rows_before_limit (read till end): not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT trace_id, max(start_time) AS m FROM t_topn GROUP BY trace_id ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1, exact_rows_before_limit = 1) WHERE explain LIKE '%TopNAggregating%';

DROP TABLE t_topn;

-- LowCardinality GROUP BY key: must produce the same result (the transform must preserve the
-- LowCardinality output column rather than strip it). Regression guard for a prior SIGABRT.
DROP TABLE IF EXISTS t_topn_lc;
CREATE TABLE t_topn_lc (k LowCardinality(String), ts UInt32) ENGINE = MergeTree ORDER BY ts SETTINGS auto_statistics_types = 'uniq';
INSERT INTO t_topn_lc SELECT toString(number % 1000), number FROM numbers(10000);
OPTIMIZE TABLE t_topn_lc FINAL;
SELECT '-- low cardinality key: optimized == reference';
SELECT
    (SELECT groupArray((k, m)) FROM (SELECT k, min(ts) AS m FROM t_topn_lc GROUP BY k ORDER BY m ASC LIMIT 5 SETTINGS optimize_topn_aggregation = 1)) =
    (SELECT groupArray((k, m)) FROM (SELECT k, min(ts) AS m FROM t_topn_lc GROUP BY k ORDER BY m ASC LIMIT 5 SETTINGS optimize_topn_aggregation = 0));
DROP TABLE t_topn_lc;

-- Reverse-flagged sorting key (ORDER BY ts DESC): physical order is opposite to a plain ascending
-- key, so Mode 1 must be rejected (else it returns the bottom-K). Result must equal the reference.
DROP TABLE IF EXISTS t_topn_reverse;
CREATE TABLE t_topn_reverse (k UInt64, ts DateTime) ENGINE = MergeTree ORDER BY ts DESC SETTINGS auto_statistics_types = 'uniq';
INSERT INTO t_topn_reverse SELECT number % 100, toDateTime('2024-01-01') + number FROM numbers(10000);
OPTIMIZE TABLE t_topn_reverse FINAL;
SELECT '-- reverse key: optimized == reference';
SELECT
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_reverse GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1)) =
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_reverse GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 0));
DROP TABLE t_topn_reverse;

-- Floating-point determining column: NaN sorts at the end of an ascending key, so reverse-reading
-- for ORDER BY max(ts) DESC would decide a NaN-containing group by its NaN first occurrence (and
-- rank it top), while the standard max skips NaN. Mode 1 must be rejected for float keys (mirrors
-- the read-in-order guard); the result must equal the reference.
DROP TABLE IF EXISTS t_topn_float;
CREATE TABLE t_topn_float (k UInt64, ts Float64) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1;
INSERT INTO t_topn_float SELECT number % 100, number FROM numbers(10000);
INSERT INTO t_topn_float VALUES (0, nan), (50, nan);
SELECT '-- float key: not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT k, max(ts) AS m FROM t_topn_float GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1) WHERE explain LIKE '%TopNAggregating%';
SELECT '-- float key with NaN: optimized == reference';
SELECT
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_float GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1, max_block_size = 1)) =
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_float GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 0));
DROP TABLE t_topn_float;

-- The NaN hazard survives a LowCardinality wrapper: aggregate analysis unwraps LowCardinality, so
-- the float guard must unwrap it too (removeLowCardinalityAndNullable) rather than test the wrapped
-- type. LowCardinality(Float64) key must be rejected; the result must equal the reference.
SET allow_suspicious_low_cardinality_types = 1;
DROP TABLE IF EXISTS t_topn_lcfloat;
CREATE TABLE t_topn_lcfloat (k UInt64, ts LowCardinality(Float64)) ENGINE = MergeTree ORDER BY ts SETTINGS index_granularity = 1;
INSERT INTO t_topn_lcfloat SELECT number % 100, number FROM numbers(10000);
INSERT INTO t_topn_lcfloat VALUES (0, nan), (50, nan);
SELECT '-- low cardinality float key: not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT k, max(ts) AS m FROM t_topn_lcfloat GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1) WHERE explain LIKE '%TopNAggregating%';
SELECT '-- low cardinality float key with NaN: optimized == reference';
SELECT
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_lcfloat GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1, max_block_size = 1)) =
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_lcfloat GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 0));
DROP TABLE t_topn_lcfloat;

-- Computed aggregate argument that reuses the sorting-key name: `-ts AS ts` is over the negated
-- value, but the name matches the storage key `ts`. Reading in storage `ts` order would decide
-- groups by the wrong quantity, so argument tracing must fail (non-pass-through) and reject Mode 1.
DROP TABLE IF EXISTS t_topn_expr;
CREATE TABLE t_topn_expr (k UInt64, ts Int64) ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_topn_expr SELECT number % 100, number FROM numbers(10000);
SELECT '-- computed arg aliasing key: not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT k, max(ts) AS m FROM (SELECT k, -ts AS ts FROM t_topn_expr) GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1) WHERE explain LIKE '%TopNAggregating%';
SELECT '-- computed arg aliasing key: optimized == reference';
SELECT
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM (SELECT k, -ts AS ts FROM t_topn_expr) GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1)) =
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM (SELECT k, -ts AS ts FROM t_topn_expr) GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 0));
DROP TABLE t_topn_expr;

-- Aggregate projection: when the query is answered from an aggregate projection, the AggregatingStep
-- runs in merge-state mode (params.only_merge) — its input is AggregateFunction state columns and
-- aggregates_positions is unpopulated. The TopN transform only calls Aggregator::executeOnBlock and
-- cannot consume state columns, so the rewrite must be rejected. force_optimize_projection guarantees
-- the projection (hence only_merge) is used; plan-shape check + correctness vs the reference.
DROP TABLE IF EXISTS t_topn_proj;
CREATE TABLE t_topn_proj (k UInt64, ts UInt64, PROJECTION p (SELECT k, max(ts) GROUP BY k))
ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_topn_proj SELECT number % 100, number FROM numbers(10000);
OPTIMIZE TABLE t_topn_proj FINAL;
SELECT '-- aggregate projection (only_merge): not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT k, max(ts) AS m FROM t_topn_proj GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1, optimize_use_projections = 1, force_optimize_projection = 1) WHERE explain LIKE '%TopNAggregating%';
SELECT '-- aggregate projection (only_merge): optimized == reference';
SELECT
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_proj GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1, optimize_use_projections = 1)) =
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_proj GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 0, optimize_use_projections = 1));
DROP TABLE t_topn_proj;

-- No statistics: the gate falls back to the absolute topn_aggregation_max_limit cap (applies for
-- small LIMIT; topn_aggregation_max_limit = 0 requires statistics, so it does not apply).
DROP TABLE IF EXISTS t_topn_nostats;
CREATE TABLE t_topn_nostats (k UInt64, ts UInt64) ENGINE = MergeTree ORDER BY ts;
INSERT INTO t_topn_nostats SELECT number, number FROM numbers(200000);
SELECT '-- no stats, LIMIT <= max_limit: applied';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT k, max(ts) AS m FROM t_topn_nostats GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1) WHERE explain LIKE '%TopNAggregating%';
SELECT '-- no stats, max_limit = 0: not applied';
SELECT count() = 0 FROM (EXPLAIN actions = 1 SELECT k, max(ts) AS m FROM t_topn_nostats GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 1, topn_aggregation_max_limit = 0) WHERE explain LIKE '%TopNAggregating%';
-- External aggregation: Mode 1 disables its own spill, so the result must equal the reference
-- (regression guard: spill once returned 0 rows).
SELECT '-- external group by: optimized == reference';
SELECT
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_nostats GROUP BY k ORDER BY m DESC LIMIT 5
        SETTINGS optimize_topn_aggregation = 1, max_bytes_before_external_group_by = 1, group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1)) =
    (SELECT groupArray((k, m)) FROM (SELECT k, max(ts) AS m FROM t_topn_nostats GROUP BY k ORDER BY m DESC LIMIT 5 SETTINGS optimize_topn_aggregation = 0));
DROP TABLE t_topn_nostats;
