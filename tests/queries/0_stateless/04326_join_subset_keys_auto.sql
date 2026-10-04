-- Verify cardinality-driven JOIN key demotion: a high-NDV equality key is moved out of the hash
-- table key set into a probe-time condition, so the hash table is built only on the low-NDV key.
--
-- Cardinalities and NDVs come from `_internal_join_table_stat_hints` instead of real column
-- statistics, so the plan does not depend on how many rows are inserted or on what the estimator
-- infers from them.

SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET join_algorithm = 'hash';
SET use_statistics = 0;
SET send_logs_level = 'error'; -- Suppress the warning about the statistics hint
SET query_plan_optimize_join_order_randomize = 0; -- Pinned because the test asserts on the join plan
SET query_plan_optimize_join_order_limit = 10;
-- Key demotion operates on the build (right) side and `EXPLAIN` prints the join clauses with a
-- fixed table order, so pin the swap to keep the printed plan stable.
SET query_plan_join_swap_table = 'false';
-- `query_plan_hash_join_subset_keys_min_saving_bytes` defaults to one arena chunk, which a 5000-row
-- build side can never reach. Drop the floor so the remaining gates are the ones deciding each case;
-- the probe cost ceiling stays at its default, so the plans below are the ones it admits.
SET query_plan_hash_join_subset_keys_min_saving_bytes = 0;

DROP TABLE IF EXISTS jks_left;
DROP TABLE IF EXISTS jks_right;

CREATE TABLE jks_left (user_id UInt64, request_id UInt64, payload UInt64) ENGINE = MergeTree ORDER BY user_id;
CREATE TABLE jks_right (user_id UInt64, request_id UInt64, extra UInt64) ENGINE = MergeTree ORDER BY user_id;

INSERT INTO jks_left SELECT number % 500, number % 10, number FROM numbers(5000);
INSERT INTO jks_right SELECT number % 500, number % 10, number FROM numbers(5000);

-- `jks_right` has 5000 rows, NDV(user_id) = 500 and NDV(request_id) = 10.
SET param__internal_join_table_stat_hints = '
{
    "jks_left":  { "cardinality": 5000, "distinct_keys": { "user_id": 500, "request_id": 10, "payload": 5000 } },
    "jks_right": { "cardinality": 5000, "distinct_keys": { "user_id": 500, "request_id": 10, "extra": 5000 } }
}';

SELECT 'count_on' AS label, count() FROM jks_left l JOIN jks_right r
    ON l.user_id = r.user_id AND l.request_id = r.request_id
SETTINGS query_plan_hash_join_subset_keys_auto = 1,
    query_plan_hash_join_subset_keys_min_rows = 0,
    query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001;

SELECT 'off' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_hash_join_subset_keys_auto = 0
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- target_ndv = 5000 * 0.001 = 5, which both single keys reach. The cheapest of them wins: keeping
-- `user_id` (NDV = 500) leaves a 10-row bucket for the probe, keeping `request_id` (NDV = 10) would
-- leave a 500-row one, so `request_id` is demoted to a probe-time condition.
SELECT 'on' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_hash_join_subset_keys_auto = 1,
        query_plan_hash_join_subset_keys_min_rows = 0,
        query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- min_rows gate: a build side below the threshold keeps both keys in the hash table.
SELECT 'rows_gate' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_hash_join_subset_keys_auto = 1, query_plan_hash_join_subset_keys_min_rows = 100000
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- selectivity gate: target_ndv = 5000 * 2.0 = 10000 exceeds every candidate NDV (the largest is
-- NDV(user_id) = 500), so nothing is demoted.
SELECT 'selectivity_gate' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_hash_join_subset_keys_auto = 1, query_plan_hash_join_subset_keys_min_rows = 0,
        query_plan_hash_join_subset_keys_min_kept_selectivity = 2.0
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- Algorithm gates. Only the hash family evaluates a mixed join condition, so an enabled algorithm
-- that ignores it must disable demotion entirely - otherwise the equality would be dropped
-- silently (merge joins) or the query would fail outright (`auto`).
SELECT 'algo_gate_full_sorting_merge' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS join_algorithm = 'full_sorting_merge',
        query_plan_hash_join_subset_keys_auto = 1,
        query_plan_hash_join_subset_keys_min_rows = 0,
        query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

SELECT 'algo_gate_parallel_full_sorting_merge' AS label, count() FROM jks_left l JOIN jks_right r
    ON l.user_id = r.user_id AND l.request_id = r.request_id
SETTINGS join_algorithm = 'parallel_full_sorting_merge',
    query_plan_hash_join_subset_keys_auto = 1,
    query_plan_hash_join_subset_keys_min_rows = 0,
    query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001;

SELECT 'algo_gate_auto' AS label, count() FROM jks_left l JOIN jks_right r
    ON l.user_id = r.user_id AND l.request_id = r.request_id
SETTINGS join_algorithm = 'auto',
    query_plan_hash_join_subset_keys_auto = 1,
    query_plan_hash_join_subset_keys_min_rows = 0,
    query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001;

-- Strictness gate: `any_join_distinct_right_table_keys = 1` promotes `ANY` to `RightAny`, which
-- `HashJoin::isAdditionalFilterSupported` rejects, so demotion must be skipped.
SELECT 'right_any_count' AS label, count() FROM jks_left l ANY LEFT JOIN jks_right r
    ON l.user_id = r.user_id AND l.request_id = r.request_id
SETTINGS any_join_distinct_right_table_keys = 1,
    query_plan_hash_join_subset_keys_auto = 1,
    query_plan_hash_join_subset_keys_min_rows = 0,
    query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001;

SELECT 'right_any_plan' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM jks_left l ANY LEFT JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS any_join_distinct_right_table_keys = 1,
        query_plan_hash_join_subset_keys_auto = 1,
        query_plan_hash_join_subset_keys_min_rows = 0,
        query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- Without join reordering the build side's statistics are never computed, so demotion cannot run.
SELECT 'reorder_disabled' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM jks_left l JOIN jks_right r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_optimize_join_order_limit = 0,
        query_plan_hash_join_subset_keys_auto = 1,
        query_plan_hash_join_subset_keys_min_rows = 0,
        query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%' OR explain ILIKE '%Mixed condition%';

-- The build-side row count is taken after the filters above the scan, not from the raw table.
-- Real column statistics are needed for that (a stats hint describes the table, not the filtered
-- stream), so this section switches back to them.
SET use_statistics = 1;
SET allow_statistics = 1;
SET param__internal_join_table_stat_hints = '';

DROP TABLE IF EXISTS jks_right_stats;
CREATE TABLE jks_right_stats
(
    user_id UInt64 STATISTICS(uniq),
    request_id UInt64 STATISTICS(uniq),
    extra UInt64
) ENGINE = MergeTree ORDER BY user_id;

INSERT INTO jks_right_stats SELECT number % 500, number % 10, number FROM numbers(5000);
OPTIMIZE TABLE jks_right_stats FINAL;

SELECT 'stats_on' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM jks_left l JOIN jks_right_stats r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_hash_join_subset_keys_auto = 1,
        query_plan_hash_join_subset_keys_min_rows = 0,
        query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%';

-- A `WHERE` above the build-side scan drops it below `min_rows`, so demotion must bail. The raw
-- table has 5000 rows and the filter keeps about a tenth of them, under the 4000 threshold.
SELECT 'filtered_below_min_rows' AS label, trimLeft(explain) FROM
(
    EXPLAIN actions = 1, pretty = 0
    SELECT count() FROM jks_left l JOIN (SELECT * FROM jks_right_stats WHERE extra < 500) r
        ON l.user_id = r.user_id AND l.request_id = r.request_id
    SETTINGS query_plan_hash_join_subset_keys_auto = 1,
        query_plan_hash_join_subset_keys_min_rows = 4000,
        query_plan_hash_join_subset_keys_min_kept_selectivity = 0.001
)
WHERE explain ILIKE '%Clauses%' OR explain ILIKE '%Residual filter%';

DROP TABLE jks_right_stats;
DROP TABLE jks_left;
DROP TABLE jks_right;
