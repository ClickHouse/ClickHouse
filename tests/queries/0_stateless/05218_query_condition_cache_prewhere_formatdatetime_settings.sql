-- Tags: no-parallel
-- Tag no-parallel: messes with the query condition cache

-- Two queries whose PREWHERE condition differs only in `formatdatetime_f_prints_single_zero` must not
-- share a query condition cache entry.

DROP TABLE IF EXISTS t_qcc_prewhere;
SET enable_analyzer = 1;
SET use_query_condition_cache = 1;
SET parallel_replicas_local_plan = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t_qcc_prewhere;

-- The auto minmax indexes would filter marks before PREWHERE sees them, and the cache stores nothing for
-- small parts. A single part keeps the number of cache entries equal to the number of distinct keys.
CREATE TABLE t_qcc_prewhere (k UInt64, d DateTime) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0;
INSERT INTO t_qcc_prewhere SELECT number, toDateTime('2024-05-05 10:00:00') + number % 86400 FROM numbers(1000000);
OPTIMIZE TABLE t_qcc_prewhere FINAL;

-- The condition is evaluated in PREWHERE, and no WHERE filter step is left in the plan.
SELECT count() FROM (EXPLAIN SELECT sum(k) FROM t_qcc_prewhere WHERE formatDateTime(d, '%f') = '0') WHERE explain ILIKE '%prewhere filter column%';
SELECT count() FROM (EXPLAIN SELECT sum(k) FROM t_qcc_prewhere WHERE formatDateTime(d, '%f') = '0') WHERE explain LIKE '%Filter ((%';

SYSTEM DROP QUERY CONDITION CACHE;

-- `formatDateTime(d, '%f')` renders '000000' by default and '0' with the setting enabled: no row matches
-- under the first value, every row matches under the second one.
SELECT sum(k) FROM t_qcc_prewhere WHERE formatDateTime(d, '%f') = '0' SETTINGS formatdatetime_f_prints_single_zero = 0;
SELECT count() FROM system.query_condition_cache;
-- The cached verdict is "no mark matches", which is wrong for the other value of the setting.
SELECT position(matching_marks, '1') FROM system.query_condition_cache;

-- The same query reuses its entry, the query with the flipped setting gets its own.
SELECT sum(k) FROM t_qcc_prewhere WHERE formatDateTime(d, '%f') = '0' SETTINGS formatdatetime_f_prints_single_zero = 0;
SELECT count() FROM system.query_condition_cache;
SELECT sum(k) FROM t_qcc_prewhere WHERE formatDateTime(d, '%f') = '0' SETTINGS formatdatetime_f_prints_single_zero = 1;
SELECT count() FROM system.query_condition_cache;
SELECT sum(k) FROM t_qcc_prewhere WHERE formatDateTime(d, '%f') = '0' SETTINGS use_query_condition_cache = 0, formatdatetime_f_prints_single_zero = 1;

DROP TABLE t_qcc_prewhere;
