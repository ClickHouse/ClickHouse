-- Tags: no-parallel-replicas
-- Random settings limits: enable_join_runtime_filters=(1, 1)

-- An expression in a subquery's SELECT list must not be evaluated on the rows that the same
-- subquery's WHERE removes. When the subquery is the probe side of a JOIN that builds a runtime
-- filter, the guard, the runtime filter and that expression all ended up in one step which still had
-- to produce the expression, so the CAST ran on the row `WHERE c4` excludes and the query failed with
-- CANNOT_READ_ARRAY_FROM_TEXT instead of returning rows.
-- join_runtime_filter_min_probe_rows = 0 is required, and the threshold is compared against the
-- planner's ESTIMATED probe rows rather than the actual ones: `WHERE c4` alone brings the estimate
-- below the default 1000, the runtime filter is then planted but never applied, and every assertion
-- here passes vacuously.
-- The remaining settings are pinned because the test asserts on which join side is probed: the apply
-- step is planted on one side only, and of the join algorithms only hash, parallel_hash and
-- grace_hash build a runtime filter at all.
-- use_query_cache = 0 is separate: log_comment is stripped before the cache key is computed, so the
-- probe statement below would otherwise be served from the first statement's cache entry and raise no
-- ProfileEvents at all.

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (c1 Int64, c2 Int64, c3 String, c4 Bool, c5 UInt64) ENGINE = ReplacingMergeTree(c5) ORDER BY c1;
CREATE TABLE t2 (c1 Int64, c2 String) ENGINE = ReplacingMergeTree ORDER BY c1;

INSERT INTO t1 SELECT number + 1, number % 5 + 1, '[\'a\',\'b\']', true, 1 FROM numbers(2000);
-- '' is not a valid Array(String), and `c4 = false` is what keeps it away from the CAST. Its c1 is
-- outside the range inserted above so that FINAL cannot collapse it into a valid row.
INSERT INTO t1 VALUES (1000001, 1, '', false, 1);
INSERT INTO t2 SELECT number + 1, concat('n', toString(number)) FROM numbers(5);

SELECT count(), sum(length(a.s))
FROM (SELECT c1, c2, CAST(c3, 'Array(String)') AS s FROM t1 FINAL WHERE c4) AS a
INNER JOIN t2 AS b ON b.c1 = a.c2
SETTINGS join_runtime_filter_min_probe_rows = 0, join_algorithm = 'hash',
         query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0,
         query_plan_optimize_join_order_limit = 10,
         query_plan_optimize_join_order_algorithm = 'greedy',
         enable_parallel_replicas = 0, use_query_cache = 0;

SELECT count(), sum(length(a.s))
FROM (SELECT c1, c2, CAST(c3, 'Array(String)') AS s FROM t1 FINAL WHERE c4) AS a
INNER JOIN t2 AS b ON b.c1 = a.c2
SETTINGS join_runtime_filter_min_probe_rows = 0, join_algorithm = 'hash',
         query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0,
         query_plan_optimize_join_order_limit = 10,
         query_plan_optimize_join_order_algorithm = 'greedy',
         enable_parallel_replicas = 0, use_query_cache = 0, enable_join_runtime_filters = 0;

-- The runtime filter must still be built, not silently dropped to avoid the CAST. This also fails if
-- the assertions above ever stop planting the filter, which would make them pass vacuously.
SELECT countIf(explain ILIKE '%BuildRuntimeFilter%') > 0
FROM (
    EXPLAIN PLAN
    SELECT count(), sum(length(a.s))
    FROM (SELECT c1, c2, CAST(c3, 'Array(String)') AS s FROM t1 FINAL WHERE c4) AS a
    INNER JOIN t2 AS b ON b.c1 = a.c2
    SETTINGS join_runtime_filter_min_probe_rows = 0, join_algorithm = 'hash',
             query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0,
             query_plan_optimize_join_order_limit = 10,
             query_plan_optimize_join_order_algorithm = 'greedy',
             enable_parallel_replicas = 0, use_query_cache = 0
);

-- Being built is not being applied, and only the probe side runs the guarded expression. This counter
-- is the direct evidence that the subquery above is the side the filter is applied to, so that a
-- configuration which cannot witness the bug reddens here instead of passing. A bare `> 0` would not
-- do: with the join orientation reversed the filter is applied to t2 instead and the counter reads 5,
-- so the threshold has to separate t1's 2001 rows from t2's 5.
SELECT count(), sum(length(a.s))
FROM (SELECT c1, c2, CAST(c3, 'Array(String)') AS s FROM t1 FINAL WHERE c4) AS a
INNER JOIN t2 AS b ON b.c1 = a.c2
SETTINGS join_runtime_filter_min_probe_rows = 0, join_algorithm = 'hash',
         query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0,
         query_plan_optimize_join_order_limit = 10,
         query_plan_optimize_join_order_algorithm = 'greedy',
         enable_parallel_replicas = 0, use_query_cache = 0, log_comment = '05255_rf_probe';

SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['RuntimeFilterRowsChecked'] >= 2000
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05255_rf_probe' AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC LIMIT 1;
