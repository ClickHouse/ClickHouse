-- Interaction between the self-join shared-scan rewrite and read-in-order (through join).
-- The rewrite runs before `optimizeReadInOrder`, so it never observes an in-order reading
-- contract, and the explicit ORDER BY sort keeps results correct whether or not it fires.

SET enable_analyzer = 1;
SET query_plan_optimize_self_join_shared_scan = 1;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET enable_shared_storage_snapshot_in_query = 1;
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_read_in_order_through_join = 1;
SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS t_sjss_rio;
CREATE TABLE t_sjss_rio (t UInt64, id UInt64) ENGINE = MergeTree ORDER BY t;
INSERT INTO t_sjss_rio SELECT number, number % 8 FROM numbers(1000);

-- The rewrite fires (left columns are a subset of right columns), the ORDER BY is on the sorting
-- key and the join is on a different column: results must be correct.
SELECT a.t, a.id, b.t FROM t_sjss_rio AS a INNER JOIN t_sjss_rio AS b ON a.id = b.id
ORDER BY a.t, b.t LIMIT 10;

-- Same query with the optimization off, results must match.
SELECT a.t, a.id, b.t FROM t_sjss_rio AS a INNER JOIN t_sjss_rio AS b ON a.id = b.id
ORDER BY a.t, b.t LIMIT 10
SETTINGS query_plan_optimize_self_join_shared_scan = 0;

-- Plan shape: one shared scan feeds a buffer, and an explicit Sorting step preserves ORDER BY.
SELECT
    countIf(explain LIKE '%ReadFromMergeTree%') AS rmt_count,
    countIf(explain LIKE '%ReadFromCommonBuffer%') AS read_count,
    countIf(explain LIKE '%Sorting (Sorting for ORDER BY)%') AS sort_count
FROM (
    EXPLAIN actions = 0
    SELECT a.t, a.id, b.t FROM t_sjss_rio AS a INNER JOIN t_sjss_rio AS b ON a.id = b.id
    ORDER BY a.t, b.t LIMIT 10
);

DROP TABLE t_sjss_rio;
