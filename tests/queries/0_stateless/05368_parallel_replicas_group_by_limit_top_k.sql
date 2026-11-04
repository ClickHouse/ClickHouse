-- `GROUP BY ... LIMIT` without `ORDER BY` over parallel replicas: the initiator keeps the smallest merged groups by the
-- keys, so every replica keeps only its smallest groups in the top-K heap of its partial aggregation.

DROP TABLE IF EXISTS t_05368;
CREATE TABLE t_05368 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v;
INSERT INTO t_05368 SELECT intHash64(number) % 100000, number FROM numbers(2000000);

SET enable_analyzer = 1, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;
-- The heap refuses `max_rows_to_group_by`, which the test profile sets.
SET enable_group_by_top_k_optimization = 1, max_rows_to_group_by = 0, exact_rows_before_limit = 0,
    query_plan_max_limit_for_top_k_optimization = 1000;

SELECT k, count() FROM t_05368 GROUP BY k ORDER BY k LIMIT 3 OFFSET 2 SETTINGS enable_parallel_replicas = 0;
SELECT k, count() FROM t_05368 GROUP BY k LIMIT 3 OFFSET 2
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0, parallel_replicas_local_plan = 0,
    serialize_query_plan = 0, log_comment = '05368_replicas_query_text';
SELECT k, count() FROM t_05368 GROUP BY k LIMIT 3 OFFSET 2
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0, parallel_replicas_local_plan = 0,
    serialize_query_plan = 1, log_comment = '05368_replicas_query_plan';
SELECT k, count() FROM t_05368 GROUP BY k LIMIT 3 OFFSET 2
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 0,
    log_comment = '05368_replicas_plan_based';
SELECT k, count() FROM t_05368 GROUP BY k LIMIT 3 OFFSET 2
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0, parallel_replicas_local_plan = 1;
SELECT k, count() FROM t_05368 GROUP BY k LIMIT 3 OFFSET 2
SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 1, parallel_replicas_local_plan = 1;

-- The remote replicas run the heap, whether they plan the query text or the plan of the initiator.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, sum(ProfileEvents['AggregationTopKRowsSkipped']) > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND NOT is_initial_query AND initial_query_id IN (
    SELECT query_id FROM system.query_log
    WHERE current_database = currentDatabase() AND is_initial_query AND log_comment IN (
        '05368_replicas_query_text', '05368_replicas_query_plan', '05368_replicas_plan_based'))
GROUP BY log_comment
ORDER BY log_comment;

-- The sort above the merge, and the heap of the local replica.
SELECT DISTINCT replaceRegexpAll(explain, '^[│└├─ ]+', '')
FROM (EXPLAIN actions = 1 SELECT k, count() FROM t_05368 GROUP BY k LIMIT 3
    SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0, parallel_replicas_local_plan = 1)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting (%';

-- An `arrayJoin` above the merge can drop groups, so the smallest groups may not be enough.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT k, arrayJoin([1, 2]) AS x, count() FROM t_05368 GROUP BY k LIMIT 3
    SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0, parallel_replicas_local_plan = 1)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting (%';

-- No replica takes the heap under `max_rows_to_group_by`, so neither is the sort added.
SELECT count()
FROM (EXPLAIN actions = 1 SELECT k, count() FROM t_05368 GROUP BY k LIMIT 3
    SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0, parallel_replicas_local_plan = 1,
        max_rows_to_group_by = 1000000)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting (%';

-- Aggregation in order takes no heap, and the sort would stop the in-order merge from ending early.
DROP TABLE IF EXISTS t_05368_in_order;
CREATE TABLE t_05368_in_order (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_05368_in_order SELECT number FROM numbers(1000);
SELECT count()
FROM (EXPLAIN actions = 1 SELECT k, count() FROM t_05368_in_order GROUP BY k LIMIT 3
    SETTINGS enable_parallel_replicas = 1, parallel_replicas_plan_based = 0, parallel_replicas_local_plan = 1,
        optimize_aggregation_in_order = 1)
WHERE explain LIKE '%Top-K%' OR explain LIKE '%Sorting (%';

-- Nothing sorts by the keys in the query text, so a key that may be grouped by but not sorted by is fine.
DROP TABLE IF EXISTS t_05368_dynamic;
CREATE TABLE t_05368_dynamic (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05368_dynamic SELECT number FROM numbers(100);
SELECT d, count() FROM t_05368_dynamic GROUP BY d LIMIT 3
SETTINGS enable_parallel_replicas = 1, allow_suspicious_types_in_group_by = 1, allow_suspicious_types_in_order_by = 0;

DROP TABLE t_05368;
DROP TABLE t_05368_in_order;
DROP TABLE t_05368_dynamic;
