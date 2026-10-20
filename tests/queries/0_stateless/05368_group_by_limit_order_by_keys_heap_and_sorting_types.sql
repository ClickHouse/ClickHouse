-- `GroupByLimitOrderByKeysPass` orders `GROUP BY ... LIMIT` by the keys when the query may run with parallel replicas.
-- It rewrites only the outermost query, so the queries below are not wrapped in subqueries.

SET enable_analyzer = 1;
-- The heap refuses `max_rows_to_group_by`, which the test profile sets.
SET enable_group_by_top_k_optimization = 1, max_rows_to_group_by = 0, exact_rows_before_limit = 0,
    query_plan_max_limit_for_top_k_optimization = 1000, group_by_top_k_optimization_observation_rows = 65536;

-- The injected order only pays off while the heap bounds the hash table, so the heap must not freeze, like with the
-- synthesized sort. Ten keys repeat for the first million rows, then every row is a new key: the heap rejects nothing
-- within its observation window, which freezes it under an `ORDER BY` of the query. Automatic parallel replicas
-- declines the read from `numbers`, so the single-node plan runs with the injected order.
SET max_threads = 1, enable_parallel_replicas = 1, automatic_parallel_replicas_mode = 1;
SELECT if(number < 1000000, number % 10, number) AS k, count() FROM numbers(3000000) GROUP BY k LIMIT 10
SETTINGS log_comment = '05368_injected';
SELECT if(number < 1000000, number % 10, number) AS k, count() FROM numbers(3000000) GROUP BY k ORDER BY k LIMIT 10
SETTINGS log_comment = '05368_order_by';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['AggregationTopKHeapsFrozen'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05368_injected', '05368_order_by')
ORDER BY log_comment;

-- The replicas analyze the injected `ORDER BY` from the query text, so a key that may be grouped by but not sorted by
-- must not be ordered by.
DROP TABLE IF EXISTS t_05368;
CREATE TABLE t_05368 (d Dynamic, v Variant(UInt64, String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_05368 SELECT number, toString(number) FROM numbers(100);

SET max_threads = 0, automatic_parallel_replicas_mode = 0, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1,
    allow_suspicious_types_in_group_by = 1, allow_suspicious_types_in_order_by = 0;
SELECT d, count() FROM t_05368 GROUP BY d LIMIT 3 FORMAT Null;
SELECT v, count() FROM t_05368 GROUP BY v LIMIT 3 FORMAT Null;
SELECT [d] AS a, count() FROM t_05368 GROUP BY a LIMIT 3 FORMAT Null;
-- A sortable key is still ordered by.
SELECT count() FROM (EXPLAIN SELECT d.UInt64 AS n, count() FROM t_05368 GROUP BY n LIMIT 3) WHERE explain LIKE '%Sorting for ORDER BY%';

DROP TABLE t_05368;
