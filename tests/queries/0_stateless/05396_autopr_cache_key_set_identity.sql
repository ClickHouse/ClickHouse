-- The statistics of automatic parallel replicas are keyed by the plan, and the key of an expression
-- leaves out the names the plan build chose. The name of an `IN` set was what told two sets of one type
-- apart, so the set itself has to be in the key: otherwise a query reading through one set is decided
-- from the statistics collected for another.

DROP TABLE IF EXISTS t_set_key;
CREATE TABLE t_set_key (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_set_key SELECT number, number FROM numbers(100000);

SET enable_analyzer = 1;
SET enable_parallel_replicas = 1;
SET automatic_parallel_replicas_mode = 1;
SET parallel_replicas_local_plan = 1;
SET parallel_replicas_for_non_replicated_merge_tree = 1;
SET max_parallel_replicas = 3;
SET cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost';
SET automatic_parallel_replicas_min_bytes_per_replica = 0;

-- The set is computed in an expression, not used as a filter: a filter keys its predicate separately.
SELECT sum(v IN (1, 2, 3)) FROM t_set_key SETTINGS log_comment = '05396_set_a';
SELECT sum(v IN (5, 6, 7)) FROM t_set_key SETTINGS log_comment = '05396_set_b';
SELECT sum(v IN (1, 2, 3)) FROM t_set_key SETTINGS log_comment = '05396_set_a_again';

SET enable_parallel_replicas = 0;
SET automatic_parallel_replicas_mode = 0;

SYSTEM FLUSH LOGS query_log;

-- Each set finds no statistics on its first run, and the first one finds its own on the next.
SELECT log_comment, ProfileEvents['AutoParallelReplicasNoStatistics'] > 0 AS no_statistics
FROM system.query_log
WHERE (event_date >= yesterday()) AND (current_database = currentDatabase())
  AND startsWith(log_comment, '05396_set_') AND (type = 'QueryFinish')
ORDER BY event_time_microseconds
FORMAT TSVWithNames;

DROP TABLE t_set_key;
