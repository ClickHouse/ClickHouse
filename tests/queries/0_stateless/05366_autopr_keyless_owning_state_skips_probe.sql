-- An aggregation without keys whose state owns memory (`uniqExact`) never gets statistics, so automatic
-- parallel replicas could never decide it, yet it built the plan with parallel replicas on every execution.
-- Red if that plan is built: `AutomaticParallelReplicasProbePlansBuilt` stays 0 for it.

DROP TABLE IF EXISTS t_keyless_owning_state;

CREATE TABLE t_keyless_owning_state (id UInt64, v UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_keyless_owning_state SELECT number, number % 1000 FROM numbers(300000);

SET enable_parallel_replicas = 1, max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1, parallel_replicas_local_plan = 1,
    automatic_parallel_replicas_mode = 1;
-- The per-replica floor skips building the plan on its own, so neutralise it.
SET automatic_parallel_replicas_min_bytes_per_replica = 0;
-- An aggregation that spills merges on a path that collects no statistics either.
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
-- Rewrites `uniqExact(v)` to a `count()` over `GROUP BY v`, an aggregation with keys.
SET count_distinct_optimization = 0;

SELECT uniqExact(v) FROM t_keyless_owning_state FORMAT Null SETTINGS log_comment = 'keyless_owning_1';
SELECT uniqExact(v) FROM t_keyless_owning_state FORMAT Null SETTINGS log_comment = 'keyless_owning_2';
-- The controls still build it: a state without memory of its own, and an aggregation with keys.
SELECT sum(v) FROM t_keyless_owning_state FORMAT Null SETTINGS log_comment = 'keyless_trivial';
SELECT id % 10, uniqExact(v) FROM t_keyless_owning_state GROUP BY 1 FORMAT Null SETTINGS log_comment = 'keyed_owning';

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE 'key%'
ORDER BY log_comment;

DROP TABLE t_keyless_owning_state;
