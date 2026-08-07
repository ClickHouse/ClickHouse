DROP TABLE IF EXISTS grace_hash_join_auto_buffer_budget_rhs;
CREATE TABLE grace_hash_join_auto_buffer_budget_rhs (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO grace_hash_join_auto_buffer_budget_rhs SELECT number FROM numbers(32768);

SELECT count()
FROM numbers(32768) AS lhs
INNER JOIN grace_hash_join_auto_buffer_budget_rhs AS rhs USING number
SETTINGS
    join_algorithm = 'grace_hash',
    grace_hash_join_initial_buckets = 0,
    grace_hash_join_max_buckets = 1024,
    max_rows_in_join = 64,
    max_bytes_in_join = 0,
    max_memory_usage = 268435456,
    max_threads = 1,
    query_plan_optimize_join_order_limit = 10,
    query_plan_join_swap_table = 0,
    collect_hash_table_stats_during_joins = 0,
    enable_parallel_replicas = 0,
    log_comment = '04666_planner_auto_buffer_budget';

SELECT count()
FROM numbers(32768) AS lhs
INNER JOIN numbers(32768) AS rhs USING number
SETTINGS
    join_algorithm = 'grace_hash',
    grace_hash_join_initial_buckets = 0,
    grace_hash_join_max_buckets = 1024,
    max_rows_in_join = 64,
    max_bytes_in_join = 0,
    max_memory_usage = 268435456,
    max_threads = 1,
    query_plan_optimize_join_order_limit = 10,
    query_plan_join_swap_table = 0,
    collect_hash_table_stats_during_joins = 0,
    enable_parallel_replicas = 0,
    log_comment = '04666_runtime_rehash_buffer_budget';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    max(ProfileEvents['JoinGraceHashJoinInitialBuckets'])
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND log_comment IN ('04666_planner_auto_buffer_budget', '04666_runtime_rehash_buffer_budget')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE grace_hash_join_auto_buffer_budget_rhs;
