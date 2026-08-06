DROP TABLE IF EXISTS grace_hash_join_auto_initial_buckets_rhs;
CREATE TABLE grace_hash_join_auto_initial_buckets_rhs (number UInt64) ENGINE = MergeTree ORDER BY number;
INSERT INTO grace_hash_join_auto_initial_buckets_rhs SELECT number FROM numbers(4000);

SELECT default, type
FROM system.settings
WHERE name = 'grace_hash_join_initial_buckets';

SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET collect_hash_table_stats_during_joins = 0;
SET join_algorithm = 'grace_hash';
SET grace_hash_join_max_buckets = 64;
SET max_bytes_in_join = 0;
SET join_overflow_mode = 'throw';

SELECT 'planner auto';
SELECT count()
FROM numbers(10) AS lhs
INNER JOIN grace_hash_join_auto_initial_buckets_rhs AS rhs USING number
SETTINGS
    grace_hash_join_initial_buckets = 0,
    max_rows_in_join = 1000,
    log_comment = '04665_planner_auto';

SELECT 'explicit value';
SELECT count()
FROM numbers(10) AS lhs
INNER JOIN numbers(4000) AS rhs USING number
SETTINGS
    grace_hash_join_initial_buckets = 2,
    max_rows_in_join = 1000,
    log_comment = '04665_explicit';

SELECT 'runtime auto';
SELECT count()
FROM numbers(10000) AS lhs
INNER JOIN numbers(10000) AS rhs USING number
SETTINGS
    join_algorithm = 'hash',
    max_threads = 1,
    max_block_size = 1000,
    grace_hash_join_initial_buckets = 0,
    max_rows_in_join = 0,
    max_bytes_before_external_join = 100000,
    log_comment = '04665_runtime_auto';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    if(log_comment IN ('04665_planner_auto', '04665_runtime_auto'),
        max(ProfileEvents['JoinGraceHashJoinInitialBuckets']) > 1,
        max(ProfileEvents['JoinGraceHashJoinInitialBuckets'])) AS initial_buckets
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND log_comment IN ('04665_planner_auto', '04665_explicit', '04665_runtime_auto')
    AND type = 'QueryFinish'
GROUP BY log_comment
ORDER BY log_comment;

DROP TABLE grace_hash_join_auto_initial_buckets_rhs;
