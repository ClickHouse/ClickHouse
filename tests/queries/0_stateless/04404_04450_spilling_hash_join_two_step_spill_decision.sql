-- `SpillingHashJoin` checks a deferred build twice: before each block it stores, against a live projection, and once
-- all blocks are stored, against the final projection. Here the right side arrives in one block, so only the final
-- check can see it. The stored block takes under half of `max_bytes_before_external_join`, but its maps will not fit,
-- so the build switches to `GraceHashJoin` before inserting any key.

SET join_algorithm = 'parallel_hash';
SET collect_hash_table_stats_during_joins = 0;
SET parallel_hash_join_threshold = 0;
SET max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0;
SET max_threads = 4;
SET grace_hash_join_initial_buckets = 4;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 'false';

SELECT 'streaming', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number * 3 AS k FROM numbers(50000)) l
INNER JOIN (SELECT number AS k, number * 7 AS v FROM numbers(200000)) r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 0, max_bytes_before_external_join = 0;

SELECT 'deferred', count(), sum(cityHash64(l.k, r.v))
FROM (SELECT number * 3 AS k FROM numbers(50000)) l
INNER JOIN (SELECT number AS k, number * 7 AS v FROM numbers(200000)) r ON l.k = r.k
SETTINGS enable_hash_join_deferred_build = 1, max_bytes_before_external_join = 8388608, max_block_size = 1000000,
    log_comment = '04404_switch_at_finish';

SYSTEM FLUSH LOGS query_log;
SELECT
    'switched at the end of the build',
    ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'],
    ProfileEvents['HashJoinDeferredBuilds']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_kind = 'Select'
    AND log_comment = '04404_switch_at_finish';
