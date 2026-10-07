-- At `max_threads = 1` a `hash` build runs on one fill thread: every right block is stored and inserted
-- as it arrives into a table that starts at 2^8 cells when no earlier run of the query has left a distinct-key count
-- in the hash table statistics cache, and doubles as the keys come, as the `hash` build does. Under
-- `max_bytes_before_external_join` those doublings must not be refused by the budget: the spilling wrapper
-- judges the resident set between blocks and at the barrier and hands the stored blocks to `GraceHashJoin` when
-- they do not fit. The right side here arrives in two blocks of eight UInt64 columns (4 MiB each); the budget of 11 MiB
-- lies between the wrapper's prediction before the second block (4 MiB stored, the 2 MiB table and its doubling)
-- and the resident set at the barrier (8 MiB stored and a 4 MiB table), so the switch is taken at the barrier.
-- Both queries must return the same rows.

SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_ratio_before_external_join = 0;
SET max_bytes_in_join = 0;
SET grace_hash_join_initial_buckets = 1;
SET grace_hash_join_max_buckets = 1024;
SET max_threads = 1;
SET join_algorithm = 'hash';
-- A distinct-key count published by an earlier run of the same query must not pre-size the table; every
-- query must start from the smallest table.
SET collect_hash_table_stats_during_joins = 0;

SELECT 'in memory', count(), sum(t2.v1 + t2.v2 + t2.v3 + t2.v4 + t2.v5 + t2.v6 + t2.v7)
FROM (SELECT number AS k FROM numbers(200000)) AS t1
INNER JOIN (SELECT number AS k, number AS v1, number AS v2, number AS v3, number AS v4, number AS v5, number AS v6, number AS v7 FROM numbers(130000)) AS t2
ON t1.k = t2.k
SETTINGS max_bytes_before_external_join = 0, log_comment = '05224 in memory';

SELECT 'spilled', count(), sum(t2.v1 + t2.v2 + t2.v3 + t2.v4 + t2.v5 + t2.v6 + t2.v7)
FROM (SELECT number AS k FROM numbers(200000)) AS t1
INNER JOIN (SELECT number AS k, number AS v1, number AS v2, number AS v3, number AS v4, number AS v5, number AS v6, number AS v7 FROM numbers(130000)) AS t2
ON t1.k = t2.k
SETTINGS max_bytes_before_external_join = 11534336, log_comment = '05224 spilled';

SYSTEM FLUSH LOGS query_log;

SELECT '-- the hintless table grew during the fill; the budgeted build switched to grace';
SELECT log_comment, ProfileEvents['HashJoinTableResizes'] > 0, ProfileEvents['JoinSpillingHashJoinSwitchedToGraceJoin'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '05224 %'
ORDER BY log_comment;
