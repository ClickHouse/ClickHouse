-- Evicting a group from the top-K heap must reuse its aggregate-state arena
-- slot.  A descending key stream under `ORDER BY k ASC LIMIT N` admits every
-- new key and evicts an older one, so without reuse the arena grows by one
-- state per distinct key seen (20M here) even though the hash table stays
-- bounded, defeating the optimization's memory contract for non-`count`
-- aggregates and failing the memory limit below.
SET enable_group_by_top_k_optimization = 1;
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;
SET max_threads = 1;
SET max_memory_usage = 100000000;
SET max_bytes_before_external_group_by = 0;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT k, sum(v) FROM
(
    SELECT 20000000 - number AS k, number AS v FROM numbers(20000000)
)
GROUP BY k
ORDER BY k ASC
LIMIT 10
SETTINGS log_comment = '04495_state_arena_reuse';

SYSTEM FLUSH LOGS query_log;

-- Prove the eviction path actually ran (otherwise this test guards nothing).
SELECT sum(ProfileEvents['AggregationTopKKeysEvicted']) > 1000000 AS evicted
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04495_state_arena_reuse'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();
