-- `ORDER BY k DESC` over a key-ascending stream puts the heap into its
-- evict-only regime: every new key beats the boundary, so nothing is ever
-- skipped and every admission evicts an older key.  This is not a failure
-- mode: pruning the evicted keys out of the hash table is what bounds memory
-- by the LIMIT rather than by the number of distinct keys (see
-- 04652_group_by_top_k_pruning_memory for the `ASC`-over-descending twin).
-- The profitability freeze must therefore stay away even with a live
-- observation window - it only fires when the heap neither skips nor evicts.

SET max_rows_to_group_by = 0;
-- CI randomizes query_plan_max_limit_for_top_k_optimization (can be tiny); pin it.
SET query_plan_max_limit_for_top_k_optimization = 1000;
SET enable_group_by_top_k_optimization = 1;
-- The default window; pinned so the freeze decision under test stays armed.
SET group_by_top_k_optimization_observation_rows = 65536;
SET optimize_trivial_group_by_limit_query = 0;
-- One stream, so the assertions below describe a single heap.
SET max_threads = 1;
SET enable_parallel_replicas = 0;
-- The heap only exists in the hash-aggregation path.
SET optimize_aggregation_in_order = 0;
SET log_queries = 1;

-- Ascending keys, two rows per key: under `ORDER BY k DESC` every new key is
-- better than the current boundary for the whole stream.
SELECT k, uniqExact(v)
FROM (SELECT intDiv(number, 2) AS k, number % 7 AS v FROM numbers(1000000))
GROUP BY k
ORDER BY k DESC
LIMIT 10
FORMAT Null
SETTINGS log_comment = '04818_desc_over_asc';

SYSTEM FLUSH LOGS query_log;

-- The heap never skips, evicts throughout the run, and does not freeze: the
-- window judges the heap by rejections, and evictions are rejections.
SELECT 'desc_over_asc: unfrozen, nothing skipped, evicting throughout';
SELECT
    max(ProfileEvents['AggregationTopKHeapsFrozen']),
    max(ProfileEvents['AggregationTopKRowsSkipped']),
    max(ProfileEvents['AggregationTopKKeysEvicted']) > 400000
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment = '04818_desc_over_asc';

-- Pruning the evicted keys is what keeps the aggregate-state arena bounded by
-- the LIMIT: half a million live `uniqExact` states would take tens of
-- megabytes, while eviction recycles the same few slots.
SELECT 'arena bounded by the LIMIT';
SELECT max(ProfileEvents['ArenaAllocBytes']) < 4000000
FROM system.query_log
WHERE event_date >= yesterday() AND current_database = currentDatabase()
    AND type = 'QueryFinish' AND log_comment = '04818_desc_over_asc';

-- Results are unaffected: the top 10 keys with their complete aggregates.
SELECT 'results';
SELECT k, count(), uniqExact(v)
FROM (SELECT intDiv(number, 2) AS k, number % 7 AS v FROM numbers(1000000))
GROUP BY k
ORDER BY k DESC
LIMIT 10;
