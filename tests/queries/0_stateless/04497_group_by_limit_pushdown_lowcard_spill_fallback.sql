-- Pattern 2 (`GROUP BY ... LIMIT`, no ORDER BY) requires hash-table pruning,
-- which `LowCardinality` keys cannot do; the heap then falls back to normal
-- aggregation at runtime.  External aggregation must stay enabled on that
-- fallback: the spill-disable decision is made in the `Aggregator` constructor
-- only when the heap is actually committed, not at plan time for every match.
-- Regression: with plan-time disabling, this query ran normal aggregation with
-- spilling forcibly off and failed the memory limit (needs ~660MB without
-- spill).
SET enable_group_by_top_k_optimization = 1;
SET max_rows_to_group_by = 0;
SET optimize_trivial_group_by_limit_query = 0;
SET max_threads = 1;
SET max_memory_usage = 300000000;
SET max_bytes_before_external_group_by = 50000000;
SET max_bytes_ratio_before_external_group_by = 0;

SELECT count() FROM
(
    SELECT k, count() AS c
    FROM (SELECT toLowCardinality(toString(number % 3000000)) AS k FROM numbers(9000000))
    GROUP BY k
    LIMIT 5
) SETTINGS log_comment = '04497_lowcard_spill_fallback';

SYSTEM FLUSH LOGS query_log;

-- The heap must have been inactive (LowCardinality cannot prune) and the
-- query must have actually spilled — proving the configured threshold
-- survived the fallback.
SELECT
    sum(ProfileEvents['AggregationTopKRowsSkipped']) AS topk_skipped,
    sum(ProfileEvents['ExternalAggregationWritePart']) > 0 AS spilled
FROM system.query_log
WHERE current_database = currentDatabase()
    AND log_comment = '04497_lowcard_spill_fallback'
    AND type = 'QueryFinish'
    AND event_date >= yesterday();
