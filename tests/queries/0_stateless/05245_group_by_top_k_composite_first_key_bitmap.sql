-- A numeric first column of a composite top-K key is checked per block, a `Nullable` one still per row:
-- both must skip exactly the same rows, with NaN first keys in every placement and with first-key ties.

SET serialize_query_plan = 0, enable_parallel_replicas = 0, log_queries = 1;
SET enable_group_by_top_k_optimization = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_rows_to_group_by = 0;
SET max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;
SET max_threads = 1, max_block_size = 1000;

-- NaNs arrive once the heap is full (`late`) or fill it first (`early`), so a per-block check that placed them wrong
-- would skip rows the per-row check keeps.
CREATE VIEW v AS
SELECT
    (intHash64(number) % 1000) / 7 AS x,
    if(number >= 50000 AND number % 7 = 0, nan, x) AS late,
    if(number < 1000 OR number % 7 = 0, nan, x) AS early,
    toUInt8(number % 3) AS t,
    toString(number % 13) AS b,
    toString(intHash64(number) % 100000) AS s
FROM numbers(100000);

-- `count` only, no aggregates, and `sum` take the three row loops of `Aggregator::executeImplBatch*`.
SELECT late, b, count() FROM v GROUP BY late, b ORDER BY late ASC NULLS FIRST, b LIMIT 3 SETTINGS log_comment = 'asc nan first';
SELECT toNullable(late) AS n, b, count() FROM v GROUP BY n, b ORDER BY n ASC NULLS FIRST, b LIMIT 3 SETTINGS log_comment = 'asc nan first' FORMAT Null;
SELECT early, b FROM v GROUP BY early, b ORDER BY early ASC NULLS LAST, b LIMIT 3 SETTINGS log_comment = 'asc nan last';
SELECT toNullable(early) AS n, b FROM v GROUP BY n, b ORDER BY n ASC NULLS LAST, b LIMIT 3 SETTINGS log_comment = 'asc nan last' FORMAT Null;
SELECT late, b, sum(t) FROM v GROUP BY late, b ORDER BY late DESC NULLS FIRST, b LIMIT 3 SETTINGS log_comment = 'desc nan first';
SELECT toNullable(late) AS n, b, sum(t) FROM v GROUP BY n, b ORDER BY n DESC NULLS FIRST, b LIMIT 3 SETTINGS log_comment = 'desc nan first' FORMAT Null;
SELECT early, b, count() FROM v GROUP BY early, b ORDER BY early DESC NULLS LAST, b LIMIT 3 SETTINGS log_comment = 'desc nan last';
SELECT toNullable(early) AS n, b, count() FROM v GROUP BY n, b ORDER BY n DESC NULLS LAST, b LIMIT 3 SETTINGS log_comment = 'desc nan last' FORMAT Null;
SELECT t, s, min(b) FROM v GROUP BY t, s ORDER BY t DESC, s LIMIT 3 SETTINGS log_comment = 'first key ties';
SELECT toNullable(t) AS n, s, min(b) FROM v GROUP BY n, s ORDER BY n DESC, s LIMIT 3 SETTINGS log_comment = 'first key ties' FORMAT Null;

-- All keys are constant, so the per-block check reads the one-row data column of a `ColumnConst`.
SELECT k, j, sum(number) FROM (SELECT 2::UInt32 AS k, 'x' AS j, number FROM numbers(10)) GROUP BY k, j ORDER BY k, j LIMIT 1 SETTINGS max_block_size = 2;

SYSTEM FLUSH LOGS query_log;

SELECT log_comment, count(), min(skipped) > 0, min(skipped) = max(skipped)
FROM
(
    SELECT log_comment, ProfileEvents['AggregationTopKRowsSkipped'] AS skipped
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('asc nan first', 'asc nan last', 'desc nan first', 'desc nan last', 'first key ties')
)
GROUP BY log_comment
ORDER BY log_comment;

DROP VIEW v;
