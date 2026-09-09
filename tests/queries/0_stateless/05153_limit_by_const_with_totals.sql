-- A constant LIMIT BY key must not cancel aggregation before the totals row is complete.
SET output_format_write_statistics = 0;
SET group_by_two_level_threshold = 1;

SELECT ignore(number), count(), 'grp' AS k
FROM numbers_mt(1000)
GROUP BY number WITH TOTALS
LIMIT 1 BY k
LIMIT 1
FORMAT JSONCompact
SETTINGS max_threads = 1, max_block_size = 10, enable_analyzer = 0;

-- Cover the sorted-stream path and the per-stream LIMIT BY sort pushdown.
SELECT number, count(), identity(1) AS k
FROM numbers_mt(1000)
GROUP BY number WITH TOTALS
ORDER BY k, number
LIMIT 1 BY k
LIMIT 1
FORMAT JSONCompact
SETTINGS max_threads = 4, max_block_size = 10, query_plan_push_limit_by_into_sort = 1, enable_analyzer = 1;
