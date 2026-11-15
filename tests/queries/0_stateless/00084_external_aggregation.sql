-- Tags: stateful, no-flaky-check
-- no-flaky-check: times out

SET max_bytes_before_external_group_by = 200000000;
SET max_bytes_ratio_before_external_group_by = 0;

SET max_memory_usage = 1500000000;
SET max_threads = 12;
SELECT URL, uniq(SearchPhrase) AS u FROM test.hits GROUP BY URL ORDER BY u DESC, URL LIMIT 10;

-- The query below peaks at around 260 MiB when it spills, and at around 855 MiB when it does not,
-- so `max_memory_usage` has to sit between the two for the test to keep checking that the
-- aggregation goes to disk. It used to be 300000000, which left only a few percent of headroom
-- over the spilling peak and made the test fail with `MEMORY_LIMIT_EXCEEDED` from time to time.
-- Anything that adds to the query on top of the aggregation state was enough to cross the limit,
-- in particular hash tables preallocated from `collect_hash_table_stats_during_aggregation`,
-- which add over 100 MiB once the statistics cache holds an entry for this aggregation.
SET max_memory_usage = 600000000;
SET max_threads = 2;
SET aggregation_memory_efficient_merge_threads = 1;
SELECT URL, uniq(SearchPhrase) AS u FROM test.hits GROUP BY URL ORDER BY u DESC, URL LIMIT 10;
