SET max_bytes_before_external_group_by = 100000000;
SET max_memory_usage = 1000000000;
SELECT URL, uniq(SearchPhrase) AS u FROM test.hits GROUP BY URL ORDER BY u DESC, URL LIMIT 10;

SET max_memory_usage = 300000000;
SET aggregation_memory_efficient_merge_threads = 1;
SET max_threads = 2;
SELECT URL, uniq(SearchPhrase) AS u FROM test.hits GROUP BY URL ORDER BY u DESC, URL LIMIT 10;
