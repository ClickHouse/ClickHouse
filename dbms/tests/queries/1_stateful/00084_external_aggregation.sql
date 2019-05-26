SET max_bytes_before_external_group_by = 200000000;

SET max_memory_usage = 1000000000;
SET max_threads = 12;
SELECT URL, uniq(SearchPhrase) AS u FROM test.hits GROUP BY URL ORDER BY u DESC, URL LIMIT 10;

SET max_memory_usage = 300000000;
SET max_threads = 2;
SET aggregation_memory_efficient_merge_threads = 1;
SELECT URL, uniq(SearchPhrase) AS u FROM test.hits GROUP BY URL ORDER BY u DESC, URL LIMIT 10;
