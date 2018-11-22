DROP TABLE IF EXISTS test.size_hint;
CREATE TABLE test.size_hint (s Array(String)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1000;

SET max_block_size = 1000;
SET max_memory_usage = 1000000000;
INSERT INTO test.size_hint SELECT arrayMap(x -> 'Hello', range(1000)) FROM numbers(10000);

SET max_memory_usage = 100000000, max_threads = 2;
SELECT count(), sum(length(s)) FROM test.size_hint;

DROP TABLE test.size_hint;
