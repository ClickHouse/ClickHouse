SET optimize_trivial_insert_select = 1;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0.0;

-- Prevent remote replicas from skipping index analysis in Parallel Replicas. Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;

DROP TABLE IF EXISTS test;
CREATE TABLE test (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS index_granularity = 1000, index_granularity_bytes = '10Mi';
INSERT INTO test SELECT * FROM numbers(1000000);
OPTIMIZE TABLE test;

SET max_rows_to_read = 2000;
SELECT count() FROM test WHERE x = 100000;
SET max_rows_to_read = 1000000;
SELECT count() FROM test WHERE x != 100000;
SET max_rows_to_read = 101000;
SELECT count() FROM test WHERE x < 100000;
SET max_rows_to_read = 900000;
SELECT count() FROM test WHERE x > 100000;
SET max_rows_to_read = 101000;
SELECT count() FROM test WHERE x <= 100000;
SET max_rows_to_read = 901000;
SELECT count() FROM test WHERE x >= 100000;

SET max_rows_to_read = 2000;
SELECT count() FROM test WHERE x = '100000';
SET max_rows_to_read = 1000000;
SELECT count() FROM test WHERE x != '100000';
SET max_rows_to_read = 101000;
SELECT count() FROM test WHERE x < '100000';
SET max_rows_to_read = 900000;
SELECT count() FROM test WHERE x > '100000';
SET max_rows_to_read = 101000;
SELECT count() FROM test WHERE x <= '100000';
SET max_rows_to_read = 901000;
SELECT count() FROM test WHERE x >= '100000';

DROP TABLE test;
