DROP TABLE IF EXISTS test;
CREATE TABLE test (d DateTime, PRIMARY KEY (d));
INSERT INTO test SELECT toDateTime('2024-01-01') + number FROM numbers(1e6);
SET max_rows_to_read = 10000;

-- Prevent remote replicas from skipping index analysis in Parallel Replicas. Otherwise, they may return full ranges and trigger max_rows_to_read validation failures.
SET parallel_replicas_index_analysis_only_on_coordinator = 0;

SELECT count() FROM test WHERE d <= '2024-01-01 02:03:04';
SELECT count() FROM test WHERE d <= toDateTime('2024-01-01 02:03:04');
SELECT count() FROM test WHERE d <= toDateTime64('2024-01-01 02:03:04', 0);
SELECT count() FROM test WHERE d <= toDateTime64('2024-01-01 02:03:04', 3);
SET max_rows_to_read = 100_000;
SELECT count() FROM test WHERE d <= '2024-01-02';
SELECT count() FROM test WHERE d <= toDate('2024-01-02');
DROP TABLE test;
