-- `query_plan_window_functions_hash_partitioning` with large partitions next to partitions of one or two rows: the
-- result of a large partition is taken once, and not for each of its rows, also in the chunks with rows of partitions of
-- one row. Each query must return the same with sorting (the first line) and with hash partitioning (the second line).

SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SET max_threads = 1;
SET max_rows_to_sort = 0, max_bytes_to_sort = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64, x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number, (number * 7919) % 1000 FROM numbers(300000);

SELECT 'one large partition and partitions of one row', sum(cityHash64(*)) FROM (SELECT n, quantileExact(0.5)(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, finalizeAggregation(uniqExactState(x % 5) OVER w) FROM t WINDOW w AS (PARTITION BY if(n < 50000, 0, n)))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'one large partition and partitions of one row', sum(cityHash64(*)) FROM (SELECT n, quantileExact(0.5)(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, finalizeAggregation(uniqExactState(x % 5) OVER w) FROM t WINDOW w AS (PARTITION BY if(n < 50000, 0, n)))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

SELECT 'large partitions and partitions of one or two rows, deferred', sum(cityHash64(*)) FROM (SELECT n, quantileExact(0.5)(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, finalizeAggregation(uniqExactState(x % 5) OVER w) FROM t WINDOW w AS (PARTITION BY if(n < 70000, n, if(n % 2 = 0, n % 10, intDiv(n, 2)))))
SETTINGS query_plan_window_functions_hash_partitioning = 0, max_block_size = 1000;
SELECT 'large partitions and partitions of one or two rows, deferred', sum(cityHash64(*)) FROM (SELECT n, quantileExact(0.5)(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, finalizeAggregation(uniqExactState(x % 5) OVER w) FROM t WINDOW w AS (PARTITION BY if(n < 70000, n, if(n % 2 = 0, n % 10, intDiv(n, 2)))))
SETTINGS query_plan_window_functions_hash_partitioning = 1, max_block_size = 1000;

SELECT 'spilled', sum(cityHash64(*)) FROM (SELECT n, quantileExact(0.5)(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, finalizeAggregation(uniqExactState(x % 5) OVER w) FROM t WINDOW w AS (PARTITION BY if(n % 3 = 0, n % 7, n)))
SETTINGS query_plan_window_functions_hash_partitioning = 0, max_bytes_before_external_sort = 1000000, max_bytes_ratio_before_external_sort = 0;
SELECT 'spilled', sum(cityHash64(*)) FROM (SELECT n, quantileExact(0.5)(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, finalizeAggregation(uniqExactState(x % 5) OVER w) FROM t WINDOW w AS (PARTITION BY if(n % 3 = 0, n % 7, n)))
SETTINGS query_plan_window_functions_hash_partitioning = 1, max_bytes_before_external_sort = 1000000, max_bytes_ratio_before_external_sort = 0;

DROP TABLE t;
