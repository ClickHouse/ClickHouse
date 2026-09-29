-- `query_plan_window_functions_hash_partitioning` with more partitions in a stream than are grouped as the rows come,
-- so that the later rows are grouped in buckets at the end: each query must return the same with sorting (the first
-- line) and with hash partitioning (the second line).

SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64, x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number, (number * 7919) % 1000 FROM numbers(300000);

-- One row in each partition.
SELECT 'one row in each partition', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, uniqExact(x) OVER w, finalizeAggregation(sumState(x) OVER w) FROM t WINDOW w AS (PARTITION BY n))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'one row in each partition', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, uniqExact(x) OVER w, finalizeAggregation(sumState(x) OVER w) FROM t WINDOW w AS (PARTITION BY n))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

-- Two rows in each partition, one of them before the deferral.
SELECT 'two rows in each partition, one of them before the deferral', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, uniqExact(x) OVER w, finalizeAggregation(sumState(x) OVER w) FROM t WINDOW w AS (PARTITION BY n % 150000))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'two rows in each partition, one of them before the deferral', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, uniqExact(x) OVER w, finalizeAggregation(sumState(x) OVER w) FROM t WINDOW w AS (PARTITION BY n % 150000))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

-- Hot partitions and partitions of one row.
SELECT 'hot partitions and partitions of one row', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, uniqExact(x) OVER w, finalizeAggregation(sumState(x) OVER w) FROM t WINDOW w AS (PARTITION BY if(n % 3 = 0, n % 100, n)))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'hot partitions and partitions of one row', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w, uniqExact(x) OVER w, finalizeAggregation(sumState(x) OVER w) FROM t WINDOW w AS (PARTITION BY if(n % 3 = 0, n % 100, n)))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

-- String.
SELECT 'String', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY toString(n % 200000)))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'String', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY toString(n % 200000)))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

-- Keys packed into 16 bytes.
SELECT 'keys packed into 16 bytes', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY n % 150000, n % 2))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'keys packed into 16 bytes', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY n % 150000, n % 2))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

-- Keys packed into 32 bytes.
SELECT 'keys packed into 32 bytes', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY n % 150000, n % 2, toUInt64(n % 5)))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'keys packed into 32 bytes', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY n % 150000, n % 2, toUInt64(n % 5)))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

-- Serialized keys.
SELECT 'serialized keys', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY if(n % 5 = 0, NULL, n % 200000)))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'serialized keys', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY if(n % 5 = 0, NULL, n % 200000)))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

-- Spilling, which groups the deferred rows before the end.
SELECT 'spilling, which groups the deferred rows before the end', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w FROM t WINDOW w AS (PARTITION BY n % 200000))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'spilling, which groups the deferred rows before the end', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w, groupArraySorted(3)(x) OVER w FROM t WINDOW w AS (PARTITION BY n % 200000))
SETTINGS query_plan_window_functions_hash_partitioning = 1, max_bytes_before_external_sort = 1000000, max_bytes_ratio_before_external_sort = 0;

DROP TABLE t;
