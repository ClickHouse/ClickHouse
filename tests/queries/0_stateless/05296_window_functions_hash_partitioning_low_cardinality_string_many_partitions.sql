-- `query_plan_window_functions_hash_partitioning` with `LowCardinality` keys of more partitions in a stream than are
-- grouped as the rows come: the serialized keys are read from their dictionaries. Each query must return the same with
-- sorting (the first line) and with hash partitioning (the second line).

SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SET max_threads = 1;
SET max_rows_to_sort = 0, max_bytes_to_sort = 0;

DROP TABLE IF EXISTS t;
CREATE TABLE t (n UInt64, s LowCardinality(String), ns LowCardinality(Nullable(String)), x Int64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT number, toString(number % 150000), if(number % 7 = 0, NULL, toString(number % 100000)), (number * 7919) % 1000 FROM numbers(300000);

SELECT 'deferred', sum(cityHash64(*)) FROM (SELECT n, s, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY s))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'deferred', sum(cityHash64(*)) FROM (SELECT n, s, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY s))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

SELECT 'spilled', sum(cityHash64(*)) FROM (SELECT n, s, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY s))
SETTINGS query_plan_window_functions_hash_partitioning = 0, max_bytes_before_external_sort = 1000000, max_bytes_ratio_before_external_sort = 0;
SELECT 'spilled', sum(cityHash64(*)) FROM (SELECT n, s, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY s))
SETTINGS query_plan_window_functions_hash_partitioning = 1, max_bytes_before_external_sort = 1000000, max_bytes_ratio_before_external_sort = 0;

SELECT 'composite', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY s, n % 2))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'composite', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY s, n % 2))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

SELECT 'nullable', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY ns))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'nullable', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY ns))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

SELECT 'nullable composite', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY ns, n % 3))
SETTINGS query_plan_window_functions_hash_partitioning = 0;
SELECT 'nullable composite', sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t WINDOW w AS (PARTITION BY ns, n % 3))
SETTINGS query_plan_window_functions_hash_partitioning = 1;

DROP TABLE t;
