-- `query_plan_window_functions_hash_partitioning` with `LowCardinality` `PARTITION BY` keys: `LowCardinality(UInt32)`
-- used to be packed as a fixed-size key, reading the `ColumnLowCardinality` as if it were a `ColumnVector`.
-- Each query must return the same with sorting and with hash partitioning.

SET query_plan_reuse_storage_ordering_for_window_functions = 0;
SET max_rows_to_sort = 0, max_bytes_to_sort = 0;
SET explain_query_plan_default = 'legacy';
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_hash_window_low_cardinality;
CREATE TABLE t_hash_window_low_cardinality
(
    n UInt64,
    x Int64,
    u8 LowCardinality(UInt8),
    u32 LowCardinality(UInt32),
    nu32 LowCardinality(Nullable(UInt32)),
    s LowCardinality(String),
    ns LowCardinality(Nullable(String)),
    fs LowCardinality(FixedString(3))
)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_hash_window_low_cardinality
SELECT
    number,
    (number * 7919) % 1000,
    number % 7,
    number % 100,
    if(number % 11 = 0, NULL, number % 50),
    toString(number % 100),
    if(number % 13 = 0, NULL, toString(number % 30)),
    toFixedString(toString(number % 40 + 100), 3)
FROM numbers(100000);

-- { echo }

SELECT trim(explain) FROM (EXPLAIN actions = 1 SELECT u32, sum(x) OVER (PARTITION BY u32) FROM t_hash_window_low_cardinality SETTINGS query_plan_window_functions_hash_partitioning = 1)
WHERE explain LIKE '%Hash partitioning%';

-- One fixed-size key.
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, u32, sum(x) OVER w, count() OVER w, any(s) OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY u32)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, u32, sum(x) OVER w, count() OVER w, any(s) OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY u32)) SETTINGS query_plan_window_functions_hash_partitioning = 1);
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, fs, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY fs)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, fs, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY fs)) SETTINGS query_plan_window_functions_hash_partitioning = 1);
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY toLowCardinality(n % 10))) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY toLowCardinality(n % 10))) SETTINGS query_plan_window_functions_hash_partitioning = 1);

-- Several fixed-size keys, packed into `UInt64` and `UInt128`.
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY u8, u32)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY u8, u32)) SETTINGS query_plan_window_functions_hash_partitioning = 1);
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY u32, n % 3, fs)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY u32, n % 3, fs)) SETTINGS query_plan_window_functions_hash_partitioning = 1);

-- Serialized keys.
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, nu32, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY nu32)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, nu32, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY nu32)) SETTINGS query_plan_window_functions_hash_partitioning = 1);
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, s, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY s)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, s, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY s)) SETTINGS query_plan_window_functions_hash_partitioning = 1);
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, ns, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY ns)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, ns, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY ns)) SETTINGS query_plan_window_functions_hash_partitioning = 1);
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY s, nu32, u8)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY s, nu32, u8)) SETTINGS query_plan_window_functions_hash_partitioning = 1);

-- More partitions than are grouped as the rows come.
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY toLowCardinality(toUInt32(n % 50000)))) SETTINGS query_plan_window_functions_hash_partitioning = 0, max_threads = 1)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY toLowCardinality(toUInt32(n % 50000)))) SETTINGS query_plan_window_functions_hash_partitioning = 1, max_threads = 1);
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY toLowCardinality(toString(n % 50000)))) SETTINGS query_plan_window_functions_hash_partitioning = 0, max_threads = 1)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY toLowCardinality(toString(n % 50000)))) SETTINGS query_plan_window_functions_hash_partitioning = 1, max_threads = 1);

-- Spilling to disk.
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, u32, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY u32)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, u32, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY u32)) SETTINGS query_plan_window_functions_hash_partitioning = 1, max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0, max_block_size = 1000);
SELECT (SELECT sum(cityHash64(*)) FROM (SELECT n, s, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY s)) SETTINGS query_plan_window_functions_hash_partitioning = 0)
    = (SELECT sum(cityHash64(*)) FROM (SELECT n, s, sum(x) OVER w, count() OVER w FROM t_hash_window_low_cardinality WINDOW w AS (PARTITION BY s)) SETTINGS query_plan_window_functions_hash_partitioning = 1, max_bytes_before_external_sort = 1, max_bytes_ratio_before_external_sort = 0, max_block_size = 1000);

DROP TABLE t_hash_window_low_cardinality;
