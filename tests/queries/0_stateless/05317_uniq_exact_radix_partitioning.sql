-- The radix partitioned `uniqExact` (`optimize_uniq_exact_radix_partitioning`) must return the same results as the ordinary aggregation.
-- Every pair of values is computed with the setting enabled and disabled.

SET max_threads = 8;

DROP TABLE IF EXISTS t_radix_uniq;

CREATE TABLE t_radix_uniq
(
    n UInt64,
    i64 Int64,
    u32 UInt32,
    i8 Int8,
    f64 Float64,
    f32 Float32,
    s String,
    fs FixedString(8),
    uuid UUID,
    i128 Int128,
    ip4 IPv4,
    ip6 IPv6,
    d Date,
    dt DateTime,
    e Enum8('a' = 1, 'b' = 2),
    ni Nullable(Int64),
    ns Nullable(String),
    lc LowCardinality(String),
    z UInt64
)
ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1024;

INSERT INTO t_radix_uniq SELECT
    number,
    intHash64(number) % 300000 - 150000,
    number % 1000,
    number,
    multiIf(number % 5 = 0, -0., number % 5 = 1, 0., number % 5 = 2, nan, (number % 77777) / 3),
    (number % 50000) / 7,
    if(number % 3 = 0, '', toString(intHash64(number) % 200000)),
    toFixedString(toString(number % 100000), 8),
    reinterpretAsUUID(toUInt128(intHash64(number) % 250000) * 1000003),
    bitShiftLeft(toInt128(intHash64(number) % 250000), 64) + number % 3,
    toIPv4(toUInt32(intHash64(number) % 100000)),
    toIPv6(IPv4NumToString(toUInt32(intHash64(number) % 100000))),
    toDate(number % 3000),
    toDateTime(intHash64(number) % 400000),
    number % 2 + 1,
    if(number % 4 = 0, NULL, intHash64(number) % 200000),
    if(number % 4 = 0, NULL, toString(number % 150000)),
    toString(number % 1000),
    0
FROM numbers(2000000);

SELECT 'unique UInt64', uniqExact(n) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'unique UInt64', uniqExact(n) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'repeated Int64', uniqExact(i64) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'repeated Int64', uniqExact(i64) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'UInt32', uniqExact(u32) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'UInt32', uniqExact(u32) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'Int8', uniqExact(i8) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'Int8', uniqExact(i8) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'Float64', uniqExact(f64) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'Float64', uniqExact(f64) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'Float32', uniqExact(f32) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'Float32', uniqExact(f32) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'String', uniqExact(s) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'String', uniqExact(s) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'FixedString', uniqExact(fs) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'FixedString', uniqExact(fs) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'UUID', uniqExact(uuid) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'UUID', uniqExact(uuid) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'Int128', uniqExact(i128) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'Int128', uniqExact(i128) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'IPv4', uniqExact(ip4) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'IPv4', uniqExact(ip4) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'IPv6', uniqExact(ip6) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'IPv6', uniqExact(ip6) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'Date', uniqExact(d) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'Date', uniqExact(d) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'DateTime', uniqExact(dt) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'DateTime', uniqExact(dt) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'Enum8', uniqExact(e) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'Enum8', uniqExact(e) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'Nullable(Int64)', uniqExact(ni) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'Nullable(Int64)', uniqExact(ni) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'Nullable(String)', uniqExact(ns) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'Nullable(String)', uniqExact(ns) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'LowCardinality(String)', uniqExact(lc) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'LowCardinality(String)', uniqExact(lc) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'zero', uniqExact(z) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'zero', uniqExact(z) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'expression', uniqExact(n % 123457) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'expression', uniqExact(n % 123457) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'count distinct', count(DISTINCT s) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'count distinct', count(DISTINCT s) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'filter', uniqExact(i64) FROM t_radix_uniq WHERE n % 3 = 1 SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'filter', uniqExact(i64) FROM t_radix_uniq WHERE n % 3 = 1 SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'numbers_mt', uniqExact(number) FROM numbers_mt(1000000) SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'numbers_mt', uniqExact(number) FROM numbers_mt(1000000) SETTINGS optimize_uniq_exact_radix_partitioning = 0;

-- Many repeated values in every stream: the streams go back to inserting into their own sets.
SELECT 'repeated UInt32', uniqExact(intHash64(number) % 200000) FROM numbers_mt(20000000) SETTINGS max_threads = 4, optimize_uniq_exact_radix_partitioning = 1;
SELECT 'repeated UInt32', uniqExact(intHash64(number) % 200000) FROM numbers_mt(20000000) SETTINGS max_threads = 4, optimize_uniq_exact_radix_partitioning = 0;
SELECT 'repeated UInt64', uniqExact(toUInt64(intHash64(number) % 200000)) FROM numbers_mt(20000000) SETTINGS max_threads = 4, optimize_uniq_exact_radix_partitioning = 1;
SELECT 'repeated UInt64', uniqExact(toUInt64(intHash64(number) % 200000)) FROM numbers_mt(20000000) SETTINGS max_threads = 4, optimize_uniq_exact_radix_partitioning = 0;
SELECT 'repeated String', uniqExact(toString(intHash64(number) % 200000)) FROM numbers_mt(10000000) SETTINGS max_threads = 4, optimize_uniq_exact_radix_partitioning = 1;
SELECT 'repeated String', uniqExact(toString(intHash64(number) % 200000)) FROM numbers_mt(10000000) SETTINGS max_threads = 4, optimize_uniq_exact_radix_partitioning = 0;

SELECT 'empty', uniqExact(n) FROM t_radix_uniq WHERE n > 1e9 SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'empty', uniqExact(n) FROM t_radix_uniq WHERE n > 1e9 SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'empty result for empty set', uniqExact(n) FROM t_radix_uniq WHERE n > 1e9 SETTINGS optimize_uniq_exact_radix_partitioning = 1, empty_result_for_aggregation_by_empty_set = 1;
SELECT 'empty result for empty set', uniqExact(n) FROM t_radix_uniq WHERE n > 1e9 SETTINGS optimize_uniq_exact_radix_partitioning = 0, empty_result_for_aggregation_by_empty_set = 1;
-- `aggregate_functions_null_for_empty` does not add `OrNull` to `uniqExact`, because it returns 0 for an empty set.
SELECT 'null for empty, empty', uniqExact(i64), count(DISTINCT i64) FROM t_radix_uniq WHERE n > 1e9 SETTINGS optimize_uniq_exact_radix_partitioning = 1, aggregate_functions_null_for_empty = 1;
SELECT 'null for empty, empty', uniqExact(i64), count(DISTINCT i64) FROM t_radix_uniq WHERE n > 1e9 SETTINGS optimize_uniq_exact_radix_partitioning = 0, aggregate_functions_null_for_empty = 1;
SELECT 'null for empty, type', toTypeName(uniqExact(i64)) FROM t_radix_uniq WHERE n > 1e9 SETTINGS optimize_uniq_exact_radix_partitioning = 1, aggregate_functions_null_for_empty = 1;
SELECT 'null for empty, type', toTypeName(uniqExact(i64)) FROM t_radix_uniq WHERE n > 1e9 SETTINGS optimize_uniq_exact_radix_partitioning = 0, aggregate_functions_null_for_empty = 1;
SELECT 'null for empty', uniqExact(i64) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 1, aggregate_functions_null_for_empty = 1;
SELECT 'null for empty', uniqExact(i64) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0, aggregate_functions_null_for_empty = 1;
SELECT 'only NULLs', uniqExact(ni) FROM t_radix_uniq WHERE n % 4 = 0 SETTINGS optimize_uniq_exact_radix_partitioning = 1, empty_result_for_aggregation_by_empty_set = 1;
SELECT 'only NULLs', uniqExact(ni) FROM t_radix_uniq WHERE n % 4 = 0 SETTINGS optimize_uniq_exact_radix_partitioning = 0, empty_result_for_aggregation_by_empty_set = 1;
SELECT 'totals', uniqExact(i64) FROM t_radix_uniq WITH TOTALS SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'totals', uniqExact(i64) FROM t_radix_uniq WITH TOTALS SETTINGS optimize_uniq_exact_radix_partitioning = 0;
SELECT 'distributed', uniqExact(i64) FROM remote('127.0.0.{1,2}', currentDatabase(), t_radix_uniq) SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'distributed', uniqExact(i64) FROM remote('127.0.0.{1,2}', currentDatabase(), t_radix_uniq) SETTINGS optimize_uniq_exact_radix_partitioning = 0;

-- The radix partitioned aggregation is used only for a single `uniqExact` of a supported type without keys.
-- It is not used where the aggregation is not finalized, such as on parallel replicas, or where `uniqExact` is rewritten to `GROUP BY`.
SET enable_parallel_replicas = 0, count_distinct_optimization = 0, optimize_uniq_exact_radix_partitioning = 1;

SELECT 'used', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(i64) FROM t_radix_uniq) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'used with null for empty', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(i64) FROM t_radix_uniq SETTINGS aggregate_functions_null_for_empty = 1) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'used for Nullable', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(ns) FROM t_radix_uniq) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'disabled', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(i64) FROM t_radix_uniq SETTINGS optimize_uniq_exact_radix_partitioning = 0) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'LowCardinality', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(lc) FROM t_radix_uniq) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'two aggregates', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(i64), uniqExact(s) FROM t_radix_uniq) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'GROUP BY', count() > 0 FROM (EXPLAIN PIPELINE SELECT u32, uniqExact(i64) FROM t_radix_uniq GROUP BY u32) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'two arguments', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(i64, s) FROM t_radix_uniq) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'single thread', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(i64) FROM t_radix_uniq SETTINGS max_threads = 1) WHERE explain LIKE '%RadixUniqExact%';

DROP TABLE t_radix_uniq;

-- The aggregation that only merges the states of an aggregate projection is left as is.
DROP TABLE IF EXISTS t_radix_uniq_projection;
CREATE TABLE t_radix_uniq_projection (x UInt64, PROJECTION p (SELECT uniqExact(x))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_radix_uniq_projection SELECT number FROM numbers(100000);
INSERT INTO t_radix_uniq_projection SELECT number * 2 FROM numbers(100000);
SELECT 'projection', uniqExact(x) FROM t_radix_uniq_projection SETTINGS optimize_uniq_exact_radix_partitioning = 1;
SELECT 'projection', uniqExact(x) FROM t_radix_uniq_projection SETTINGS optimize_uniq_exact_radix_partitioning = 0;
DROP TABLE t_radix_uniq_projection;
