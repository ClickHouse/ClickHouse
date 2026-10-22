-- The radix partitioned `uniqExact` (`optimize_uniq_exact_radix_partitioning`) must return the same results as the ordinary aggregation.
-- A thread routes its values into the partitions only after it has seen enough distinct values, so most inputs have hundreds of thousands of them.

-- The arguments are kept as they are written, so that the strings and `Nullable` values reach `uniqExact`.
SET max_threads = 2, optimize_uniq_exact_radix_partitioning = 1, optimize_injective_functions_inside_uniq = 0, optimize_rewrite_aggregate_function_with_if = 0;

SELECT 'UInt64', uniqExact(number) FROM numbers_mt(400000);
SELECT 'UInt64 consecutive duplicates', uniqExact(intDiv(number, 3)) FROM numbers_mt(1200000);
SELECT 'UInt32', uniqExact(toUInt32(number)) FROM numbers_mt(1000000);
SELECT 'UInt16', uniqExact(toUInt16(number)) FROM numbers_mt(400000);
SELECT 'Int8', uniqExact(toInt8(number)) FROM numbers_mt(400000);
SELECT 'Float64', uniqExact(multiIf(number % 5 = 0, -0., number % 5 = 1, 0., number % 5 = 2, nan, number / 3)) FROM numbers_mt(400000);
SELECT 'Float32', uniqExact(toFloat32(number)) FROM numbers_mt(1000000);
SELECT 'String', uniqExact(if(number % 3 = 0, '', toString(number))) FROM numbers_mt(400000);
SELECT 'String consecutive duplicates', uniqExact(toString(intDiv(number, 3))) FROM numbers_mt(400000);
SELECT 'FixedString', uniqExact(toFixedString(toString(number), 8)) FROM numbers_mt(400000);
SELECT 'IPv6', uniqExact(toIPv6(toIPv4(number))) FROM numbers_mt(400000);
SELECT 'UUID', uniqExact(reinterpretAsUUID(toUInt128(number))) FROM numbers_mt(400000);
-- The values differ only in their high halves.
SELECT 'Int128', uniqExact(bitShiftLeft(toInt128(number), 64)) FROM numbers_mt(400000);
SELECT 'Nullable(Int64)', uniqExact(if(number % 4 = 0, NULL, toInt64(number))) FROM numbers_mt(400000);
SELECT 'Nullable(String)', uniqExact(if(number % 4 = 0, NULL, toString(number))) FROM numbers_mt(400000);
SELECT 'count distinct', count(DISTINCT number) FROM numbers_mt(400000);
-- A constant is routed once per block, and not at all for an empty block.
SELECT 'constant', uniqExact(42) FROM numbers_mt(400000);
SELECT 'constant Nullable', uniqExact(toNullable(42)) FROM numbers_mt(400000);
SELECT 'constant String', uniqExact('abc') FROM numbers_mt(400000);
SELECT 'constant for empty', uniqExact(42) FROM numbers_mt(400000) WHERE number < 0;

-- Most values are repeated, so the threads go back to inserting into their own sets.
SELECT 'repeated UInt64', uniqExact(toUInt64(number % 70000)) FROM numbers_mt(2000000);
SELECT 'repeated UInt32', uniqExact(toUInt32(number % 140000)) FROM numbers_mt(3000000);
SELECT 'repeated String', uniqExact(toString(number % 40000)) FROM numbers_mt(1000000);
-- Too many distinct values for the threads to go back to their own sets, so the routed values are deduplicated.
SELECT 'repeated many UInt64', uniqExact(toUInt64(number % 300000)) FROM numbers_mt(2000000);

-- Only the values of a sparse column that differ from the default one are routed, and the default one if there are such rows.
DROP TABLE IF EXISTS t_radix_uniq_sparse;
CREATE TABLE t_radix_uniq_sparse (x UInt64, s String) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_radix_uniq_sparse SELECT if(number % 4 = 0, number + 1, 0), if(number % 4 = 0, toString(number), '') FROM numbers(400000);
SELECT 'sparse', uniqExact(x) FROM t_radix_uniq_sparse;
SELECT 'sparse String', uniqExact(s) FROM t_radix_uniq_sparse;
SELECT 'sparse without defaults', uniqExact(x) FROM t_radix_uniq_sparse WHERE x != 0;
DROP TABLE t_radix_uniq_sparse;

-- The second run of a query uses the statistics of the first one: the threads start in the radix mode, or in a set
-- allocated for all their values, without deduplications if their values are distinct.
SELECT 'statistics radix', uniqExact(toString(number)) FROM numbers_mt(600000) SETTINGS collect_hash_table_stats_during_aggregation = 1;
SELECT 'statistics radix', uniqExact(toString(number)) FROM numbers_mt(600000) SETTINGS collect_hash_table_stats_during_aggregation = 1;
SELECT 'statistics set', uniqExact(number) FROM numbers_mt(400000) SETTINGS collect_hash_table_stats_during_aggregation = 1;
SELECT 'statistics set', uniqExact(number) FROM numbers_mt(400000) SETTINGS collect_hash_table_stats_during_aggregation = 1;
SELECT 'statistics repeated', uniqExact(toUInt64(number % 300000)) FROM numbers_mt(2000000) SETTINGS collect_hash_table_stats_during_aggregation = 1;
SELECT 'statistics repeated', uniqExact(toUInt64(number % 300000)) FROM numbers_mt(2000000) SETTINGS collect_hash_table_stats_during_aggregation = 1;

SELECT 'empty', uniqExact(number) FROM numbers_mt(400000) WHERE number > 1e9;
SELECT 'empty result for empty set', uniqExact(number) FROM numbers_mt(400000) WHERE number > 1e9 SETTINGS empty_result_for_aggregation_by_empty_set = 1;
SELECT 'only NULLs', uniqExact(if(number % 4 = 0, NULL, toInt64(number))) FROM numbers_mt(400000) WHERE number % 4 = 0 SETTINGS empty_result_for_aggregation_by_empty_set = 1;
-- `aggregate_functions_null_for_empty` does not add `OrNull` to `uniqExact`, because it returns 0 for an empty set.
SELECT 'null for empty, empty', uniqExact(number), count(DISTINCT number), toTypeName(uniqExact(number)) FROM numbers_mt(400000) WHERE number > 1e9 SETTINGS aggregate_functions_null_for_empty = 1;
SELECT 'null for empty', uniqExact(number) FROM numbers_mt(400000) SETTINGS aggregate_functions_null_for_empty = 1;
SELECT 'totals', uniqExact(number) FROM numbers_mt(400000) WITH TOTALS;
SELECT 'distributed', uniqExact(number) FROM remote('127.0.0.{1,2}', numbers_mt(400000));
SELECT 'single thread', uniqExact(number) FROM numbers_mt(400000) SETTINGS max_threads = 1;

-- The radix partitioned aggregation is used only for a single `uniqExact` of a supported type without keys.
-- It is not used where the aggregation is not finalized, such as on parallel replicas, or where `uniqExact` is rewritten to `GROUP BY`.
SET enable_parallel_replicas = 0, count_distinct_optimization = 0;

SELECT 'used', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(number) FROM numbers_mt(400000)) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'used with null for empty', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(number) FROM numbers_mt(400000) SETTINGS aggregate_functions_null_for_empty = 1) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'used with a single thread', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(number) FROM numbers_mt(400000) SETTINGS max_threads = 1) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'used for a constant', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(42) FROM numbers_mt(400000)) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'used for Nullable', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(toNullable(toString(number))) FROM numbers_mt(400000)) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'disabled', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(number) FROM numbers_mt(400000) SETTINGS optimize_uniq_exact_radix_partitioning = 0) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'LowCardinality', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(toLowCardinality(toString(number))) FROM numbers_mt(400000)) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'two aggregates', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(number), uniqExact(number % 7) FROM numbers_mt(400000)) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'GROUP BY', count() > 0 FROM (EXPLAIN PIPELINE SELECT number % 10 AS k, uniqExact(number) FROM numbers_mt(400000) GROUP BY k) WHERE explain LIKE '%RadixUniqExact%';
SELECT 'two arguments', count() > 0 FROM (EXPLAIN PIPELINE SELECT uniqExact(number, toString(number)) FROM numbers_mt(400000)) WHERE explain LIKE '%RadixUniqExact%';

-- The aggregation that only merges the states of an aggregate projection is left as is.
DROP TABLE IF EXISTS t_radix_uniq_projection;
CREATE TABLE t_radix_uniq_projection (x UInt64, PROJECTION p (SELECT uniqExact(x))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_radix_uniq_projection SELECT number FROM numbers(100000);
INSERT INTO t_radix_uniq_projection SELECT number * 2 FROM numbers(100000);
SELECT 'projection', uniqExact(x) FROM t_radix_uniq_projection;
DROP TABLE t_radix_uniq_projection;
