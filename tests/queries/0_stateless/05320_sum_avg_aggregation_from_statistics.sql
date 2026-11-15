-- Tags: no-parallel-replicas
-- no-parallel-replicas: the test checks EXPLAIN of plans with reading steps replaced by prepared sources.

-- `sum`, `avg` and `sumCount` are answered from per-part `basic` statistics, and `count(column)` from the row counts,
-- with exactly the result of reading the data, also when the sums wrap around within a part and across parts.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SET optimize_syntax_fuse_functions = 1, optimize_arithmetic_operations_in_aggregate_functions = 1;
-- `use_statistics_for_min_max_aggregation` and `use_statistics_for_sum_avg_aggregation` keep their defaults, because
-- `compatibility` below does not change settings that were set explicitly.
SET materialize_statistics_on_insert = 1, mutations_sync = 2;

DROP TABLE IF EXISTS t_sum;
DROP TABLE IF EXISTS t_late_column;

CREATE TABLE t_sum
(
    i8 Int8, i16 Int16, i32 Int32, i64 Int64, i128 Int128, i256 Int256,
    u8 UInt8, u16 UInt16, u32 UInt32, u64 UInt64, u128 UInt128, u256 UInt256, b Bool,
    d32 Decimal32(2), d64 Decimal64(4), d128 Decimal128(0), d256 Decimal256(4),
    f Float64, n Nullable(Int32)
)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic';

SYSTEM STOP MERGES t_sum;

INSERT INTO t_sum SELECT
    number * 7 - 300, number * 1001 - 50000, number * 1000003 - 500000, number * 1000000007 - 500000000000,
    number * 1000000007 - 500000000000, number * 1000000007 - 500000000000,
    number, number * 61, number * 1000003, number * 1000000007, number * 1000000007, number * 1000000007, number % 3 = 0,
    number / 7 - 50, number / 13 - 50, number * 1000000007 - 500000000000, number / 17 - 50, number / 3, number
FROM numbers(1000);

-- the values near the upper bounds make the sums of the 64-bit integer, 128-bit and 256-bit columns wrap around
INSERT INTO t_sum SELECT
    127, 32767, 2147483647, 9223372036854775807 - number,
    toInt128('170141183460469231731687303715884105727') - number,
    toInt256('57896044618658097711785492504343953926634992332820282019728792003956564819967') - number,
    255, 65535, 4294967295, 18446744073709551615 - number,
    toUInt128('340282366920938463463374607431768211455') - number,
    toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') - number, 1,
    toDecimal32('9999999.99', 2), toDecimal64('99999999999999.9999', 4), toDecimal128('9e37', 0), toDecimal256('9e71', 4),
    1e300, NULL
FROM numbers(7);

SELECT 'answered from statistics, the same as without them';
SELECT sum(i8), sum(i16), sum(i32), sum(i64), sum(i128), sum(i256), sum(u8), sum(u16), sum(u32), sum(u64), sum(u128), sum(u256), sum(b), sum(d32), sum(d64), sum(d128), sum(d256) FROM t_sum;
SELECT sum(i8), sum(i16), sum(i32), sum(i64), sum(i128), sum(i256), sum(u8), sum(u16), sum(u32), sum(u64), sum(u128), sum(u256), sum(b), sum(d32), sum(d64), sum(d128), sum(d256) FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT avg(i8), avg(i16), avg(i32), avg(i64), avg(i128), avg(i256), avg(u8), avg(u16), avg(u32), avg(u64), avg(u128), avg(u256), avg(b), avg(d32), avg(d64), avg(d128), avg(d256) FROM t_sum;
SELECT avg(i8), avg(i16), avg(i32), avg(i64), avg(i128), avg(i256), avg(u8), avg(u16), avg(u32), avg(u64), avg(u128), avg(u256), avg(b), avg(d32), avg(d64), avg(d128), avg(d256) FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT sumCount(i8), sumCount(i64), sumCount(u256), sumCount(d32), sumCount(d256), count(f), count(u128) FROM t_sum;
SELECT sumCount(i8), sumCount(i64), sumCount(u256), sumCount(d32), sumCount(d256), count(f), count(u128) FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(i8), avg(i256), sumCount(d128), count(f) FROM t_sum);

SELECT 'sums and averages of expressions that the analyzer rewrites to aggregates of columns';
SELECT sum(i16), sum(i16 + 1), sum(i16 + 2), sum(i16 + 3) FROM t_sum;
SELECT sum(i16), sum(i16 + 1), sum(i16 + 2), sum(i16 + 3) FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT count() FROM (EXPLAIN actions = 1 SELECT sum(i16), sum(i16 + 1) FROM t_sum) WHERE explain LIKE '%Aggregates: sumCount(i16)%';
SELECT sum(i16 - 3), avg(i16), avg(i64 + 1), sum(i32 * 2) FROM t_sum;
SELECT sum(i16 - 3), avg(i16), avg(i64 + 1), sum(i32 * 2) FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(i8), count(), avg(i16), avg(i64), sum(i16 + 1), sum(i32 * 2) FROM t_sum);
SELECT sum(d64) FROM t_sum SETTINGS force_optimize_projection_name = '_statistics_projection';

SELECT 'a part without statistics is read, the others are answered from statistics';
INSERT INTO t_sum (i64, u256, d128) SETTINGS materialize_statistics_on_insert = 0 VALUES (-5, 5, -5);
SELECT sum(i64), avg(u256), sumCount(d128) FROM t_sum;
SELECT sum(i64), avg(u256), sumCount(d128) FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(i64), avg(u256), sumCount(d128) FROM t_sum);

-- `ATTACH` drops `SYSTEM STOP MERGES`, so only the use of the statistics is checked here, not which parts are read
SELECT 'statistics loaded from disk';
DETACH TABLE t_sum;
ATTACH TABLE t_sum;
SELECT sum(i128), avg(u64), sumCount(d256) FROM t_sum;
SELECT sum(i128), avg(u64), sumCount(d256) FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(i128), avg(u64), sumCount(d256) FROM t_sum);

SELECT 'the merged part has the sum of the merged parts and of the rebuilt one';
OPTIMIZE TABLE t_sum FINAL;
SELECT sum(i64), sum(u64), avg(i256), sumCount(d128) FROM t_sum;
SELECT sum(i64), sum(u64), avg(i256), sumCount(d128) FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(i64), sum(u64), avg(i256), sumCount(d128) FROM t_sum);

SELECT 'each kind of aggregate needs its own setting, and count() works with either';
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT min(i32), sum(i64) FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT min(i32), count() FROM t_sum SETTINGS use_statistics_for_sum_avg_aggregation = 0);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT min(i32), sum(i64) FROM t_sum SETTINGS use_statistics_for_min_max_aggregation = 0);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(i64), count() FROM t_sum SETTINGS use_statistics_for_min_max_aggregation = 0);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(i64) FROM t_sum SETTINGS compatibility = '26.9');
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT min(i64) FROM t_sum SETTINGS compatibility = '26.9');

SELECT 'count of a column that cannot be NULL';
SELECT sum(i16), count(i16), min(i32), count(d256) FROM t_sum SETTINGS optimize_syntax_fuse_functions = 0;
SELECT sum(i16), count(i16), min(i32), count(d256) FROM t_sum SETTINGS optimize_syntax_fuse_functions = 0, use_statistics_for_sum_avg_aggregation = 0;
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(i16), count(i16), min(i32), count(d256) FROM t_sum SETTINGS optimize_syntax_fuse_functions = 0);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT min(i32), count(n) FROM t_sum);

SELECT 'not applied';
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(f) FROM t_sum);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(n) FROM t_sum);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sumWithOverflow(i64) FROM t_sum);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sumKahan(i64) FROM t_sum);
SELECT countIf(explain LIKE '%_statistics_projection%') FROM (EXPLAIN SELECT sum(i64) FROM t_sum GROUP BY b);

SELECT 'a column added after the part was written is read';
CREATE TABLE t_late_column (id UInt64) ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = 'basic', min_bytes_for_wide_part = 0;
INSERT INTO t_late_column SELECT number FROM numbers(4);
ALTER TABLE t_late_column ADD COLUMN c Int32 DEFAULT 5;
ALTER TABLE t_late_column MATERIALIZE STATISTICS ALL;
ALTER TABLE t_late_column MODIFY COLUMN c Int32 DEFAULT 7;
SYSTEM STOP MERGES t_late_column;
INSERT INTO t_late_column SELECT number, 100 FROM numbers(2);
SELECT sum(c), avg(c) FROM t_late_column;
SELECT countIf(explain LIKE '%_statistics_projection%'), countIf(explain LIKE '%ReadFromMergeTree%')
FROM (EXPLAIN SELECT sum(c), avg(c) FROM t_late_column);

DROP TABLE t_sum;
DROP TABLE t_late_column;
