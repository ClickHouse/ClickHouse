-- An ordering comparison against a string constant holding an integer outside the other operand's range is
-- decided by the direction of the overflow, because every value of the type is on one side of the constant.
-- It used to be false in both directions, so `x <= C` and `NOT (x <= C)` returned the same rows and a plan
-- that rewrites a negated comparison into its complement disagreed with the predicate evaluated per row.
-- A constant too large for the widest integer type also used to wrap around, so an equality, an `IN` set, a
-- primary key range and `ALTER TABLE ... DROP PARTITION` matched an unrelated value.

SELECT 'above max', toInt32(0) < '2147483648', toInt32(0) <= '2147483648', toInt32(0) > '2147483648', toInt32(0) >= '2147483648';
SELECT 'below min', toInt32(0) < '-2147483649', toInt32(0) <= '-2147483649', toInt32(0) > '-2147483649', toInt32(0) >= '-2147483649';
SELECT 'above max, constant on the left', '2147483648' < toInt32(0), '2147483648' <= toInt32(0), '2147483648' > toInt32(0), '2147483648' >= toInt32(0);
SELECT 'below min, constant on the left', '-2147483649' < toInt32(0), '-2147483649' <= toInt32(0), '-2147483649' > toInt32(0), '-2147483649' >= toInt32(0);

SELECT 'equality', toInt32(0) = '2147483648', toInt32(0) != '2147483648', toUInt64(0) = '18446744073709551616', toUInt64(0) != '18446744073709551616';
SELECT 'equality against a constant that wrapped onto the column', toInt64('-9223372036854775808') = '9223372036854775808', toInt256(0) = '115792089237316195423570985008687907853269984665640564039457584007913129639936';

-- Every width at exactly max + 1, and every signed width also at min - 1. `<` alone answers 0 both for a
-- constant that is correctly rejected and for one that wrapped around, so every width is asserted with `>`
-- as well. A negative constant against an unsigned type is rejected instead (see below).
SELECT 'UInt8', toUInt8(0) < '256', toUInt8(0) > '256';
SELECT 'UInt16', toUInt16(0) < '65536', toUInt16(0) > '65536';
SELECT 'UInt32', toUInt32(0) < '4294967296', toUInt32(0) > '4294967296';
SELECT 'UInt64', toUInt64(0) < '18446744073709551616', toUInt64(0) > '18446744073709551616';
SELECT 'UInt128', toUInt128(0) < '340282366920938463463374607431768211456', toUInt128(0) > '340282366920938463463374607431768211456';
SELECT 'UInt256', toUInt256(0) < '115792089237316195423570985008687907853269984665640564039457584007913129639936', toUInt256(0) > '115792089237316195423570985008687907853269984665640564039457584007913129639936';
SELECT 'Int8', toInt8(0) < '128', toInt8(0) > '128', toInt8(0) < '-129', toInt8(0) > '-129';
SELECT 'Int16', toInt16(0) < '32768', toInt16(0) > '32768', toInt16(0) < '-32769', toInt16(0) > '-32769';
SELECT 'Int32', toInt32(0) < '2147483648', toInt32(0) > '2147483648', toInt32(0) < '-2147483649', toInt32(0) > '-2147483649';
SELECT 'Int64', toInt64(0) < '9223372036854775808', toInt64(0) > '9223372036854775808', toInt64(0) < '-9223372036854775809', toInt64(0) > '-9223372036854775809';
SELECT 'Int128', toInt128(0) < '170141183460469231731687303715884105728', toInt128(0) > '170141183460469231731687303715884105728', toInt128(0) < '-170141183460469231731687303715884105729', toInt128(0) > '-170141183460469231731687303715884105729';
SELECT 'Int256', toInt256(0) < '57896044618658097711785492504343953926634992332820282019728792003956564819968', toInt256(0) > '57896044618658097711785492504343953926634992332820282019728792003956564819968', toInt256(0) < '-57896044618658097711785492504343953926634992332820282019728792003956564819969', toInt256(0) > '-57896044618658097711785492504343953926634992332820282019728792003956564819969';

-- A constant whose magnitude does not fit the widest integer type either: its sign decides the direction.
SELECT 'beyond every width', toInt32(0) < '1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000', toInt32(0) > '-1000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000';

-- The extremes themselves are in range and must keep comparing as values, not as overflows.
SELECT 'in range at the boundary', toInt64(0) < '9223372036854775807', toInt64(9223372036854775807) = '9223372036854775807', toInt8(-128) = '-128', toUInt256('115792089237316195423570985008687907853269984665640564039457584007913129639935') = '115792089237316195423570985008687907853269984665640564039457584007913129639935', toInt256('-57896044618658097711785492504343953926634992332820282019728792003956564819968') = '-57896044618658097711785492504343953926634992332820282019728792003956564819968';

SELECT 'wrappers', toNullable(toInt32(0)) < '2147483648', toLowCardinality(toInt32(0)) < '2147483648', toLowCardinality(toNullable(toInt32(0))) < '2147483648', materialize(toInt32(0)) < '2147483648', toInt32(0) < toFixedString('2147483648', 10);

-- Targets whose string parse saturates or is exact were already right and must stay so, and a numeric
-- constant takes a common supertype instead of being narrowed.
SELECT 'other target families', toFloat32(1) < '1e300', toDate('2020-01-01') < '2400-01-01', toInt32(0) <= 1908606676453317954;
SELECT 'enum constant that is not a member', CAST('a', 'Enum8(\'a\' = 1)') < 'zz';

-- A negative constant against an unsigned type is mathematically below the minimum, but it is rejected and
-- must stay rejected. So must an unparseable constant, a decimal overflow, and `Bool`, which reads its own
-- text syntax rather than digits.
SELECT toUInt8(1) > '-1'; -- { serverError TYPE_MISMATCH }
SELECT toUInt64(1) < '-1'; -- { serverError TYPE_MISMATCH }
SELECT toUInt8(0) IN ('-1'); -- { serverError TYPE_MISMATCH }
SELECT toInt32(0) < 'abc'; -- { serverError TYPE_MISMATCH }
SELECT toDecimal32(1, 2) < '1e30'; -- { serverError DECIMAL_OVERFLOW }
SELECT true < '256'; -- { serverError CANNOT_PARSE_BOOL }
SELECT CAST(1, 'Bool') < '256'; -- { serverError CANNOT_PARSE_BOOL }

-- An `IN` set holds the values the type can represent, so an out-of-range constant is not a member.
SELECT 'in sets', toUInt64(0) IN ('18446744073709551616'), toInt64('-9223372036854775808') IN ('9223372036854775808'), toInt32(0) IN ('4294967296'), toUInt8(1) IN ('257'), toUInt64(0) IN (18446744073709551616), toUInt8(1) IN ('1');

DROP TABLE IF EXISTS t_oor_key32;
CREATE TABLE t_oor_key32 (k Int32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_oor_key32 SELECT number - 2 FROM numbers(5);
SELECT 'every row is below the constant', count() FROM t_oor_key32 WHERE k <= '2147483648';
SELECT 'and none is above it', count() FROM t_oor_key32 WHERE NOT (k <= '2147483648');
SELECT 'granules', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_oor_key32 WHERE k <= '2147483648') WHERE explain ILIKE '%Granules: %/%';

DROP TABLE IF EXISTS t_oor_key64;
CREATE TABLE t_oor_key64 (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_oor_key64 SELECT number FROM numbers(3);
SELECT 'a key range from a constant of the widest width keeps every row', count() FROM t_oor_key64 WHERE k <= '18446744073709551616';
SELECT 'granules', trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_oor_key64 WHERE k <= '18446744073709551616') WHERE explain ILIKE '%Granules: %/%';

DROP TABLE IF EXISTS t_oor_and;
CREATE TABLE t_oor_and (u UInt8, i Int32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_oor_and VALUES (1, 5), (2, -5);
SELECT 'inside an AND chain', count() FROM t_oor_and WHERE u < '256' AND i > 0;
SELECT 'inside an AND chain, redundant comparisons off', count() FROM t_oor_and WHERE u < '256' AND i > 0 SETTINGS optimize_redundant_comparisons = 0;

DROP TABLE IF EXISTS t_oor_join;
DROP VIEW IF EXISTS v_oor_join;
CREATE TABLE t_oor_join (s String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_oor_join SELECT toString(number) FROM numbers(20);
CREATE VIEW v_oor_join (z) AS SELECT toUInt8(0) FROM t_oor_join;
SELECT 'join, per row', countIf(CAST(v_oor_join.z AS Int32) <= '1908606676453317954'), countIf(NOT (CAST(v_oor_join.z AS Int32) <= '1908606676453317954')) FROM t_oor_join, v_oor_join;
SELECT 'join, in WHERE', count() FROM t_oor_join, v_oor_join WHERE CAST(v_oor_join.z AS Int32) <= '1908606676453317954';
SELECT 'join, in a negated WHERE', count() FROM t_oor_join, v_oor_join WHERE NOT (CAST(v_oor_join.z AS Int32) <= '1908606676453317954');
SELECT 'join, in a negated WHERE, no filter push down', count() FROM t_oor_join, v_oor_join WHERE NOT (CAST(v_oor_join.z AS Int32) <= '1908606676453317954') SETTINGS query_plan_filter_push_down = 0;

DROP TABLE IF EXISTS t_oor_join_memory;
DROP VIEW IF EXISTS v_oor_join_memory;
CREATE TABLE t_oor_join_memory (s String) ENGINE = Memory;
INSERT INTO t_oor_join_memory SELECT toString(number) FROM numbers(20);
CREATE VIEW v_oor_join_memory (z) AS SELECT toUInt8(0) FROM t_oor_join_memory;
SELECT 'join over Memory, per row and in a negated WHERE', (SELECT countIf(NOT (CAST(v_oor_join_memory.z AS Int32) <= '1908606676453317954')) FROM t_oor_join_memory, v_oor_join_memory), (SELECT count() FROM t_oor_join_memory, v_oor_join_memory WHERE NOT (CAST(v_oor_join_memory.z AS Int32) <= '1908606676453317954'));

DROP TABLE IF EXISTS t_oor_partition;
CREATE TABLE t_oor_partition (p UInt64, v UInt8) ENGINE = MergeTree PARTITION BY p ORDER BY tuple();
INSERT INTO t_oor_partition VALUES (0, 1), (1, 2);
ALTER TABLE t_oor_partition DROP PARTITION '18446744073709551616'; -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'no partition was dropped', count() FROM t_oor_partition;

-- Callers that materialize a value rather than build a comparison bound keep working.
SELECT 'values', count() FROM values('x UInt8', '1', '255');
SELECT * FROM values('x UInt8', '256'); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT 'with fill', count() FROM (SELECT number FROM numbers(3) ORDER BY number WITH FILL FROM 0 TO 5 STEP 1);
SELECT 'window frame', count() FROM (SELECT sum(number) OVER (ORDER BY number ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM numbers(3));

DROP VIEW v_oor_join_memory;
DROP VIEW v_oor_join;
DROP TABLE t_oor_join_memory;
DROP TABLE t_oor_join;
DROP TABLE t_oor_and;
DROP TABLE t_oor_key64;
DROP TABLE t_oor_key32;
DROP TABLE t_oor_partition;
