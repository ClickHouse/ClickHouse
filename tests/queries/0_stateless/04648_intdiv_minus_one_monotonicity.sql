-- Tags: no-random-settings, no-random-merge-tree-settings

-- `intDiv(x, -1)` is mathematically `-x`, which is not representable at the signed minimum of the
-- computation width: the minimum folds back onto itself while `minimum + 1` maps to the maximum.
-- The function is therefore non-monotonic on any range containing that minimum, and claiming
-- monotonicity makes MergeTree key analysis prune a granule that really holds a matching row.
-- Every assertion below compares the keyed MergeTree table against an `ENGINE=Memory` oracle on
-- identical data, so the oracle is the test's own control.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_key_i64_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_i64_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_i32_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_i32_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_null_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_null_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_lc_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_lc_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_minonly_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_minonly_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_range_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_range_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_u8_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_u8_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_u16_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_u16_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_i8f_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_i8f_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_i16_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_multi_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_multi_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_arr64_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arr64_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_arr32_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arr32_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_arrarr_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arrarr_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_arrlc_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arrlc_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_tup_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_tup_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_tupmix_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_tupmix_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_arrrange_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arrrange_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_arru8_04648 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arru8_04648 SETTINGS ignore_drop_queries_probability = 0;

CREATE TABLE t_key_i64_04648 (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_i64_04648 (a Int64) ENGINE = Memory;
INSERT INTO t_key_i64_04648 VALUES (-9223372036854775808), (-9223372036854775807), (-5), (5);
INSERT INTO t_mem_i64_04648 VALUES (-9223372036854775808), (-9223372036854775807), (-5), (5);

CREATE TABLE t_key_i32_04648 (a Int32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_i32_04648 (a Int32) ENGINE = Memory;
INSERT INTO t_key_i32_04648 VALUES (-2147483648), (-2147483647), (-5), (5);
INSERT INTO t_mem_i32_04648 VALUES (-2147483648), (-2147483647), (-5), (5);

CREATE TABLE t_key_null_04648 (a Nullable(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE t_mem_null_04648 (a Nullable(Int64)) ENGINE = Memory;
INSERT INTO t_key_null_04648 VALUES (-9223372036854775808), (-9223372036854775807), (-5), (5);
INSERT INTO t_mem_null_04648 VALUES (-9223372036854775808), (-9223372036854775807), (-5), (5);

CREATE TABLE t_key_lc_04648 (a LowCardinality(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_lc_04648 (a LowCardinality(Int64)) ENGINE = Memory;
INSERT INTO t_key_lc_04648 VALUES (-9223372036854775808), (-9223372036854775807), (-5), (5);
INSERT INTO t_mem_lc_04648 VALUES (-9223372036854775808), (-9223372036854775807), (-5), (5);

-- Compound dividends: an `Array`/`Tuple` divided by a scalar constant is evaluated element-wise, so an
-- element type that wraps carries the same defect while the monotonicity verdict sees only the outer type.
CREATE TABLE t_key_arr64_04648 (a Array(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arr64_04648 (a Array(Int64)) ENGINE = Memory;
INSERT INTO t_key_arr64_04648 VALUES ([-9223372036854775808]), ([-9223372036854775807]), ([-5]), ([5]);
INSERT INTO t_mem_arr64_04648 VALUES ([-9223372036854775808]), ([-9223372036854775807]), ([-5]), ([5]);

CREATE TABLE t_key_arr32_04648 (a Array(Int32)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arr32_04648 (a Array(Int32)) ENGINE = Memory;
INSERT INTO t_key_arr32_04648 VALUES ([-2147483648]), ([-2147483647]), ([-5]), ([5]);
INSERT INTO t_mem_arr32_04648 VALUES ([-2147483648]), ([-2147483647]), ([-5]), ([5]);

CREATE TABLE t_key_arrarr_04648 (a Array(Array(Int64))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arrarr_04648 (a Array(Array(Int64))) ENGINE = Memory;
INSERT INTO t_key_arrarr_04648 VALUES ([[-9223372036854775808]]), ([[-9223372036854775807]]), ([[-5]]), ([[5]]);
INSERT INTO t_mem_arrarr_04648 VALUES ([[-9223372036854775808]]), ([[-9223372036854775807]]), ([[-5]]), ([[5]]);

CREATE TABLE t_key_arrlc_04648 (a Array(LowCardinality(Int64))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arrlc_04648 (a Array(LowCardinality(Int64))) ENGINE = Memory;
INSERT INTO t_key_arrlc_04648 VALUES ([-9223372036854775808]), ([-9223372036854775807]), ([-5]), ([5]);
INSERT INTO t_mem_arrlc_04648 VALUES ([-9223372036854775808]), ([-9223372036854775807]), ([-5]), ([5]);

CREATE TABLE t_key_tup_04648 (a Tuple(Int64, Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_tup_04648 (a Tuple(Int64, Int64)) ENGINE = Memory;
INSERT INTO t_key_tup_04648 VALUES ((-9223372036854775808, -9223372036854775808)), ((-9223372036854775807, -9223372036854775807)), ((-5, -5)), ((5, 5));
INSERT INTO t_mem_tup_04648 VALUES ((-9223372036854775808, -9223372036854775808)), ((-9223372036854775807, -9223372036854775807)), ((-5, -5)), ((5, 5));

-- A MIXED tuple: only the first element is of a wrapping type, and one wrapping element is enough.
CREATE TABLE t_key_tupmix_04648 (a Tuple(Int64, Int8)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_tupmix_04648 (a Tuple(Int64, Int8)) ENGINE = Memory;
INSERT INTO t_key_tupmix_04648 VALUES ((-9223372036854775808, 3)), ((-9223372036854775807, 3)), ((-5, 3)), ((5, 3));
INSERT INTO t_mem_tupmix_04648 VALUES ((-9223372036854775808, 3)), ((-9223372036854775807, 3)), ((-5, 3)), ((5, 3));

SELECT 'wrong results: keyed count must equal the Memory oracle';

-- Every spelling of an effective `-1` divisor that takes `DivideIntegralByConstantImpl::vectorConstant`
-- (an `Int32`/`Int64` dividend with a native signed divisor), plus the `IN` spelling, which goes through
-- `MergeTreeSetIndex::checkInRange` instead of `KeyCondition::checkInRange`.
SELECT 'Int64 / toInt64(-1)', (SELECT count() FROM t_key_i64_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(-9223372036854775808)) AS keyed, (SELECT count() FROM t_mem_i64_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(-9223372036854775808)) AS oracle;
SELECT 'Int64 / toInt64(-1), IN', (SELECT count() FROM t_key_i64_04648 WHERE intDiv(a, toInt64(-1)) IN (toInt64(-9223372036854775808))) AS keyed, (SELECT count() FROM t_mem_i64_04648 WHERE intDiv(a, toInt64(-1)) IN (toInt64(-9223372036854775808))) AS oracle;
SELECT 'Int64 / toInt32(-1)', (SELECT count() FROM t_key_i64_04648 WHERE intDiv(a, toInt32(-1)) = toInt64(-9223372036854775808)) AS keyed, (SELECT count() FROM t_mem_i64_04648 WHERE intDiv(a, toInt32(-1)) = toInt64(-9223372036854775808)) AS oracle;
SELECT 'Int64 / toInt16(-1)', (SELECT count() FROM t_key_i64_04648 WHERE intDiv(a, toInt16(-1)) = toInt64(-9223372036854775808)) AS keyed, (SELECT count() FROM t_mem_i64_04648 WHERE intDiv(a, toInt16(-1)) = toInt64(-9223372036854775808)) AS oracle;
SELECT 'Int64 / toInt8(-1)', (SELECT count() FROM t_key_i64_04648 WHERE intDiv(a, toInt8(-1)) = toInt64(-9223372036854775808)) AS keyed, (SELECT count() FROM t_mem_i64_04648 WHERE intDiv(a, toInt8(-1)) = toInt64(-9223372036854775808)) AS oracle;
SELECT 'Int32 / toInt32(-1)', (SELECT count() FROM t_key_i32_04648 WHERE intDiv(a, toInt32(-1)) = toInt32(-2147483648)) AS keyed, (SELECT count() FROM t_mem_i32_04648 WHERE intDiv(a, toInt32(-1)) = toInt32(-2147483648)) AS oracle;
SELECT 'Int32 / toInt16(-1)', (SELECT count() FROM t_key_i32_04648 WHERE intDiv(a, toInt16(-1)) = toInt32(-2147483648)) AS keyed, (SELECT count() FROM t_mem_i32_04648 WHERE intDiv(a, toInt16(-1)) = toInt32(-2147483648)) AS oracle;
SELECT 'Int32 / toInt8(-1)', (SELECT count() FROM t_key_i32_04648 WHERE intDiv(a, toInt8(-1)) = toInt32(-2147483648)) AS keyed, (SELECT count() FROM t_mem_i32_04648 WHERE intDiv(a, toInt8(-1)) = toInt32(-2147483648)) AS oracle;
SELECT 'Int32 / toInt64(-1)', (SELECT count() FROM t_key_i32_04648 WHERE intDiv(a, toInt64(-1)) = toInt32(-2147483648)) AS keyed, (SELECT count() FROM t_mem_i32_04648 WHERE intDiv(a, toInt64(-1)) = toInt32(-2147483648)) AS oracle;

-- Wrapper carriers: the guard reads types stripped with `removeNullable(recursiveRemoveLowCardinality(...))`.
SELECT 'Nullable(Int64) / toInt64(-1)', (SELECT count() FROM t_key_null_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(-9223372036854775808)) AS keyed, (SELECT count() FROM t_mem_null_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(-9223372036854775808)) AS oracle;
SELECT 'LowCardinality(Int64) / toInt64(-1)', (SELECT count() FROM t_key_lc_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(-9223372036854775808)) AS keyed, (SELECT count() FROM t_mem_lc_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(-9223372036854775808)) AS oracle;

-- Compound carriers: `getMonotonicityForRange` sees only the OUTER type, so the scalar dividend gate
-- cannot detect an `Array`/`Tuple` whose ELEMENTS are of a wrapping type. Nesting is followed
-- recursively, and a tuple is a carrier as soon as any one of its elements is.
SELECT 'Array(Int64) / toInt64(-1)', (SELECT count() FROM t_key_arr64_04648 WHERE intDiv(a, toInt64(-1)) = [toInt64(-9223372036854775808)]) AS keyed, (SELECT count() FROM t_mem_arr64_04648 WHERE intDiv(a, toInt64(-1)) = [toInt64(-9223372036854775808)]) AS oracle;
SELECT 'Array(Int32) / toInt32(-1)', (SELECT count() FROM t_key_arr32_04648 WHERE intDiv(a, toInt32(-1)) = [toInt32(-2147483648)]) AS keyed, (SELECT count() FROM t_mem_arr32_04648 WHERE intDiv(a, toInt32(-1)) = [toInt32(-2147483648)]) AS oracle;
SELECT 'Array(Array(Int64)) / toInt64(-1)', (SELECT count() FROM t_key_arrarr_04648 WHERE intDiv(a, toInt64(-1)) = [[toInt64(-9223372036854775808)]]) AS keyed, (SELECT count() FROM t_mem_arrarr_04648 WHERE intDiv(a, toInt64(-1)) = [[toInt64(-9223372036854775808)]]) AS oracle;
SELECT 'Array(LowCardinality(Int64)) / toInt64(-1)', (SELECT count() FROM t_key_arrlc_04648 WHERE intDiv(a, toInt64(-1)) = [toInt64(-9223372036854775808)]) AS keyed, (SELECT count() FROM t_mem_arrlc_04648 WHERE intDiv(a, toInt64(-1)) = [toInt64(-9223372036854775808)]) AS oracle;
SELECT 'Tuple(Int64, Int64) / toInt64(-1)', (SELECT count() FROM t_key_tup_04648 WHERE intDiv(a, toInt64(-1)) = (toInt64(-9223372036854775808), toInt64(-9223372036854775808))) AS keyed, (SELECT count() FROM t_mem_tup_04648 WHERE intDiv(a, toInt64(-1)) = (toInt64(-9223372036854775808), toInt64(-9223372036854775808))) AS oracle;
SELECT 'Tuple(Int64, Int8) / toInt64(-1)', (SELECT count() FROM t_key_tupmix_04648 WHERE intDiv(a, toInt64(-1)) = (toInt64(-9223372036854775808), toInt8(-3))) AS keyed, (SELECT count() FROM t_mem_tupmix_04648 WHERE intDiv(a, toInt64(-1)) = (toInt64(-9223372036854775808), toInt8(-3))) AS oracle;

SELECT 'wrong ORDER BY: the keyed order must equal the Memory oracle';

-- `ReadInOrderOptimizer` probes with unbounded endpoints and only reads `is_monotonic`/`is_positive`,
-- so the same false claim makes it read the part in reverse order and emit mis-sorted rows. There is no
-- `WHERE`, so nothing here can be pruned: this assertion pins the read-in-order consumer only.
SELECT 'ORDER BY intDiv(a, -1)',
       (SELECT groupArray(x) FROM (SELECT intDiv(a, toInt64(-1)) AS x FROM t_key_i64_04648 ORDER BY intDiv(a, toInt64(-1)))) AS keyed,
       (SELECT groupArray(x) FROM (SELECT intDiv(a, toInt64(-1)) AS x FROM t_mem_i64_04648 ORDER BY intDiv(a, toInt64(-1)))) AS oracle;
SELECT 'ORDER BY intDiv(Array(Int64), -1)',
       (SELECT groupArray(x) FROM (SELECT intDiv(a, toInt64(-1)) AS x FROM t_key_arr64_04648 ORDER BY intDiv(a, toInt64(-1)))) AS keyed,
       (SELECT groupArray(x) FROM (SELECT intDiv(a, toInt64(-1)) AS x FROM t_mem_arr64_04648 ORDER BY intDiv(a, toInt64(-1)))) AS oracle;

SELECT 'preserved pruning: the guard must not fire outside the wrap';

-- A single-point range is trivially monotonic and the transform is exact there (the equal-endpoint branch
-- decides it, and this PR does not touch that branch), so pruning a table whose every adjacent mark pair
-- is `(minimum, minimum)` must keep working. Eight rows so that a non-matching predicate has marks to
-- reject: `intDiv(min, -1)` is `min`, so `= 5` must prune the whole part away.
CREATE TABLE t_key_minonly_04648 (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_minonly_04648 (a Int64) ENGINE = Memory;
INSERT INTO t_key_minonly_04648 SELECT -9223372036854775808 FROM numbers(8);
INSERT INTO t_mem_minonly_04648 SELECT -9223372036854775808 FROM numbers(8);
SELECT 'single-point range at the minimum', (SELECT count() FROM t_key_minonly_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(-9223372036854775808)) AS keyed, (SELECT count() FROM t_mem_minonly_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(-9223372036854775808)) AS oracle;
SELECT 'single-point range at the minimum still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_minonly_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(5)) WHERE explain ILIKE '%Granules: 0/8%';
SELECT 'single-point range at the minimum prunes correctly', (SELECT count() FROM t_key_minonly_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(5)) AS keyed, (SELECT count() FROM t_mem_minonly_04648 WHERE intDiv(a, toInt64(-1)) = toInt64(5)) AS oracle;

CREATE TABLE t_key_range_04648 (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_range_04648 (a Int64) ENGINE = Memory;
INSERT INTO t_key_range_04648 SELECT number * 100 FROM numbers(100);
INSERT INTO t_mem_range_04648 SELECT number * 100 FROM numbers(100);

-- An ordinary positive divisor is untouched.
SELECT 'ordinary intDiv still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_range_04648 WHERE intDiv(a, 10) = 50) WHERE explain ILIKE '%Granules: 2/100%';

-- A half-bounded range that excludes the minimum must keep pruning: this is what pins the
-- containment helper's side-awareness (an unbounded RIGHT endpoint does not reach the minimum).
SELECT 'range excluding the minimum still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_range_04648 WHERE a >= 0 AND intDiv(a, toInt64(-1)) = -5000) WHERE explain ILIKE '%Granules: 2/100%';
SELECT 'range excluding the minimum answers', (SELECT count() FROM t_key_range_04648 WHERE a >= 0 AND intDiv(a, toInt64(-1)) = -5000) AS keyed, (SELECT count() FROM t_mem_range_04648 WHERE a >= 0 AND intDiv(a, toInt64(-1)) = -5000) AS oracle;

-- A divisor that is not effectively `-1` is untouched.
SELECT 'divisor -2 still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_range_04648 WHERE intDiv(a, toInt8(-2)) = -2500) WHERE explain ILIKE '%Granules: 2/100%';
SELECT 'divisor -2 answers', (SELECT count() FROM t_key_range_04648 WHERE intDiv(a, toInt8(-2)) = -2500) AS keyed, (SELECT count() FROM t_mem_range_04648 WHERE intDiv(a, toInt8(-2)) = -2500) AS oracle;

-- Unsigned/unsigned never reaches the signed wrap: `intDiv(UInt8, toUInt8(255))` is not a carrier and
-- must keep answering AND keep pruning. This pins the dividend-type gate. The tables span the full
-- unsigned domain so that a predicate selecting a single quotient has marks to reject.
CREATE TABLE t_key_u8_04648 (a UInt8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_u8_04648 (a UInt8) ENGINE = Memory;
INSERT INTO t_key_u8_04648 SELECT number FROM numbers(256);
INSERT INTO t_mem_u8_04648 SELECT number FROM numbers(256);
SELECT 'UInt8 / toUInt8(255)', (SELECT count() FROM t_key_u8_04648 WHERE intDiv(a, toUInt8(255)) = 1) AS keyed, (SELECT count() FROM t_mem_u8_04648 WHERE intDiv(a, toUInt8(255)) = 1) AS oracle;
SELECT 'UInt8 / toUInt8(255) still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_u8_04648 WHERE intDiv(a, toUInt8(255)) = 1) WHERE explain ILIKE '%Granules: 2/256%';

CREATE TABLE t_key_u16_04648 (a UInt16) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_u16_04648 (a UInt16) ENGINE = Memory;
INSERT INTO t_key_u16_04648 SELECT if(number = 66, 65535, number * 1000) FROM numbers(67);
INSERT INTO t_mem_u16_04648 SELECT if(number = 66, 65535, number * 1000) FROM numbers(67);
SELECT 'UInt16 / toUInt16(65535)', (SELECT count() FROM t_key_u16_04648 WHERE intDiv(a, toUInt16(65535)) = 1) AS keyed, (SELECT count() FROM t_mem_u16_04648 WHERE intDiv(a, toUInt16(65535)) = 1) AS oracle;
SELECT 'UInt16 / toUInt16(65535) still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_u16_04648 WHERE intDiv(a, toUInt16(65535)) = 1) WHERE explain ILIKE '%Granules: 2/67%';

-- A Float divisor takes the floating-point path, whose unsafe set is different and not `-1`-specific,
-- so it stays outside the guard and must keep pruning. This pins the divisor-type gate.
CREATE TABLE t_key_i8f_04648 (a Int8) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_i8f_04648 (a Int8) ENGINE = Memory;
INSERT INTO t_key_i8f_04648 SELECT toInt8(number - 126) FROM numbers(254);
INSERT INTO t_mem_i8f_04648 SELECT toInt8(number - 126) FROM numbers(254);
SELECT 'Float divisor still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_i8f_04648 WHERE intDiv(a, toFloat64(-1)) = 50) WHERE explain ILIKE '%Granules: 2/254%';
SELECT 'Float divisor answers', (SELECT count() FROM t_key_i8f_04648 WHERE intDiv(a, toFloat64(-1)) = 50) AS keyed, (SELECT count() FROM t_mem_i8f_04648 WHERE intDiv(a, toFloat64(-1)) = 50) AS oracle;

-- Read-in-order is what the null-endpoint verdict decides, so it is the only observable that can pin the
-- two type gates for a divisor that is `-1`: widening either gate would silently cost this optimization
-- for operand pairs that never wrap silently. `Int64 / toFloat64(-1)` pins the divisor gate (a Float
-- divisor computes through floating point), `Int16 / toInt16(-1)` pins the dividend gate (an `Int16`
-- dividend has no vectorized specialization and throws instead of wrapping).
SELECT 'Float divisor keeps read-in-order', count() > 0 FROM (EXPLAIN SELECT intDiv(a, toFloat64(-1)) FROM t_key_range_04648 ORDER BY intDiv(a, toFloat64(-1)) SETTINGS optimize_read_in_order = 1) WHERE explain ILIKE '%Read type: InReverseOrder%';
SELECT 'Int128 divisor keeps read-in-order', count() > 0 FROM (EXPLAIN SELECT intDiv(a, toInt128(-1)) FROM t_key_range_04648 ORDER BY intDiv(a, toInt128(-1)) SETTINGS optimize_read_in_order = 1) WHERE explain ILIKE '%Read type: InReverseOrder%';

CREATE TABLE t_key_i16_04648 (a Int16) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_key_i16_04648 SELECT toInt16(number * 100 - 3000) FROM numbers(60);
SELECT 'Int16 dividend keeps read-in-order', count() > 0 FROM (EXPLAIN SELECT intDiv(a, toInt16(-1)) FROM t_key_i16_04648 ORDER BY intDiv(a, toInt16(-1)) SETTINGS optimize_read_in_order = 1) WHERE explain ILIKE '%Read type: InReverseOrder%';

-- A multi-column key reaches the null-endpoint branch with a genuinely HALF-BOUNDED range
-- (a concrete left bound and an unbounded right one), which is the shape that pins the containment
-- helper's side-awareness: treating any null endpoint as "contains the minimum" reads one granule more.
CREATE TABLE t_key_multi_04648 (x UInt8, a Int64) ENGINE = MergeTree ORDER BY (x, a) SETTINGS index_granularity = 1;
CREATE TABLE t_mem_multi_04648 (x UInt8, a Int64) ENGINE = Memory;
INSERT INTO t_key_multi_04648 SELECT number % 3, number * 100 FROM numbers(100);
INSERT INTO t_mem_multi_04648 SELECT number % 3, number * 100 FROM numbers(100);
SELECT 'half-bounded range prunes exactly', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_multi_04648 WHERE x = 1 AND intDiv(a, toInt64(-1)) >= -5000) WHERE explain ILIKE '%Granules: 18/100%';
SELECT 'half-bounded range answers', (SELECT count() FROM t_key_multi_04648 WHERE x = 1 AND intDiv(a, toInt64(-1)) >= -5000) AS keyed, (SELECT count() FROM t_mem_multi_04648 WHERE x = 1 AND intDiv(a, toInt64(-1)) >= -5000) AS oracle;

-- The compound guard must not over-fire: it is keyed on the ELEMENT type and the constant, so an ordinary
-- positive divisor and a non-carrier element type must both keep pruning through an `Array` key.
CREATE TABLE t_key_arrrange_04648 (a Array(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arrrange_04648 (a Array(Int64)) ENGINE = Memory;
INSERT INTO t_key_arrrange_04648 SELECT [number * 100] FROM numbers(100);
INSERT INTO t_mem_arrrange_04648 SELECT [number * 100] FROM numbers(100);
SELECT 'Array dividend, positive divisor still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_arrrange_04648 WHERE intDiv(a, toInt64(10)) = [toInt64(50)]) WHERE explain ILIKE '%Granules: 2/100%';
SELECT 'Array dividend, positive divisor answers', (SELECT count() FROM t_key_arrrange_04648 WHERE intDiv(a, toInt64(10)) = [toInt64(50)]) AS keyed, (SELECT count() FROM t_mem_arrrange_04648 WHERE intDiv(a, toInt64(10)) = [toInt64(50)]) AS oracle;

CREATE TABLE t_key_arru8_04648 (a Array(UInt8)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arru8_04648 (a Array(UInt8)) ENGINE = Memory;
INSERT INTO t_key_arru8_04648 SELECT [toUInt8(number)] FROM numbers(256);
INSERT INTO t_mem_arru8_04648 SELECT [toUInt8(number)] FROM numbers(256);
SELECT 'Array(UInt8) element is not a carrier, still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_arru8_04648 WHERE intDiv(a, toUInt8(255)) = [toUInt8(1)]) WHERE explain ILIKE '%Granules: 2/256%';
SELECT 'Array(UInt8) element is not a carrier, answers', (SELECT count() FROM t_key_arru8_04648 WHERE intDiv(a, toUInt8(255)) = [toUInt8(1)]) AS keyed, (SELECT count() FROM t_mem_arru8_04648 WHERE intDiv(a, toUInt8(255)) = [toUInt8(1)]) AS oracle;

DROP TABLE t_key_i64_04648;
DROP TABLE t_mem_i64_04648;
DROP TABLE t_key_i32_04648;
DROP TABLE t_mem_i32_04648;
DROP TABLE t_key_null_04648;
DROP TABLE t_mem_null_04648;
DROP TABLE t_key_lc_04648;
DROP TABLE t_mem_lc_04648;
DROP TABLE t_key_minonly_04648;
DROP TABLE t_mem_minonly_04648;
DROP TABLE t_key_range_04648;
DROP TABLE t_mem_range_04648;
DROP TABLE t_key_u8_04648;
DROP TABLE t_mem_u8_04648;
DROP TABLE t_key_u16_04648;
DROP TABLE t_mem_u16_04648;
DROP TABLE t_key_i8f_04648;
DROP TABLE t_mem_i8f_04648;
DROP TABLE t_key_i16_04648;
DROP TABLE t_key_multi_04648;
DROP TABLE t_mem_multi_04648;
DROP TABLE t_key_arr64_04648;
DROP TABLE t_mem_arr64_04648;
DROP TABLE t_key_arr32_04648;
DROP TABLE t_mem_arr32_04648;
DROP TABLE t_key_arrarr_04648;
DROP TABLE t_mem_arrarr_04648;
DROP TABLE t_key_arrlc_04648;
DROP TABLE t_mem_arrlc_04648;
DROP TABLE t_key_tup_04648;
DROP TABLE t_mem_tup_04648;
DROP TABLE t_key_tupmix_04648;
DROP TABLE t_mem_tupmix_04648;
DROP TABLE t_key_arrrange_04648;
DROP TABLE t_mem_arrrange_04648;
DROP TABLE t_key_arru8_04648;
DROP TABLE t_mem_arru8_04648;
