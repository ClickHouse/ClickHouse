-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the preservation assertions read exact granule counts, which
-- `index_granularity` randomization moves.

-- `plus`/`minus`/`multiply`/`divide`/`intDiv` over an `Array`/`Tuple` value are evaluated
-- element-wise, but the comparison of a compound value is lexicographic, so an element-wise
-- monotonicity verdict says nothing about the order of the whole value. Key analysis used to
-- report such a verdict as monotonic, which dropped matching rows, mis-sorted `ORDER BY` output
-- and threw `BAD_TYPE_OF_FIELD` on valid queries. Each row below prints the keyed MergeTree
-- answer beside an `ENGINE = Memory` oracle, so any divergence shows up in the reference diff.

DROP TABLE IF EXISTS t_key_arr64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arr64_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_arr64_04652 (a Array(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arr64_04652 (a Array(Int64)) ENGINE = Memory;
INSERT INTO t_key_arr64_04652 VALUES ([10,100]),([11,5]),([12,0]),([20,0]);
INSERT INTO t_mem_arr64_04652 VALUES ([10,100]),([11,5]),([12,0]),([20,0]);

-- The headline case: an ordinary POSITIVE divisor, no wrap and no reinterpretation. `intDiv(.,10)`
-- maps `[10,100]` and `[11,5]` to `[1,10]` and `[1,0]`, so the leading elements tie and the
-- lexicographic order inverts.
SELECT 'Array(Int64) / 10, equals', (SELECT count() FROM t_key_arr64_04652 WHERE intDiv(a, toInt64(10)) = [toInt64(1), toInt64(0)]) AS keyed, (SELECT count() FROM t_mem_arr64_04652 WHERE intDiv(a, toInt64(10)) = [toInt64(1), toInt64(0)]) AS oracle;
SELECT 'Array(Int64) / 10, range', (SELECT count() FROM t_key_arr64_04652 WHERE intDiv(a, toInt64(10)) > [toInt64(1), toInt64(5)]) AS keyed, (SELECT count() FROM t_mem_arr64_04652 WHERE intDiv(a, toInt64(10)) > [toInt64(1), toInt64(5)]) AS oracle;
SELECT 'Array(Int64) / 4', (SELECT count() FROM t_key_arr64_04652 WHERE intDiv(a, toInt64(4)) = [toInt64(2), toInt64(25)]) AS keyed, (SELECT count() FROM t_mem_arr64_04652 WHERE intDiv(a, toInt64(4)) = [toInt64(2), toInt64(25)]) AS oracle;
SELECT 'Array(Int64) / 10, IN', (SELECT count() FROM t_key_arr64_04652 WHERE intDiv(a, toInt64(10)) IN ([toInt64(1), toInt64(0)], [toInt64(2), toInt64(0)])) AS keyed, (SELECT count() FROM t_mem_arr64_04652 WHERE intDiv(a, toInt64(10)) IN ([toInt64(1), toInt64(0)], [toInt64(2), toInt64(0)])) AS oracle;

-- Read-in-order uses the same verdict, so the sort itself was wrong. CI randomizes
-- `optimize_read_in_order`, and an ordinary sort answers correctly either way, so pin it.
SET optimize_read_in_order = 1;
SELECT 'ORDER BY intDiv(Array(Int64), 10)',
       (SELECT groupArray(x) FROM (SELECT intDiv(a, toInt64(10)) AS x FROM t_key_arr64_04652 ORDER BY intDiv(a, toInt64(10)))) AS keyed,
       (SELECT groupArray(x) FROM (SELECT intDiv(a, toInt64(10)) AS x FROM t_mem_arr64_04652 ORDER BY intDiv(a, toInt64(10)))) AS oracle;

-- `Tuple` is affected identically: the mechanism is the lexicographic comparison, not the container.
DROP TABLE IF EXISTS t_key_tup64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_tup64_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_tup64_04652 (a Tuple(Int64, Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_tup64_04652 (a Tuple(Int64, Int64)) ENGINE = Memory;
INSERT INTO t_key_tup64_04652 VALUES ((10,100)),((11,5)),((12,0)),((20,0));
INSERT INTO t_mem_tup64_04652 VALUES ((10,100)),((11,5)),((12,0)),((20,0));
SELECT 'Tuple(Int64, Int64) / 10', (SELECT count() FROM t_key_tup64_04652 WHERE intDiv(a, toInt64(10)) = (toInt64(1), toInt64(0))) AS keyed, (SELECT count() FROM t_mem_tup64_04652 WHERE intDiv(a, toInt64(10)) = (toInt64(1), toInt64(0))) AS oracle;

-- Nesting is followed: the outer type is still a container.
DROP TABLE IF EXISTS t_key_arrarr_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arrarr_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_arrarr_04652 (a Array(Array(Int64))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arrarr_04652 (a Array(Array(Int64))) ENGINE = Memory;
INSERT INTO t_key_arrarr_04652 VALUES ([[10,100]]),([[11,5]]),([[12,0]]),([[20,0]]);
INSERT INTO t_mem_arrarr_04652 VALUES ([[10,100]]),([[11,5]]),([[12,0]]),([[20,0]]);
SELECT 'Array(Array(Int64)) / 10', (SELECT count() FROM t_key_arrarr_04652 WHERE intDiv(a, toInt64(10)) = [[toInt64(1), toInt64(0)]]) AS keyed, (SELECT count() FROM t_mem_arrarr_04652 WHERE intDiv(a, toInt64(10)) = [[toInt64(1), toInt64(0)]]) AS oracle;

-- A container under `Nullable` must be unwrapped before the check, otherwise this shape escapes it.
DROP TABLE IF EXISTS t_key_ntup_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_ntup_04652 SETTINGS ignore_drop_queries_probability = 0;
SET enable_nullable_tuple_type = 1;
CREATE TABLE t_key_ntup_04652 (a Nullable(Tuple(Int64, Int64))) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE t_mem_ntup_04652 (a Nullable(Tuple(Int64, Int64))) ENGINE = Memory;
INSERT INTO t_key_ntup_04652 VALUES ((10,100)),((11,5)),((12,0)),((20,0));
INSERT INTO t_mem_ntup_04652 VALUES ((10,100)),((11,5)),((12,0)),((20,0));
SELECT 'Nullable(Tuple(Int64, Int64)) / 10', (SELECT count() FROM t_key_ntup_04652 WHERE intDiv(a, toInt64(10)) = (toInt64(1), toInt64(0))) AS keyed, (SELECT count() FROM t_mem_ntup_04652 WHERE intDiv(a, toInt64(10)) = (toInt64(1), toInt64(0))) AS oracle;

-- `Int128` elements: the element width is irrelevant, only the containerness is.
DROP TABLE IF EXISTS t_key_arr128_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arr128_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_arr128_04652 (a Array(Int128)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arr128_04652 (a Array(Int128)) ENGINE = Memory;
INSERT INTO t_key_arr128_04652 VALUES ([10,100]),([11,5]),([12,0]),([20,0]);
INSERT INTO t_mem_arr128_04652 VALUES ([10,100]),([11,5]),([12,0]),([20,0]);
SELECT 'Array(Int128) / 10', (SELECT count() FROM t_key_arr128_04652 WHERE intDiv(a, toInt128(10)) = [toInt128(1), toInt128(0)]) AS keyed, (SELECT count() FROM t_mem_arr128_04652 WHERE intDiv(a, toInt128(10)) = [toInt128(1), toInt128(0)]) AS oracle;

-- An unsigned element divided by a signed constant reinterprets through a signed cast, so the
-- element map itself is a step function. The existing scalar guard for this cannot see it, because
-- `isUInt` is false for `Array(UInt64)`.
DROP TABLE IF EXISTS t_key_arru64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arru64_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_arru64_04652 (a Array(UInt64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arru64_04652 (a Array(UInt64)) ENGINE = Memory;
INSERT INTO t_key_arru64_04652 VALUES ([1000000000000000000]),([9223372036854775808]),([18000000000000000000]);
INSERT INTO t_mem_arru64_04652 VALUES ([1000000000000000000]),([9223372036854775808]),([18000000000000000000]);
SELECT 'Array(UInt64) / 1e18 signed wrap', (SELECT count() FROM t_key_arru64_04652 WHERE intDiv(a, toInt64(1000000000000000000)) = [toInt64(-9)]) AS keyed, (SELECT count() FROM t_mem_arru64_04652 WHERE intDiv(a, toInt64(1000000000000000000)) = [toInt64(-9)]) AS oracle;
-- A magnitude-1 divisor does not make the element map safe: the signed reinterpretation still jumps.
SELECT 'Array(UInt64) / 1 still wraps', (SELECT count() FROM t_key_arru64_04652 WHERE intDiv(a, toInt64(1)) = [toInt64(-9223372036854775808)]) AS keyed, (SELECT count() FROM t_mem_arru64_04652 WHERE intDiv(a, toInt64(1)) = [toInt64(-9223372036854775808)]) AS oracle;

DROP TABLE IF EXISTS t_key_tupu64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_tupu64_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_tupu64_04652 (a Tuple(UInt64, UInt64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_tupu64_04652 (a Tuple(UInt64, UInt64)) ENGINE = Memory;
INSERT INTO t_key_tupu64_04652 VALUES ((1000000000000000000, 1)),((9223372036854775808, 1)),((18000000000000000000, 1));
INSERT INTO t_mem_tupu64_04652 VALUES ((1000000000000000000, 1)),((9223372036854775808, 1)),((18000000000000000000, 1));
SELECT 'Tuple(UInt64, UInt64) / 1e18 signed wrap', (SELECT count() FROM t_key_tupu64_04652 WHERE intDiv(a, toInt64(1000000000000000000)) = (toInt64(-9), toInt64(0))) AS keyed, (SELECT count() FROM t_mem_tupu64_04652 WHERE intDiv(a, toInt64(1000000000000000000)) = (toInt64(-9), toInt64(0))) AS oracle;

-- Mirror case: a signed element with an unsigned constant divisor whose high bit is set divides by
-- an effectively negative value, so the element map is decreasing.
DROP TABLE IF EXISTS t_key_arri8_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arri8_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_arri8_04652 (a Array(Int8)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arri8_04652 (a Array(Int8)) ENGINE = Memory;
INSERT INTO t_key_arri8_04652 VALUES ([-112]),([-56]),([0]),([56]),([112]);
INSERT INTO t_mem_arri8_04652 VALUES ([-112]),([-56]),([0]),([56]),([112]);
SELECT 'Array(Int8) / toUInt8(200) direction flip', (SELECT count() FROM t_key_arri8_04652 WHERE intDiv(a, toUInt8(200)) = [toInt8(1)]) AS keyed, (SELECT count() FROM t_mem_arri8_04652 WHERE intDiv(a, toUInt8(200)) = [toInt8(1)]) AS oracle;
-- The same shape in the `IN` spelling used to violate the `MergeTreeSetIndex` binary-search
-- invariant and abort a debug build instead of merely over-pruning.
SELECT 'Array(Int8) / toUInt8(200), IN', (SELECT count() FROM t_key_arri8_04652 WHERE intDiv(a, toUInt8(200)) IN ([toInt8(1)], [toInt8(2)])) AS keyed, (SELECT count() FROM t_mem_arri8_04652 WHERE intDiv(a, toUInt8(200)) IN ([toInt8(1)], [toInt8(2)])) AS oracle;

DROP TABLE IF EXISTS t_key_tupi8_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_tupi8_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_tupi8_04652 (a Tuple(Int8, Int8)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_tupi8_04652 (a Tuple(Int8, Int8)) ENGINE = Memory;
INSERT INTO t_key_tupi8_04652 VALUES ((-112,1)),((-56,1)),((0,1)),((56,1)),((112,1));
INSERT INTO t_mem_tupi8_04652 VALUES ((-112,1)),((-56,1)),((0,1)),((56,1)),((112,1));
SELECT 'Tuple(Int8, Int8) / toUInt8(200)', (SELECT count() FROM t_key_tupi8_04652 WHERE intDiv(a, toUInt8(200)) = (toInt8(1), toInt8(0))) AS keyed, (SELECT count() FROM t_mem_tupi8_04652 WHERE intDiv(a, toUInt8(200)) = (toInt8(1), toInt8(0))) AS oracle;

-- A mixed tuple diverges per element (the `Int8` element flips, the `Int64` element widens and
-- stays positive), so a single verdict cannot describe it at all.
DROP TABLE IF EXISTS t_key_tupmix_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_tupmix_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_tupmix_04652 (a Tuple(Int8, Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_tupmix_04652 (a Tuple(Int8, Int64)) ENGINE = Memory;
INSERT INTO t_key_tupmix_04652 VALUES ((-112,-1000)),((-56,-400)),((0,0)),((56,400)),((112,1000));
INSERT INTO t_mem_tupmix_04652 VALUES ((-112,-1000)),((-56,-400)),((0,0)),((56,400)),((112,1000));
SELECT 'Tuple(Int8, Int64) / toUInt8(200)', (SELECT count() FROM t_key_tupmix_04652 WHERE intDiv(a, toUInt8(200)) = (toInt8(1), toInt64(-2))) AS keyed, (SELECT count() FROM t_mem_tupmix_04652 WHERE intDiv(a, toUInt8(200)) = (toInt8(1), toInt64(-2))) AS oracle;

-- `divide` is affected too: `Float64` cannot separate consecutive `Int64` values above 2^53, so its
-- element map collapses them even though the function reports itself strictly monotonic.
DROP TABLE IF EXISTS t_key_arrbig_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arrbig_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_arrbig_04652 (a Array(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arrbig_04652 (a Array(Int64)) ENGINE = Memory;
INSERT INTO t_key_arrbig_04652 VALUES ([9007199254740992, 5]),([9007199254740993, 1]);
INSERT INTO t_mem_arrbig_04652 VALUES ([9007199254740992, 5]),([9007199254740993, 1]);
SELECT 'divide(Array(Int64), 1) above 2^53', (SELECT count() FROM t_key_arrbig_04652 WHERE divide(a, toInt64(1)) = [toFloat64(9007199254740992), toFloat64(1)]) AS keyed, (SELECT count() FROM t_mem_arrbig_04652 WHERE divide(a, toInt64(1)) = [toFloat64(9007199254740992), toFloat64(1)]) AS oracle;
SELECT 'ORDER BY divide(Array(Int64), 1) above 2^53',
       (SELECT groupArray(x) FROM (SELECT divide(a, toInt64(1)) AS x FROM t_key_arrbig_04652 ORDER BY divide(a, toInt64(1)))) AS keyed,
       (SELECT groupArray(x) FROM (SELECT divide(a, toInt64(1)) AS x FROM t_mem_arrbig_04652 ORDER BY divide(a, toInt64(1)))) AS oracle;

-- A tiny divisor sends every element to `inf`, so element width is not a safe exemption either.
DROP TABLE IF EXISTS t_key_arri32_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arri32_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_arri32_04652 (a Array(Int32)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arri32_04652 (a Array(Int32)) ENGINE = Memory;
INSERT INTO t_key_arri32_04652 VALUES ([1, 9]),([2, 5]),([3, 1]);
INSERT INTO t_mem_arri32_04652 VALUES ([1, 9]),([2, 5]),([3, 1]);
SELECT 'divide(Array(Int32), 5e-324) saturates', (SELECT count() FROM t_key_arri32_04652 WHERE divide(a, 5e-324) = [inf, inf]) AS keyed, (SELECT count() FROM t_mem_arri32_04652 WHERE divide(a, 5e-324) = [inf, inf]) AS oracle;

-- Arity is not a property of the type: one `Array(Int64)` column holds both `[5]` and `[10,100]`,
-- so no type-level verdict can exempt the arity-1 case. On a mixed-arity table the false verdict
-- was wrong in the other direction (an OVERCOUNT), through the implicit-projection consumer, so
-- both settings must agree with the oracle.
DROP TABLE IF EXISTS t_key_arrmix_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arrmix_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_arrmix_04652 (a Array(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arrmix_04652 (a Array(Int64)) ENGINE = Memory;
INSERT INTO t_key_arrmix_04652 VALUES ([10]),([10,100]),([11,5]),([12,0]);
INSERT INTO t_mem_arrmix_04652 VALUES ([10]),([10,100]),([11,5]),([12,0]);
SELECT 'mixed arity, implicit projections on', (SELECT count() FROM t_key_arrmix_04652 WHERE intDiv(a, toInt64(10)) = [toInt64(1), toInt64(0)] SETTINGS optimize_use_implicit_projections = 1) AS keyed, (SELECT count() FROM t_mem_arrmix_04652 WHERE intDiv(a, toInt64(10)) = [toInt64(1), toInt64(0)]) AS oracle;
SELECT 'mixed arity, implicit projections off', (SELECT count() FROM t_key_arrmix_04652 WHERE intDiv(a, toInt64(10)) = [toInt64(1), toInt64(0)] SETTINGS optimize_use_implicit_projections = 0) AS keyed, (SELECT count() FROM t_mem_arrmix_04652 WHERE intDiv(a, toInt64(10)) = [toInt64(1), toInt64(0)]) AS oracle;

-- With the compound value on the RIGHT of the operator, key analysis compared an `Array` endpoint
-- against an integer and threw `BAD_TYPE_OF_FIELD`, so these valid queries could not run at all.
-- The divisor column is free of zeros so the arithmetic itself is defined.
DROP TABLE IF EXISTS t_key_arrnz_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_arrnz_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_arrnz_04652 (a Array(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_arrnz_04652 (a Array(Int64)) ENGINE = Memory;
INSERT INTO t_key_arrnz_04652 VALUES ([10,100]),([11,5]),([12,4]),([20,2]);
INSERT INTO t_mem_arrnz_04652 VALUES ([10,100]),([11,5]),([12,4]),([20,2]);
SELECT 'intDiv(const, Array key)', (SELECT count() FROM t_key_arrnz_04652 WHERE intDiv(toInt64(100), a) = [toInt64(9), toInt64(20)]) AS keyed, (SELECT count() FROM t_mem_arrnz_04652 WHERE intDiv(toInt64(100), a) = [toInt64(9), toInt64(20)]) AS oracle;
SELECT 'divide(const, Array key)', (SELECT count() FROM t_key_arrnz_04652 WHERE divide(toInt64(100), a) = [toFloat64(10), toFloat64(1)]) AS keyed, (SELECT count() FROM t_mem_arrnz_04652 WHERE divide(toInt64(100), a) = [toFloat64(10), toFloat64(1)]) AS oracle;

-- A compound CONSTANT against a compound key reaches `plus`/`minus`, which a scalar constant never
-- does (it fails type resolution first). Same throw.
SELECT 'plus(Array key, Array const)', (SELECT count() FROM t_key_arr64_04652 WHERE plus(a, [toInt64(0), toInt64(0)]) = [toInt64(11), toInt64(5)]) AS keyed, (SELECT count() FROM t_mem_arr64_04652 WHERE plus(a, [toInt64(0), toInt64(0)]) = [toInt64(11), toInt64(5)]) AS oracle;
SELECT 'minus(Array key, Array const)', (SELECT count() FROM t_key_arr64_04652 WHERE minus(a, [toInt64(0), toInt64(0)]) = [toInt64(11), toInt64(5)]) AS keyed, (SELECT count() FROM t_mem_arr64_04652 WHERE minus(a, [toInt64(0), toInt64(0)]) = [toInt64(11), toInt64(5)]) AS oracle;

-- `plus`/`minus` also drop rows outright: the transformed ENDPOINTS keep their order here, so the
-- overflow of the INTERIOR element is invisible to the endpoint check and its granule is pruned away.
DROP TABLE IF EXISTS t_key_ovf_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_ovf_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_ovf_04652 (a Array(Int64)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_ovf_04652 (a Array(Int64)) ENGINE = Memory;
INSERT INTO t_key_ovf_04652 VALUES ([1,0]),([1,9223372036854775807]),([2,0]);
INSERT INTO t_mem_ovf_04652 VALUES ([1,0]),([1,9223372036854775807]),([2,0]);
SELECT 'plus(Array key, Array const) interior overflow', (SELECT count() FROM t_key_ovf_04652 WHERE plus(a, [toInt64(0), toInt64(1)]) = [toInt64(1), toInt64(-9223372036854775808)]) AS keyed, (SELECT count() FROM t_mem_ovf_04652 WHERE plus(a, [toInt64(0), toInt64(1)]) = [toInt64(1), toInt64(-9223372036854775808)]) AS oracle;
SELECT 'minus(Array key, Array const) interior overflow', (SELECT count() FROM t_key_ovf_04652 WHERE minus(a, [toInt64(0), toInt64(-1)]) = [toInt64(1), toInt64(-9223372036854775808)]) AS keyed, (SELECT count() FROM t_mem_ovf_04652 WHERE minus(a, [toInt64(0), toInt64(-1)]) = [toInt64(1), toInt64(-9223372036854775808)]) AS oracle;

-- `Tuple * Tuple` lowers to `dotProduct`, so the varying operand is compound while the RESULT is
-- scalar. Only a check on the argument type catches this one.
SELECT 'multiply(Tuple key, Tuple const) = dotProduct', (SELECT count() FROM t_key_tup64_04652 WHERE multiply(a, (toInt64(1), toInt64(1))) = toInt64(110)) AS keyed, (SELECT count() FROM t_mem_tup64_04652 WHERE multiply(a, (toInt64(1), toInt64(1))) = toInt64(110)) AS oracle;

-- Mirror shape: a compound CONSTANT against a SCALAR key. Here the varying operand is scalar and
-- only the RESULT is compound, so a check on the argument type alone would miss it.
DROP TABLE IF EXISTS t_key_scalar_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_scalar_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_scalar_04652 (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_scalar_04652 (a Int64) ENGINE = Memory;
INSERT INTO t_key_scalar_04652 VALUES (10),(20),(30);
INSERT INTO t_mem_scalar_04652 VALUES (10),(20),(30);
SELECT 'intDiv(Array const, scalar key)', (SELECT count() FROM t_key_scalar_04652 WHERE intDiv([toInt64(100), toInt64(200)], a) = [toInt64(10), toInt64(20)]) AS keyed, (SELECT count() FROM t_mem_scalar_04652 WHERE intDiv([toInt64(100), toInt64(200)], a) = [toInt64(10), toInt64(20)]) AS oracle;
SELECT 'divide(Array const, scalar key)', (SELECT count() FROM t_key_scalar_04652 WHERE divide([toInt64(100), toInt64(200)], a) = [toFloat64(10), toFloat64(20)]) AS keyed, (SELECT count() FROM t_mem_scalar_04652 WHERE divide([toInt64(100), toInt64(200)], a) = [toFloat64(10), toFloat64(20)]) AS oracle;
SELECT 'multiply(Array const, scalar key)', (SELECT count() FROM t_key_scalar_04652 WHERE multiply([toInt64(100), toInt64(200)], a) = [toInt64(1000), toInt64(2000)]) AS keyed, (SELECT count() FROM t_mem_scalar_04652 WHERE multiply([toInt64(100), toInt64(200)], a) = [toInt64(1000), toInt64(2000)]) AS oracle;

-- Preservation. Scalar keys are untouched, so the granule counts below must not move: reading 2 of
-- 100 granules means the monotonic chain is still being applied. A guard that rejects more than
-- compound values shows up here immediately as a full scan.
DROP TABLE IF EXISTS t_key_prune_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_mem_prune_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_prune_04652 (a Int64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_mem_prune_04652 (a Int64) ENGINE = Memory;
INSERT INTO t_key_prune_04652 SELECT number * 100 FROM numbers(100);
INSERT INTO t_mem_prune_04652 SELECT number * 100 FROM numbers(100);
SELECT 'scalar intDiv still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_prune_04652 WHERE intDiv(a, toInt64(10)) = toInt64(50)) WHERE explain ILIKE '%Granules: 2/100%';
SELECT 'scalar divide still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_prune_04652 WHERE divide(a, toInt64(10)) = toFloat64(50)) WHERE explain ILIKE '%Granules: 2/100%';
SELECT 'scalar plus still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_prune_04652 WHERE plus(a, toInt64(1)) = toInt64(501)) WHERE explain ILIKE '%Granules: 2/100%';
SELECT 'scalar multiply still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_prune_04652 WHERE multiply(a, toInt64(2)) = toInt64(1000)) WHERE explain ILIKE '%Granules: 2/100%';
SELECT 'scalar intDiv answers', (SELECT count() FROM t_key_prune_04652 WHERE intDiv(a, toInt64(10)) = toInt64(50)) AS keyed, (SELECT count() FROM t_mem_prune_04652 WHERE intDiv(a, toInt64(10)) = toInt64(50)) AS oracle;

-- A scalar `String` key with `INTERVAL` is the reason the check must test containerness directly
-- rather than `isValueRepresentedByNumber()`, which is false for `String` and would silently
-- disable this working pruning.
DROP TABLE IF EXISTS t_key_str_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_str_04652 (a String) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_key_str_04652 SELECT toString(toDate('2020-01-01') + number) FROM numbers(100);
SELECT 'String key + INTERVAL still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_str_04652 WHERE a + INTERVAL 1 DAY = toDateTime64('2020-01-03 00:00:00', 3)) WHERE explain ILIKE '%Granules: 2/100%';

-- A `Tuple(Interval, Interval)` operand against a date/time key has a SCALAR result and a SCALAR
-- varying operand, so neither check fires and its pruning is preserved. This is why the second
-- check is on the result type and not on "either operand is compound".
DROP TABLE IF EXISTS t_key_date_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_dt_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_date32_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE IF EXISTS t_key_dt64_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_key_date_04652 (a Date) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_key_dt_04652 (a DateTime) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_key_date32_04652 (a Date32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t_key_dt64_04652 (a DateTime64(3)) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
INSERT INTO t_key_date_04652 SELECT toDate('2020-01-01') + number FROM numbers(100);
INSERT INTO t_key_dt_04652 SELECT toDateTime('2020-01-01 00:00:00') + number * 86400 FROM numbers(100);
INSERT INTO t_key_date32_04652 SELECT toDate32('2020-01-01') + number FROM numbers(100);
INSERT INTO t_key_dt64_04652 SELECT toDateTime64('2020-01-01 00:00:00', 3) + number * 86400 FROM numbers(100);
SELECT 'Date + Tuple(Interval) still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_date_04652 WHERE a + (INTERVAL 1 DAY, INTERVAL 1 MONTH) = toDateTime('2020-02-02 00:00:00')) WHERE explain ILIKE '%Granules: 1/100%';
SELECT 'DateTime + Tuple(Interval) still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_dt_04652 WHERE a + (INTERVAL 1 DAY, INTERVAL 1 MONTH) = toDateTime('2020-02-02 00:00:00')) WHERE explain ILIKE '%Granules: 1/100%';
SELECT 'Date32 + Tuple(Interval) still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_date32_04652 WHERE a + (INTERVAL 1 DAY, INTERVAL 1 MONTH) = toDateTime64('2020-02-02 00:00:00', 3)) WHERE explain ILIKE '%Granules: 1/100%';
SELECT 'DateTime64 + Tuple(Interval) still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_dt64_04652 WHERE a + (INTERVAL 1 DAY, INTERVAL 1 MONTH) = toDateTime64('2020-02-02 00:00:00', 3)) WHERE explain ILIKE '%Granules: 1/100%';

-- The same shape with the `Tuple(Interval, Interval)` operand on the LEFT. The varying operand is
-- still the scalar key and the result is still scalar, so this must keep pruning too. This is the
-- shape that distinguishes "the type of the varying argument" from "the type of the left argument":
-- deriving the checked type from the operand position would classify the constant's `Tuple` as
-- compound and silently disable this working pruning.
DROP TABLE IF EXISTS t_mem_date_04652 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_mem_date_04652 (a Date) ENGINE = Memory;
INSERT INTO t_mem_date_04652 SELECT toDate('2020-01-01') + number FROM numbers(100);
SELECT 'Tuple(Interval) + Date still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_date_04652 WHERE (INTERVAL 1 DAY, INTERVAL 1 MONTH) + a = toDateTime('2020-02-02 00:00:00')) WHERE explain ILIKE '%Granules: 1/100%';
SELECT 'Tuple(Interval) + Date answers', (SELECT count() FROM t_key_date_04652 WHERE (INTERVAL 1 DAY, INTERVAL 1 MONTH) + a = toDateTime('2020-02-02 00:00:00')) AS keyed, (SELECT count() FROM t_mem_date_04652 WHERE (INTERVAL 1 DAY, INTERVAL 1 MONTH) + a = toDateTime('2020-02-02 00:00:00')) AS oracle;
SELECT 'Tuple(Interval) + DateTime still prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_key_dt_04652 WHERE (INTERVAL 1 DAY, INTERVAL 1 MONTH) + a = toDateTime('2020-02-02 00:00:00')) WHERE explain ILIKE '%Granules: 1/100%';

-- `modulo` has no monotonicity information at all, so it is unaffected either way.
SELECT 'modulo over Array answers', (SELECT count() FROM t_key_arr64_04652 WHERE modulo(a, toInt64(10)) = [toInt64(0), toInt64(0)]) AS keyed, (SELECT count() FROM t_mem_arr64_04652 WHERE modulo(a, toInt64(10)) = [toInt64(0), toInt64(0)]) AS oracle;
-- `multiply` over a compound key answered correctly before the fix and must keep doing so.
SELECT 'multiply(Array key, scalar const) answers', (SELECT count() FROM t_key_arr64_04652 WHERE multiply(a, toInt64(3)) = [toInt64(33), toInt64(15)]) AS keyed, (SELECT count() FROM t_mem_arr64_04652 WHERE multiply(a, toInt64(3)) = [toInt64(33), toInt64(15)]) AS oracle;

DROP TABLE t_key_arr64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_arr64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_tup64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_tup64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_arrarr_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_arrarr_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_ntup_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_ntup_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_arr128_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_arr128_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_arru64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_arru64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_tupu64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_tupu64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_arri8_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_arri8_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_tupi8_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_tupi8_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_tupmix_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_tupmix_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_arrbig_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_arrbig_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_arri32_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_arri32_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_arrmix_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_arrmix_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_arrnz_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_arrnz_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_ovf_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_ovf_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_scalar_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_scalar_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_prune_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_prune_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_str_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_date_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_dt_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_date32_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_key_dt64_04652 SETTINGS ignore_drop_queries_probability = 0;
DROP TABLE t_mem_date_04652 SETTINGS ignore_drop_queries_probability = 0;
