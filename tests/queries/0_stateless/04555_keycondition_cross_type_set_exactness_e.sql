-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings

-- Part of the 04549/04552-04562 family: one set-index exactness suite split across files to fit
-- the flaky check's 180s per-test budget. Every part is self-contained.

SET explain_query_plan_default = 'legacy';
SET optimize_use_implicit_projections = 0;
-- A randomized `compatibility` below 25.12 reverts this setting to false, and the `Time64` cells then
-- fail to create their column. A session `SET` survives that: the compatibility pass skips settings
-- already changed manually.
SET enable_time_time64_type = 1;
-- The set elements below that spell `DateTime` without a zone take it from the session, which the test
-- runner randomizes; pin it so the no-zone/zone pair stays the discriminator by construction.
SET session_timezone = 'UTC';

-- A set-index atom may only be treated as an exact image of the predicate when the conversion
-- preserves equality in BOTH directions: index preparation casts the set values into the key type,
-- runtime membership casts the key into the set type. Every carrier below returned a WRONG result
-- (rows silently vanished) because a non-equality-preserving cast was treated as exact. Each carrier
-- asserts the MergeTree answer against an identical `ENGINE = Memory` oracle.

SELECT '--- composite has() over TWO key columns: the one shape where the composite rule decides ---';

-- With a two-column key the per-column checks see the UNPACKED scalars and admit any integer pair, so
-- the composite rule is the deciding gate here and these cells are what pin it. (With a PACKED tuple or
-- array key the per-column check sees the whole composite instead; for `has` it applies the same
-- `Field`-identity tolerance, so the two shapes agree - see the T33/A33 cells above.)

DROP TABLE IF EXISTS s2c; DROP TABLE IF EXISTS p2c;
CREATE TABLE s2c (a Int32, b Int32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE p2c (a Int32, b Int32) ENGINE = Memory;
INSERT INTO s2c VALUES (1, 1); INSERT INTO s2c VALUES (2, 2); INSERT INTO s2c VALUES (3, 3);
INSERT INTO p2c VALUES (1, 1), (2, 2), (3, 3);
SELECT 'S2C signedness has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM s2c WHERE has([tuple(toUInt16(1), toUInt16(1)), tuple(toUInt16(2), toUInt16(2))], (a, b))) WHERE explain ILIKE '%element set%';
SELECT 'S2C signedness has',
    (SELECT count() FROM s2c WHERE has([tuple(toUInt16(1), toUInt16(1)), tuple(toUInt16(2), toUInt16(2))], (a, b))) = (SELECT count() FROM p2c WHERE has([tuple(toUInt16(1), toUInt16(1)), tuple(toUInt16(2), toUInt16(2))], (a, b)));
DROP TABLE s2c; DROP TABLE p2c;

SELECT '--- composite has() over a TRANSFORMING key expression: the left type is not reconstructible ---';

-- `data_types` always carries the type of the KEY column, so under a transforming key expression it is
-- the type of the transformed key, not of the runtime left tuple. Deciding composite identity from it
-- compares the wrong pair, and because `negate` is injective the atom stays EXACT, so `NOT has` prunes
-- a partition that still holds a match. The pair below is `(UInt32, UInt32)` against
-- `Tuple(Int64, Int64)`: the runtime `Field` compare is 0 (different signedness), so `NOT has` is true
-- for every row, yet master's reconstruction saw the negated key's type and admitted the pair.

DROP TABLE IF EXISTS ctn; DROP TABLE IF EXISTS ctno;
CREATE TABLE ctn (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (negate(a), negate(b)) PARTITION BY (negate(a), negate(b)) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE ctno (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO ctn VALUES (1, 1); INSERT INTO ctn VALUES (2, 2);
INSERT INTO ctno VALUES (1, 1), (2, 2);
SELECT 'CTN transforming key NOT has',
    (SELECT count() FROM ctn WHERE NOT has([tuple(toInt64(1), toInt64(1))], (a, b))) = (SELECT count() FROM ctno WHERE NOT has([tuple(toInt64(1), toInt64(1))], (a, b)));
SELECT 'CTN transforming key NOT has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ctn WHERE NOT has([tuple(toInt64(1), toInt64(1))], (a, b))) WHERE explain ILIKE '%element set%';
DROP TABLE ctn; DROP TABLE ctno;

-- The must-not-regress partner: a same-type literal over a NON-transforming key, otherwise identical.
-- Declining every composite `has` would pass the two cells above and fail this one.

DROP TABLE IF EXISTS ctp; DROP TABLE IF EXISTS ctpo;
CREATE TABLE ctp (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE ctpo (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO ctp VALUES (1, 1); INSERT INTO ctp VALUES (2, 2);
INSERT INTO ctpo VALUES (1, 1), (2, 2);
-- Assert the partition reduction, not just the atom's presence: a relaxed atom is still installed
-- (still prints `element set`) but sets `can_be_false = true` before the negation, so `NOT has` stops
-- pruning while the answer stays correct. Only the part count separates exact from relaxed here.
-- `Parts:` is format-independent, so the `explain_query_plan_default` pin at the top suffices.
SELECT 'CTP plain key NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ctp WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b))) WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'CTP plain key NOT has',
    (SELECT count() FROM ctp WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b))) = (SELECT count() FROM ctpo WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b)));
DROP TABLE ctp; DROP TABLE ctpo;

SELECT '--- composite has(): the attribute axis, pinned per direction ---';

-- Every other attribute-axis control in this file is a scalar `IN`. Without these three the composite
-- identity rule could be reverted to comparing canonical names and the whole file would still pass,
-- silently losing composite pruning again - which is the regression the first cell below catches.

-- A time zone is an attribute `equals` treats as interchangeable and a `Field` does not represent at
-- all, so it cannot change the runtime verdict: this pair must KEEP pruning in both directions.

DROP TABLE IF EXISTS ca_tz; DROP TABLE IF EXISTS oa_tz;
CREATE TABLE ca_tz (kt Tuple(DateTime('UTC'))) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE oa_tz (kt Tuple(DateTime('UTC'))) ENGINE = Memory;
-- `INSERT ... VALUES ((toDateTime(100)))` is rejected with `Code: 53` for a 1-tuple column, so build
-- the rows with `SELECT tuple(...)`. One part per row, so `Parts:` can register a reduction.
INSERT INTO ca_tz SELECT tuple(toDateTime(100));
INSERT INTO ca_tz SELECT tuple(toDateTime(200));
INSERT INTO ca_tz SELECT tuple(toDateTime(300));
INSERT INTO oa_tz SELECT tuple(toDateTime(100 + number * 100)) FROM numbers(3);
SELECT 'attr Tuple(DateTime(UTC))/Tuple(DateTime) has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ca_tz WHERE has([tuple(CAST(100, 'DateTime'))], kt)) WHERE explain ILIKE '%Parts: 1/3%';
SELECT 'attr Tuple(DateTime(UTC))/Tuple(DateTime) has',
    (SELECT count() FROM ca_tz WHERE has([tuple(CAST(100, 'DateTime'))], kt)) = (SELECT count() FROM oa_tz WHERE has([tuple(CAST(100, 'DateTime'))], kt));
SELECT 'attr Tuple(DateTime(UTC))/Tuple(DateTime) NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ca_tz WHERE NOT has([tuple(CAST(100, 'DateTime'))], kt)) WHERE explain ILIKE '%Parts: 2/3%';
SELECT 'attr Tuple(DateTime(UTC))/Tuple(DateTime) NOT has',
    (SELECT count() FROM ca_tz WHERE NOT has([tuple(CAST(100, 'DateTime'))], kt)) = (SELECT count() FROM oa_tz WHERE NOT has([tuple(CAST(100, 'DateTime'))], kt));
DROP TABLE ca_tz; DROP TABLE oa_tz;

-- The other direction of the same axis: a custom name IS load-bearing, because `Bool`'s cast wrapper
-- clamps every nonzero value to 1, so the preparation direction is not injective even though the
-- runtime matches the pair. Master admits it; the atom must now be absent.

DROP TABLE IF EXISTS ca_bl; DROP TABLE IF EXISTS oa_bl;
CREATE TABLE ca_bl (kt Tuple(Bool)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE oa_bl (kt Tuple(Bool)) ENGINE = Memory;
INSERT INTO ca_bl SELECT tuple(number % 2 = 1) FROM numbers(3);
INSERT INTO oa_bl SELECT tuple(number % 2 = 1) FROM numbers(3);
SELECT 'attr Tuple(Bool)/Tuple(UInt8) has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ca_bl WHERE has([tuple(toUInt8(1))], kt)) WHERE explain ILIKE '%element set%';
SELECT 'attr Tuple(Bool)/Tuple(UInt8) has',
    (SELECT count() FROM ca_bl WHERE has([tuple(toUInt8(1))], kt)) = (SELECT count() FROM oa_bl WHERE has([tuple(toUInt8(1))], kt));
DROP TABLE ca_bl; DROP TABLE oa_bl;

-- A native integer against a 128-bit one keeps its own `Field` variant, so the runtime never matches
-- the pair (`has([tuple(toUInt128(1), toUInt128(0))], tuple(toUInt64(1), toUInt64(0)))` is 0) even
-- though the preparation cast would be lossless. Master admits it; the atom must now be absent, and
-- declining agrees with the oracle.

DROP TABLE IF EXISTS ca_w; DROP TABLE IF EXISTS oa_w;
CREATE TABLE ca_w (kt Tuple(UInt64, UInt64)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE oa_w (kt Tuple(UInt64, UInt64)) ENGINE = Memory;
INSERT INTO ca_w SELECT tuple(toUInt64(number), toUInt64(0)) FROM numbers(3);
INSERT INTO oa_w SELECT tuple(toUInt64(number), toUInt64(0)) FROM numbers(3);
SELECT 'width Tuple(UInt64,UInt64)/Tuple(UInt128,UInt128) has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM ca_w WHERE has([tuple(toUInt128(1), toUInt128(0))], kt)) WHERE explain ILIKE '%element set%';
SELECT 'width Tuple(UInt64,UInt64)/Tuple(UInt128,UInt128) has',
    (SELECT count() FROM ca_w WHERE has([tuple(toUInt128(1), toUInt128(0))], kt)) = (SELECT count() FROM oa_w WHERE has([tuple(toUInt128(1), toUInt128(0))], kt));
DROP TABLE ca_w; DROP TABLE oa_w;

SELECT '--- named tuples: the cast maps fields by name, so the pair must decline ---';

DROP TABLE IF EXISTS nt; DROP TABLE IF EXISTS nto;
CREATE TABLE nt (kt Tuple(a UInt8, b UInt8)) ENGINE = MergeTree ORDER BY kt PARTITION BY kt;
CREATE TABLE nto (kt Tuple(a UInt8, b UInt8)) ENGINE = Memory;
INSERT INTO nt VALUES ((1, 1));
INSERT INTO nt VALUES ((2, 2));
INSERT INTO nto VALUES ((1, 1)), ((2, 2));
SELECT 'named tuple result',
    (SELECT count() FROM nt WHERE kt NOT IN (SELECT CAST((1, 1), 'Tuple(c UInt8, d UInt8)'))) = (SELECT count() FROM nto WHERE kt NOT IN (SELECT CAST((1, 1), 'Tuple(c UInt8, d UInt8)')));
SELECT 'named tuple declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM nt WHERE kt NOT IN (SELECT CAST((1, 1), 'Tuple(c UInt8, d UInt8)'))) WHERE explain ILIKE '%element set%';

SELECT '--- the same names over an UNPACKED tuple expression: only the OUTER layer stops discriminating ---';

-- The mirror image of the packed cells above. There the whole composite is cast by name, so a name
-- difference is genuinely not equality-preserving. Here the left type is SYNTHESIZED with the unnamed
-- `DataTypeTuple` ctor, so its outer names are the placeholders `1`, `2` and no outer tuple cast ever
-- runs (`tryPrepareSetColumnsForIndex` unpacks positionally first). Comparing the placeholders against
-- the element's real outer names declined every named element and lost sound pruning.

DROP TABLE IF EXISTS unt; DROP TABLE IF EXISTS unto;
CREATE TABLE unt (a UInt8, b UInt8) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE unto (a UInt8, b UInt8) ENGINE = Memory;
INSERT INTO unt VALUES (1, 1);
INSERT INTO unt VALUES (2, 2);
INSERT INTO unto VALUES (1, 1), (2, 2);
-- Assert the partition reduction rather than only the atom's presence: a relaxed atom still prints
-- `element set` but stops pruning under the negation, so only the part count separates exact from
-- relaxed. `Parts:` is format-independent, so the `explain_query_plan_default` pin at the top suffices.
SELECT 'unpacked named tuple has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM unt WHERE has([CAST((1, 1), 'Tuple(c UInt8, d UInt8)')], (a, b))) WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'unpacked named tuple has',
    (SELECT count() FROM unt WHERE has([CAST((1, 1), 'Tuple(c UInt8, d UInt8)')], (a, b))) = (SELECT count() FROM unto WHERE has([CAST((1, 1), 'Tuple(c UInt8, d UInt8)')], (a, b)));
-- The negated direction is the one that would over-prune if the atom were wrongly exact, so pin it too.
SELECT 'unpacked named tuple NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM unt WHERE NOT has([CAST((1, 1), 'Tuple(c UInt8, d UInt8)')], (a, b))) WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'unpacked named tuple NOT has',
    (SELECT count() FROM unt WHERE NOT has([CAST((1, 1), 'Tuple(c UInt8, d UInt8)')], (a, b))) = (SELECT count() FROM unto WHERE NOT has([CAST((1, 1), 'Tuple(c UInt8, d UInt8)')], (a, b)));
-- `Point` is a custom-named `Tuple(Float64, Float64)`. A float element gets no set atom, so the outer
-- custom name is not what decides here; these cells pin that the decline costs no correctness.

DROP TABLE IF EXISTS upt; DROP TABLE IF EXISTS upto;
CREATE TABLE upt (a Float64, b Float64) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE upto (a Float64, b Float64) ENGINE = Memory;
INSERT INTO upt SELECT toFloat64(number), toFloat64(number) FROM numbers(3);
INSERT INTO upto SELECT toFloat64(number), toFloat64(number) FROM numbers(3);
SELECT 'unpacked Point has declines on float', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM upt WHERE has([CAST((1.0, 1.0), 'Point')], (a, b))) WHERE explain ILIKE '%element set%';
SELECT 'unpacked Point has',
    (SELECT count() FROM upt WHERE has([CAST((1.0, 1.0), 'Point')], (a, b))) = (SELECT count() FROM upto WHERE has([CAST((1.0, 1.0), 'Point')], (a, b)));
DROP TABLE upt; DROP TABLE upto;

-- The scope guard. Only the OUTER layer stopped discriminating: a NESTED name difference must still
-- decline, because there the per-scalar cast IS name-mapped and zeroes the value
-- (`accurateCastOrNull(CAST(tuple(1), 'Tuple(d UInt8)'), 'Tuple(c UInt8)')` = `(0)`). The matching
-- nested name is the partner cell: it is what the relaxation is allowed to admit.

DROP TABLE IF EXISTS unn; DROP TABLE IF EXISTS unno;
CREATE TABLE unn (a Tuple(c UInt8), b UInt8) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE unno (a Tuple(c UInt8), b UInt8) ENGINE = Memory;
INSERT INTO unn VALUES (tuple(1), 1);
INSERT INTO unn VALUES (tuple(2), 2);
INSERT INTO unno VALUES (tuple(1), 1), (tuple(2), 2);
SELECT 'unpacked nested name differs declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM unn WHERE has([CAST((tuple(1), 1), 'Tuple(x Tuple(d UInt8), y UInt8)')], (a, b))) WHERE explain ILIKE '%element set%';
SELECT 'unpacked nested name differs',
    (SELECT count() FROM unn WHERE has([CAST((tuple(1), 1), 'Tuple(x Tuple(d UInt8), y UInt8)')], (a, b))) = (SELECT count() FROM unno WHERE has([CAST((tuple(1), 1), 'Tuple(x Tuple(d UInt8), y UInt8)')], (a, b)));
SELECT 'unpacked nested name matches keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM unn WHERE has([CAST((tuple(1), 1), 'Tuple(x Tuple(c UInt8), y UInt8)')], (a, b))) WHERE explain ILIKE '%Parts: 1/2%';
SELECT 'unpacked nested name matches',
    (SELECT count() FROM unn WHERE has([CAST((tuple(1), 1), 'Tuple(x Tuple(c UInt8), y UInt8)')], (a, b))) = (SELECT count() FROM unno WHERE has([CAST((tuple(1), 1), 'Tuple(x Tuple(c UInt8), y UInt8)')], (a, b)));
DROP TABLE unn; DROP TABLE unno;
DROP TABLE unt; DROP TABLE unto;

-- The branch guard. `has()` over a PACKED tuple key takes the other branch of the same function,
-- where the left type is a REAL key column with real names and the whole composite IS cast by name, so
-- the outer names must keep discriminating there. The two cells below bracket that branch: a differing
-- outer name declines, a matching one keeps pruning. Widening the relaxation to this branch collapses
-- the pair.

DROP TABLE IF EXISTS pkn; DROP TABLE IF EXISTS pkno;
CREATE TABLE pkn (kt Tuple(a UInt8)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE pkno (kt Tuple(a UInt8)) ENGINE = Memory;
INSERT INTO pkn SELECT tuple(toUInt8(number)) FROM numbers(3);
INSERT INTO pkno SELECT tuple(toUInt8(number)) FROM numbers(3);
SELECT 'packed named key differing outer name declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM pkn WHERE has([CAST(tuple(1), 'Tuple(c UInt8)')], kt)) WHERE explain ILIKE '%element set%';
SELECT 'packed named key differing outer name',
    (SELECT count() FROM pkn WHERE has([CAST(tuple(1), 'Tuple(c UInt8)')], kt)) = (SELECT count() FROM pkno WHERE has([CAST(tuple(1), 'Tuple(c UInt8)')], kt));
SELECT 'packed named key matching outer name keeps atom', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM pkn WHERE has([CAST(tuple(1), 'Tuple(a UInt8)')], kt)) WHERE explain ILIKE '%element set%';
SELECT 'packed named key matching outer name',
    (SELECT count() FROM pkn WHERE has([CAST(tuple(1), 'Tuple(a UInt8)')], kt)) = (SELECT count() FROM pkno WHERE has([CAST(tuple(1), 'Tuple(a UInt8)')], kt));
DROP TABLE pkn; DROP TABLE pkno;

SELECT '--- composite IN over a narrowing element: pruning is withdrawn, matching the oracle ---';

-- Pre-existing behaviour recorded for completeness: on a narrowing composite pair the runtime cast
-- throws CANNOT_CONVERT_TYPE while master silently pruned instead. Declining the atom makes
-- MergeTree agree with the ENGINE = Memory oracle, which also throws.
DROP TABLE IF EXISTS d1;
CREATE TABLE d1 (kt Tuple(UInt32)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO d1 VALUES (tuple(257));
INSERT INTO d1 VALUES (tuple(1));
SELECT count() FROM d1 WHERE kt IN (SELECT tuple(toUInt8(1))); -- { serverError CANNOT_CONVERT_TYPE }
-- boundary: the WIDENING direction is unaffected and keeps its result
DROP TABLE IF EXISTS w1; DROP TABLE IF EXISTS w1o;
CREATE TABLE w1 (kt Tuple(UInt8, UInt8)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE w1o (kt Tuple(UInt8, UInt8)) ENGINE = Memory;
INSERT INTO w1 VALUES ((1, 1));
INSERT INTO w1 VALUES ((2, 2));
INSERT INTO w1o VALUES ((1, 1)), ((2, 2));
SELECT 'D1 widening direction',
    (SELECT count() FROM w1 WHERE kt IN (SELECT (toUInt32(1), toUInt32(1)))) = (SELECT count() FROM w1o WHERE kt IN (SELECT (toUInt32(1), toUInt32(1))));
-- boundary: the SCALAR narrowing case is unaffected at default settings; arm 2 stays intact
DROP TABLE IF EXISTS s1; DROP TABLE IF EXISTS s1o;
CREATE TABLE s1 (k UInt32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE s1o (k UInt32) ENGINE = Memory;
INSERT INTO s1 VALUES (257);
INSERT INTO s1 VALUES (1);
INSERT INTO s1o VALUES (257), (1);
SELECT 'D1 scalar narrowing at default settings',
    (SELECT count() FROM s1 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM s1o WHERE k IN (SELECT toUInt8(1)));

SELECT '--- integer composites: pruning is withdrawn for IN, results stay correct ---';

-- 8x8 packed integer composites, plain and Nullable. Generated; do not thin.

DROP TABLE IF EXISTS c_gc_uint8; DROP TABLE IF EXISTS o_gc_uint8;
CREATE TABLE c_gc_uint8 (kt Tuple(UInt8, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_uint8 (kt Tuple(UInt8, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_uint8 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_uint8 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid P UInt8/UInt8' AS c1,
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid P UInt8/UInt16' AS c1,
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid P UInt8/UInt32' AS c1,
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid P UInt8/UInt64' AS c1,
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid P UInt8/Int8' AS c1,
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid P UInt8/Int16' AS c1,
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid P UInt8/Int32' AS c1,
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid P UInt8/Int64' AS c1,
    (SELECT count() FROM c_gc_uint8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint8 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gc_uint8; DROP TABLE o_gc_uint8;

DROP TABLE IF EXISTS c_gc_uint16; DROP TABLE IF EXISTS o_gc_uint16;
CREATE TABLE c_gc_uint16 (kt Tuple(UInt16, UInt8)) ENGINE = MergeTree ORDER BY kt;
CREATE TABLE o_gc_uint16 (kt Tuple(UInt16, UInt8)) ENGINE = Memory;
INSERT INTO c_gc_uint16 VALUES ((1, 1)), ((2, 1));
INSERT INTO o_gc_uint16 VALUES ((1, 1)), ((2, 1));
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'grid P UInt16/UInt8' AS c1,
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toUInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toUInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 2 AS ord, 'grid P UInt16/UInt16' AS c1,
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toUInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toUInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 3 AS ord, 'grid P UInt16/UInt32' AS c1,
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toUInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toUInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 4 AS ord, 'grid P UInt16/UInt64' AS c1,
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toUInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toUInt64(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 5 AS ord, 'grid P UInt16/Int8' AS c1,
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toInt8(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toInt8(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 6 AS ord, 'grid P UInt16/Int16' AS c1,
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toInt16(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toInt16(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 7 AS ord, 'grid P UInt16/Int32' AS c1,
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toInt32(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toInt32(1), toUInt8(1)))) AS c3
    UNION ALL
    SELECT 8 AS ord, 'grid P UInt16/Int64' AS c1,
    (SELECT count() FROM c_gc_uint16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt IN (SELECT (toInt64(1), toUInt8(1)))) AS c2,
    (SELECT count() FROM c_gc_uint16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) = (SELECT count() FROM o_gc_uint16 WHERE kt NOT IN (SELECT (toInt64(1), toUInt8(1)))) AS c3
) ORDER BY ord;
DROP TABLE c_gc_uint16; DROP TABLE o_gc_uint16;
