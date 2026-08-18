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

SELECT '--- v14 has()/composite carriers: the same predicate fixes has() too ---';

DROP TABLE IF EXISTS c_cv; DROP TABLE IF EXISTS o_cv;
CREATE TABLE c_cv (a Int64, b UInt8) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
CREATE TABLE o_cv (a Int64, b UInt8) ENGINE = Memory;
INSERT INTO c_cv VALUES (1, 1), (2, 1);
INSERT INTO o_cv VALUES (1, 1), (2, 1);
SELECT 'CV-has unpacked (Int64,UInt8)/(Decimal(10,2),UInt8)',
    (SELECT count() FROM c_cv WHERE NOT has([(CAST('1.50', 'Decimal(10,2)'), toUInt8(1))], (a, b))) = (SELECT count() FROM o_cv WHERE NOT has([(CAST('1.50', 'Decimal(10,2)'), toUInt8(1))], (a, b)));
SELECT 'CV-in unpacked',
    (SELECT count() FROM c_cv WHERE (a, b) NOT IN (SELECT (CAST('1.50', 'Decimal(10,2)'), toUInt8(1)))) = (SELECT count() FROM o_cv WHERE (a, b) NOT IN (SELECT (CAST('1.50', 'Decimal(10,2)'), toUInt8(1))));

DROP TABLE IF EXISTS c_cvp; DROP TABLE IF EXISTS o_cvp;
CREATE TABLE c_cvp (kt Tuple(Int64, UInt8)) ENGINE = MergeTree ORDER BY kt PARTITION BY kt;
CREATE TABLE o_cvp (kt Tuple(Int64, UInt8)) ENGINE = Memory;
INSERT INTO c_cvp VALUES ((1, 1)), ((2, 1));
INSERT INTO o_cvp VALUES ((1, 1)), ((2, 1));
SELECT 'CV-has packed Tuple(Int64,UInt8)',
    (SELECT count() FROM c_cvp WHERE NOT has([(CAST('1.50', 'Decimal(10,2)'), toUInt8(1))], kt)) = (SELECT count() FROM o_cvp WHERE NOT has([(CAST('1.50', 'Decimal(10,2)'), toUInt8(1))], kt));

DROP TABLE IF EXISTS c_cg; DROP TABLE IF EXISTS o_cg;
CREATE TABLE c_cg (a UInt64, b UInt8) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
CREATE TABLE o_cg (a UInt64, b UInt8) ENGINE = Memory;
INSERT INTO c_cg VALUES (1, 1), (2, 1);
INSERT INTO o_cg VALUES (1, 1), (2, 1);
SELECT 'CG-has (UInt64,UInt8)/(Decimal64(1),UInt8)',
    (SELECT count() FROM c_cg WHERE NOT has([(CAST(1.5, 'Decimal64(1)'), toUInt8(1))], (a, b))) = (SELECT count() FROM o_cg WHERE NOT has([(CAST(1.5, 'Decimal64(1)'), toUInt8(1))], (a, b)));

DROP TABLE IF EXISTS c_cn6; DROP TABLE IF EXISTS o_cn6;
CREATE TABLE c_cn6 (a Decimal(20, 4), b UInt8) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
CREATE TABLE o_cn6 (a Decimal(20, 4), b UInt8) ENGINE = Memory;
INSERT INTO c_cn6 VALUES (1.0001, 1), (2.0000, 1);
INSERT INTO o_cn6 VALUES (1.0001, 1), (2.0000, 1);
-- the truncating Decimal pair under-approximates, so it is the POSITIVE direction that over-prunes
SELECT 'CN6-in (Decimal(20,4),UInt8)/(Decimal(10,2),UInt8)',
    (SELECT count() FROM c_cn6 WHERE (a, b) IN (SELECT (CAST('1.00', 'Decimal(10,2)'), toUInt8(1)))) = (SELECT count() FROM o_cn6 WHERE (a, b) IN (SELECT (CAST('1.00', 'Decimal(10,2)'), toUInt8(1))));
SELECT 'N6-in scalar Decimal(20,4)/Decimal(10,2)',
    (SELECT count() FROM c_cn6 WHERE a IN (SELECT CAST('1.00', 'Decimal(10,2)'))) = (SELECT count() FROM o_cn6 WHERE a IN (SELECT CAST('1.00', 'Decimal(10,2)')));

SELECT '--- composite cross-type over a PACKED composite key: admitted for has(), declined for IN ---';

DROP TABLE IF EXISTS t33; DROP TABLE IF EXISTS t33o;
CREATE TABLE t33 (kt Tuple(UInt32, UInt32)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE t33o (kt Tuple(UInt32, UInt32)) ENGINE = Memory;
INSERT INTO t33 VALUES ((10, 0));
INSERT INTO t33 VALUES ((50000, 0));
INSERT INTO t33 VALUES ((7, 7));
INSERT INTO t33o VALUES ((10, 0)), ((50000, 0)), ((7, 7));
-- With a PACKED composite key column the unpack loop in `tryPrepareSetColumnsForIndex` does not run,
-- so the per-column check receives the whole `Tuple`/`Array` pair. Whether such a pair is exact depends
-- on the CALLER, and these cells pin both halves of that asymmetry on ONE pair of types
-- (`Tuple(UInt16, UInt8)` literal against a `Tuple(UInt32, UInt32)` key, i.e. width-only):
--   `has` casts nothing at runtime and compares `Field`s, which collapse native integer widths, so the
--   atom is exact and must KEEP pruning;
--   `IN` casts the KEY into the set type with `castColumnAccurate`, which THROWS on a narrowing
--   composite instead of nulling, so the same pair must still be declined there.
SELECT 'T33 packed tuple has result',
    (SELECT count() FROM t33 WHERE has([(10, 0), (50000, 0)], kt)) = (SELECT count() FROM t33o WHERE has([(10, 0), (50000, 0)], kt));
SELECT 'T33 packed tuple has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t33 WHERE has([(10, 0), (50000, 0)], kt)) WHERE explain ILIKE '%element set%';
SELECT 'T33 packed tuple NOT has result',
    (SELECT count() FROM t33 WHERE NOT has([(10, 0), (50000, 0)], kt)) = (SELECT count() FROM t33o WHERE NOT has([(10, 0), (50000, 0)], kt));
-- Assert the part reduction, not the atom's own text: a RELAXED atom still prints `element set` while
-- `can_be_false` is forced true before the negation, so negative pruning is off - which the text cannot
-- distinguish from the exact outcome this cell exists to pin. `Parts:` is format-independent.
SELECT 'T33 packed tuple NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t33 WHERE NOT has([(10, 0), (50000, 0)], kt)) WHERE explain ILIKE '%Parts: 1/3%';
-- The `IN` side of the SAME pair: it must still decline, otherwise the runtime cast throws.
SELECT 'T33 packed tuple IN declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t33 WHERE kt IN (SELECT (toUInt16(10), toUInt8(0)))) WHERE explain ILIKE '%element set%';
SELECT 'T33 packed tuple IN result',
    (SELECT count() FROM t33 WHERE kt IN (SELECT (toUInt16(10), toUInt8(0)))) = (SELECT count() FROM t33o WHERE kt IN (SELECT (toUInt16(10), toUInt8(0))));

DROP TABLE IF EXISTS a33; DROP TABLE IF EXISTS a33o;
CREATE TABLE a33 (ak Array(UInt32)) ENGINE = MergeTree ORDER BY ak SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE a33o (ak Array(UInt32)) ENGINE = Memory;
INSERT INTO a33 VALUES ([10, 11]);
INSERT INTO a33 VALUES ([50000, 50001]);
INSERT INTO a33 VALUES ([7, 7]);
INSERT INTO a33o VALUES ([10, 11]), ([50000, 50001]), ([7, 7]);
-- Same width-only shape one container deeper (`Array(Array(UInt16))` literal against an `Array(UInt32)`
-- key), so the same per-caller rule applies and `has` keeps pruning here too.
SELECT 'A33 array key has result',
    (SELECT count() FROM a33 WHERE has([[10, 11], [50000, 50001]], ak)) = (SELECT count() FROM a33o WHERE has([[10, 11], [50000, 50001]], ak));
SELECT 'A33 array key has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM a33 WHERE has([[10, 11], [50000, 50001]], ak)) WHERE explain ILIKE '%element set%';
SELECT 'A33 array key NOT has result',
    (SELECT count() FROM a33 WHERE NOT has([[10, 11], [50000, 50001]], ak)) = (SELECT count() FROM a33o WHERE NOT has([[10, 11], [50000, 50001]], ak));
-- Same `Parts:` idiom as the T33 cell above, and for the same reason.
SELECT 'A33 array key NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM a33 WHERE NOT has([[10, 11], [50000, 50001]], ak)) WHERE explain ILIKE '%Parts: 1/3%';

-- A nested `Nullable` on ONE side only. It is not represented in a `Field` at all (a non-NULL value
-- carries the same variant as its plain counterpart, a NULL carries `Types::Null` and matches nothing),
-- and the preparation cast maps a NULL element to a NULL WHOLE composite, which the caller filters out.
-- Two shapes, because they are fixed by different code: the PACKED one goes through the per-column
-- check on the whole composite, the UNPACKED one through the composite identity rule.

DROP TABLE IF EXISTS n33; DROP TABLE IF EXISTS n33o;
CREATE TABLE n33 (kt Tuple(Nullable(UInt32), Nullable(UInt32))) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, allow_nullable_key = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE n33o (kt Tuple(Nullable(UInt32), Nullable(UInt32))) ENGINE = Memory;
INSERT INTO n33 SELECT tuple(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'));
INSERT INTO n33 SELECT tuple(CAST(50000, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'));
INSERT INTO n33 SELECT tuple(CAST(7, 'Nullable(UInt32)'), CAST(7, 'Nullable(UInt32)'));
INSERT INTO n33o SELECT tuple(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'));
INSERT INTO n33o SELECT tuple(CAST(50000, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'));
INSERT INTO n33o SELECT tuple(CAST(7, 'Nullable(UInt32)'), CAST(7, 'Nullable(UInt32)'));
SELECT 'N33 packed nested-Nullable has result',
    (SELECT count() FROM n33 WHERE has([(10, 0), (50000, 0), (0, NULL), (NULL, 10)], kt)) = (SELECT count() FROM n33o WHERE has([(10, 0), (50000, 0), (0, NULL), (NULL, 10)], kt));
SELECT 'N33 packed nested-Nullable has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM n33 WHERE has([(10, 0), (50000, 0), (0, NULL), (NULL, 10)], kt)) WHERE explain ILIKE '%element set%';
SELECT 'N33 packed nested-Nullable NOT has result',
    (SELECT count() FROM n33 WHERE NOT has([(10, 0), (50000, 0), (0, NULL), (NULL, 10)], kt)) = (SELECT count() FROM n33o WHERE NOT has([(10, 0), (50000, 0), (0, NULL), (NULL, 10)], kt));
SELECT 'N33 packed nested-Nullable NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM n33 WHERE NOT has([(10, 0), (50000, 0), (0, NULL), (NULL, 10)], kt)) WHERE explain ILIKE '%Parts: 1/3%';

DROP TABLE IF EXISTS u33; DROP TABLE IF EXISTS u33o;
CREATE TABLE u33 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE u33o (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO u33 VALUES (10, 0);
INSERT INTO u33 VALUES (50000, 0);
INSERT INTO u33 VALUES (7, 7);
-- The all-default tuple. It is the value a dropped source NULL turns into on this path, so it is the
-- only row that can witness the unpacked shape admitting a one-sided `Nullable`: the per-scalar
-- conversion strips the wrapper and reads the nested column, whose value at a NULL row is the type
-- default, so `(NULL, NULL)` would enter the pruning set as `(0, 0)` and prune this very part.
INSERT INTO u33 VALUES (0, 0);
INSERT INTO u33o VALUES (10, 0), (50000, 0), (7, 7), (0, 0);
SELECT 'U33 unpacked nested-Nullable has result',
    (SELECT count() FROM u33 WHERE has([(10, 0), (50000, 0), (NULL, NULL)], (a, b))) = (SELECT count() FROM u33o WHERE has([(10, 0), (50000, 0), (NULL, NULL)], (a, b)));
-- The unpacked shape must DECLINE a one-sided nested `Nullable`, so all four single-row parts are read.
-- `Parts:` is asserted rather than the atom's own text because a RELAXED atom still prints that text
-- while its `can_be_false` is forced true, which cannot be told apart from the exact outcome.
SELECT 'U33 unpacked nested-Nullable has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u33 WHERE has([(10, 0), (50000, 0), (NULL, NULL)], (a, b))) WHERE explain ILIKE '%Parts: 4/4%';
SELECT 'U33 unpacked nested-Nullable NOT has result',
    (SELECT count() FROM u33 WHERE NOT has([(10, 0), (50000, 0), (NULL, NULL)], (a, b))) = (SELECT count() FROM u33o WHERE NOT has([(10, 0), (50000, 0), (NULL, NULL)], (a, b)));
SELECT 'U33 unpacked nested-Nullable NOT has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u33 WHERE NOT has([(10, 0), (50000, 0), (NULL, NULL)], (a, b))) WHERE explain ILIKE '%Parts: 4/4%';

-- The PACKED counterpart of the same one-sided shape, which is where the tolerance IS sound: the whole
-- composite is cast by `castColumnAccurateOrNull`, so the `(NULL, NULL)` element nulls the entire tuple
-- and `tryPrepareSetColumnsForIndex` filters that set row out (the plan below reports a `2-element set`
-- for a 3-element literal). The atom therefore stays exact and keeps pruning, including under negation.
-- This pair of shapes is what makes the per-caller gating load-bearing rather than a blanket decline.

DROP TABLE IF EXISTS q33; DROP TABLE IF EXISTS q33o;
CREATE TABLE q33 (kt Tuple(UInt32, UInt32)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE q33o (kt Tuple(UInt32, UInt32)) ENGINE = Memory;
INSERT INTO q33 VALUES ((10, 0));
INSERT INTO q33 VALUES ((50000, 0));
INSERT INTO q33 VALUES ((0, 0));
INSERT INTO q33o VALUES ((10, 0)), ((50000, 0)), ((0, 0));
SELECT 'Q33 packed one-sided Nullable has result',
    (SELECT count() FROM q33 WHERE has([(10, 0), (50000, 0), (NULL, NULL)], kt)) = (SELECT count() FROM q33o WHERE has([(10, 0), (50000, 0), (NULL, NULL)], kt));
SELECT 'Q33 packed one-sided Nullable NOT has result',
    (SELECT count() FROM q33 WHERE NOT has([(10, 0), (50000, 0), (NULL, NULL)], kt)) = (SELECT count() FROM q33o WHERE NOT has([(10, 0), (50000, 0), (NULL, NULL)], kt));
SELECT 'Q33 packed one-sided Nullable NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM q33 WHERE NOT has([(10, 0), (50000, 0), (NULL, NULL)], kt)) WHERE explain ILIKE '%Parts: 1/3%';

-- The unpacked decline above is driven by an actual source NULL, not by the `Nullable` TYPE: the harm is
-- that stripping the wrapper reads the nested column, whose value at a NULL row is the type default. A
-- `Nullable`-typed element carrying no NULL has no such row, so the per-scalar conversion is
-- value-preserving and the atom stays exact. Declining on the type alone would lose this pruning, so this
-- cell is what keeps the `U33` decline value-sensitive rather than a blanket type rule. Reuses the `u33`
-- shape, including its all-default row.
SELECT 'V33 unpacked NULL-free Nullable has result',
    (SELECT count() FROM u33 WHERE has([(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'))], (a, b))) = (SELECT count() FROM u33o WHERE has([(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'))], (a, b)));
SELECT 'V33 unpacked NULL-free Nullable has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u33 WHERE has([(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'))], (a, b))) WHERE explain ILIKE '%Parts: 1/4%';
SELECT 'V33 unpacked NULL-free Nullable NOT has result',
    (SELECT count() FROM u33 WHERE NOT has([(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'))], (a, b))) = (SELECT count() FROM u33o WHERE NOT has([(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'))], (a, b)));
-- The NEGATIVE direction has to be asserted separately: a relaxed atom keeps positive pruning while
-- losing all negative pruning, because `can_be_false` is forced true for any relaxed element. A control
-- that only checks the `has` direction therefore stays green through a regression that silently kills
-- `NOT has` pruning.
SELECT 'V33 unpacked NULL-free Nullable NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u33 WHERE NOT has([(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)'))], (a, b))) WHERE explain ILIKE '%Parts: 3/4%';
-- The NULL that forfeits exactness need not be in the first row or the first position: the check is over
-- the whole constant. A NULL appearing only in a LATER row still declines.
SELECT 'V33 unpacked NULL in a later row declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u33 WHERE has([(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)')), (CAST(50000, 'Nullable(UInt32)'), CAST(NULL, 'Nullable(UInt32)'))], (a, b))) WHERE explain ILIKE '%Parts: 4/4%';
SELECT 'V33 unpacked NULL in a later row result',
    (SELECT count() FROM u33 WHERE has([(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)')), (CAST(50000, 'Nullable(UInt32)'), CAST(NULL, 'Nullable(UInt32)'))], (a, b))) = (SELECT count() FROM u33o WHERE has([(CAST(10, 'Nullable(UInt32)'), CAST(0, 'Nullable(UInt32)')), (CAST(50000, 'Nullable(UInt32)'), CAST(NULL, 'Nullable(UInt32)'))], (a, b)));
-- `LowCardinality` has to be seen through as well, in both directions: its dictionary can hold an
-- unreferenced NULL slot, so the decision is made on the materialized values. Without that branch the
-- no-NULL case would fail closed and lose this pruning.
SET allow_suspicious_low_cardinality_types = 1;
SELECT 'V33 unpacked LC(Nullable) NULL-free keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u33 WHERE has([(CAST(10, 'LowCardinality(Nullable(UInt32))'), CAST(0, 'LowCardinality(Nullable(UInt32))'))], (a, b))) WHERE explain ILIKE '%Parts: 1/4%';
SELECT 'V33 unpacked LC(Nullable) with NULL declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM u33 WHERE has([(CAST(10, 'LowCardinality(Nullable(UInt32))'), CAST(0, 'LowCardinality(Nullable(UInt32))')), (NULL, NULL)], (a, b))) WHERE explain ILIKE '%Parts: 4/4%';
SELECT 'V33 unpacked LC(Nullable) with NULL result',
    (SELECT count() FROM u33 WHERE has([(CAST(10, 'LowCardinality(Nullable(UInt32))'), CAST(0, 'LowCardinality(Nullable(UInt32))')), (NULL, NULL)], (a, b))) = (SELECT count() FROM u33o WHERE has([(CAST(10, 'LowCardinality(Nullable(UInt32))'), CAST(0, 'LowCardinality(Nullable(UInt32))')), (NULL, NULL)], (a, b)));
SET allow_suspicious_low_cardinality_types = 0;

-- Tuple field names are only mapped by the preparation cast when BOTH sides declare them explicitly:
-- `createTupleWrapper` takes its name-matching branch under `from_type->hasExplicitNames() &&
-- to_type->hasExplicitNames()` and otherwise converts POSITIONALLY. Measured on the cast itself:
--     accurateCastOrNull(CAST(tuple(1), 'Tuple(d UInt8)'), 'Tuple(c UInt8)') = (0)   both explicit
--     accurateCastOrNull(CAST(tuple(1), 'Tuple(d UInt8)'), 'Tuple(UInt8)')   = (1)   one-sided
--     accurateCastOrNull(CAST(tuple(1), 'Tuple(UInt8)'),   'Tuple(c UInt8)') = (1)   one-sided
-- So a one-sided-explicit pair preserves equality in both directions and must keep pruning, while a
-- both-explicit differing pair must still decline. Both directions of the one-sided case are covered.
SELECT 'W33 packed unnamed key vs named element result',
    (SELECT count() FROM q33 WHERE has([CAST((10, 0), 'Tuple(c UInt32, d UInt32)')], kt)) = (SELECT count() FROM q33o WHERE has([CAST((10, 0), 'Tuple(c UInt32, d UInt32)')], kt));
SELECT 'W33 packed unnamed key vs named element keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM q33 WHERE has([CAST((10, 0), 'Tuple(c UInt32, d UInt32)')], kt)) WHERE explain ILIKE '%Parts: 1/3%';
SELECT 'W33 packed unnamed key vs named element NOT has result',
    (SELECT count() FROM q33 WHERE NOT has([CAST((10, 0), 'Tuple(c UInt32, d UInt32)')], kt)) = (SELECT count() FROM q33o WHERE NOT has([CAST((10, 0), 'Tuple(c UInt32, d UInt32)')], kt));
SELECT 'W33 packed unnamed key vs named element NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM q33 WHERE NOT has([CAST((10, 0), 'Tuple(c UInt32, d UInt32)')], kt)) WHERE explain ILIKE '%Parts: 2/3%';

DROP TABLE IF EXISTS w33; DROP TABLE IF EXISTS w33o;
CREATE TABLE w33 (kt Tuple(x UInt32, y UInt32)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE w33o (kt Tuple(x UInt32, y UInt32)) ENGINE = Memory;
INSERT INTO w33 VALUES ((10, 0));
INSERT INTO w33 VALUES ((50000, 0));
INSERT INTO w33 VALUES ((0, 0));
INSERT INTO w33o VALUES ((10, 0)), ((50000, 0)), ((0, 0));
SELECT 'W33 packed named key vs unnamed element result',
    (SELECT count() FROM w33 WHERE has([(toUInt32(10), toUInt32(0))], kt)) = (SELECT count() FROM w33o WHERE has([(toUInt32(10), toUInt32(0))], kt));
SELECT 'W33 packed named key vs unnamed element keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w33 WHERE has([(toUInt32(10), toUInt32(0))], kt)) WHERE explain ILIKE '%Parts: 1/3%';
SELECT 'W33 packed named key vs unnamed element NOT has result',
    (SELECT count() FROM w33 WHERE NOT has([(toUInt32(10), toUInt32(0))], kt)) = (SELECT count() FROM w33o WHERE NOT has([(toUInt32(10), toUInt32(0))], kt));
SELECT 'W33 packed named key vs unnamed element NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w33 WHERE NOT has([(toUInt32(10), toUInt32(0))], kt)) WHERE explain ILIKE '%Parts: 2/3%';
-- The both-explicit differing pair the tolerance must NOT admit: the cast zeroes the values, so treating
-- the atom as exact prunes the real all-default part and drops a row (master answers this one wrongly).
SELECT 'W33 packed both-explicit differing names result',
    (SELECT count() FROM w33 WHERE has([CAST((10, 0), 'Tuple(c UInt32, d UInt32)')], kt)) = (SELECT count() FROM w33o WHERE has([CAST((10, 0), 'Tuple(c UInt32, d UInt32)')], kt));
SELECT 'W33 packed both-explicit differing names declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w33 WHERE has([CAST((10, 0), 'Tuple(c UInt32, d UInt32)')], kt)) WHERE explain ILIKE '%Parts: 3/3%';
-- ... and the both-explicit AGREEING pair still prunes, so the guard is name-sensitive rather than a
-- decline of every named key.
SELECT 'W33 packed both-explicit same names keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w33 WHERE has([CAST((10, 0), 'Tuple(x UInt32, y UInt32)')], kt)) WHERE explain ILIKE '%Parts: 1/3%';

-- A NESTED named tuple, in the unpacked shape, where the per-scalar cast IS the one being name-mapped.
-- Differing explicit names zero the value and must decline; a one-sided pair is positional and prunes.

DROP TABLE IF EXISTS y33; DROP TABLE IF EXISTS y33o;
CREATE TABLE y33 (a Tuple(c UInt32), b UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE y33o (a Tuple(c UInt32), b UInt32) ENGINE = Memory;
INSERT INTO y33 VALUES (tuple(10), 0);
INSERT INTO y33 VALUES (tuple(50000), 0);
INSERT INTO y33 VALUES (tuple(0), 0);
INSERT INTO y33o VALUES (tuple(10), 0);
INSERT INTO y33o VALUES (tuple(50000), 0);
INSERT INTO y33o VALUES (tuple(0), 0);
SELECT 'Y33 unpacked nested differing names result',
    (SELECT count() FROM y33 WHERE has([(CAST(tuple(10), 'Tuple(d UInt32)'), toUInt32(0))], (a, b))) = (SELECT count() FROM y33o WHERE has([(CAST(tuple(10), 'Tuple(d UInt32)'), toUInt32(0))], (a, b)));
SELECT 'Y33 unpacked nested differing names declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM y33 WHERE has([(CAST(tuple(10), 'Tuple(d UInt32)'), toUInt32(0))], (a, b))) WHERE explain ILIKE '%Parts: 3/3%';
SELECT 'Y33 unpacked nested one-sided names result',
    (SELECT count() FROM y33 WHERE has([(tuple(toUInt32(10)), toUInt32(0))], (a, b))) = (SELECT count() FROM y33o WHERE has([(tuple(toUInt32(10)), toUInt32(0))], (a, b)));
SELECT 'Y33 unpacked nested one-sided names keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM y33 WHERE has([(tuple(toUInt32(10)), toUInt32(0))], (a, b))) WHERE explain ILIKE '%Parts: 1/3%';
SELECT 'Y33 unpacked nested one-sided names NOT has result',
    (SELECT count() FROM y33 WHERE NOT has([(tuple(toUInt32(10)), toUInt32(0))], (a, b))) = (SELECT count() FROM y33o WHERE NOT has([(tuple(toUInt32(10)), toUInt32(0))], (a, b)));
SELECT 'Y33 unpacked nested one-sided names NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM y33 WHERE NOT has([(tuple(toUInt32(10)), toUInt32(0))], (a, b))) WHERE explain ILIKE '%Parts: 2/3%';

-- A nested container BETWEEN the packed root and a one-sided `Nullable` wrapper. The element type is
-- `Tuple(Array(Nullable(UInt16)), UInt8)` against a `Tuple(Array(UInt32), UInt32)` key, so the wrapper
-- sits one container deeper than the tuple root - deeper than the whole-composite cast's null
-- aggregation reaches, since that lives in `createTupleWrapper` and is top-level only, while
-- `createArrayWrapper` rebuilds the array with its original offsets and leaves a nullable data column
-- nested. So the tolerance has no mechanism behind it here and the container arms decline.
--
-- Declining is also what keeps the query working at all: this pair never converts, it FAILS. The cast's
-- target validation walks `Tuple` elements and rejects a nested type that cannot be inside `Nullable`
-- (`validateNestedTypesForAccurateCastOrNull`; `canBeInsideNullable` is false for both `Array` and
-- `Map`), so admitting the pair reached `castColumnAccurateOrNull` and failed the whole query with
-- `ILLEGAL_TYPE_OF_ARGUMENT` - measured identically on master, i.e. pre-existing. Now the atom is left
-- unused and the answer matches the oracle. Same for a `Map` in the same position.

DROP TABLE IF EXISTS z33; DROP TABLE IF EXISTS z33o;
CREATE TABLE z33 (kt Tuple(Array(UInt32), UInt32)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE z33o (kt Tuple(Array(UInt32), UInt32)) ENGINE = Memory;
INSERT INTO z33 VALUES (([10], 0));
INSERT INTO z33 VALUES (([50000], 0));
INSERT INTO z33 VALUES (([0], 0));
INSERT INTO z33o VALUES (([10], 0)), (([50000], 0)), (([0], 0));
SELECT 'Z33 nested Array under packed root element type', toTypeName([([10], 0), ([50000], 0), ([NULL], 0)]);
SELECT 'Z33 nested Array under packed root has result',
    (SELECT count() FROM z33 WHERE has([([10], 0), ([50000], 0), ([NULL], 0)], kt)) = (SELECT count() FROM z33o WHERE has([([10], 0), ([50000], 0), ([NULL], 0)], kt));
SELECT 'Z33 nested Array under packed root NOT has result',
    (SELECT count() FROM z33 WHERE NOT has([([10], 0), ([50000], 0), ([NULL], 0)], kt)) = (SELECT count() FROM z33o WHERE NOT has([([10], 0), ([50000], 0), ([NULL], 0)], kt));

DROP TABLE IF EXISTS z34; DROP TABLE IF EXISTS z34o;
CREATE TABLE z34 (kt Tuple(Map(UInt32, UInt32), UInt32)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE z34o (kt Tuple(Map(UInt32, UInt32), UInt32)) ENGINE = Memory;
INSERT INTO z34 VALUES ((map(1, 10), 0));
INSERT INTO z34 VALUES ((map(1, 50000), 0));
INSERT INTO z34 VALUES ((map(1, 0), 0));
INSERT INTO z34o VALUES ((map(1, 10), 0)), ((map(1, 50000), 0)), ((map(1, 0), 0));
SELECT 'Z34 nested Map under packed root element type', toTypeName([(map(1, 10), 0), (map(1, 50000), 0), (map(1, NULL), 0)]);
SELECT 'Z34 nested Map under packed root NOT has result',
    (SELECT count() FROM z34 WHERE NOT has([(map(1, 10), 0), (map(1, 50000), 0), (map(1, NULL), 0)], kt)) = (SELECT count() FROM z34o WHERE NOT has([(map(1, 10), 0), (map(1, 50000), 0), (map(1, NULL), 0)], kt));
-- The same one-sided wrapper on the KEY side instead, nested under the same `Array`. This one is exact
-- either way - the element casts safely into the key type, so the plain `castColumn` path runs and no
-- `accurateOrNull` validation is involved - but it shares the declining container arm, so it loses
-- pruning as the price of the two cells above. It answers correctly, and this cell records the cost so a
-- future narrowing or widening of that arm is measured rather than assumed.

DROP TABLE IF EXISTS z35; DROP TABLE IF EXISTS z35o;
CREATE TABLE z35 (kt Tuple(Array(Nullable(UInt32)), UInt32)) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0, allow_nullable_key = 1;
CREATE TABLE z35o (kt Tuple(Array(Nullable(UInt32)), UInt32)) ENGINE = Memory;
INSERT INTO z35 VALUES (([10], 0));
INSERT INTO z35 VALUES (([50000], 0));
INSERT INTO z35 VALUES (([0], 0));
INSERT INTO z35o VALUES (([10], 0)), (([50000], 0)), (([0], 0));
SELECT 'Z35 key-side nested Nullable has result',
    (SELECT count() FROM z35 WHERE has([(CAST([10], 'Array(UInt8)'), toUInt8(0))], kt)) = (SELECT count() FROM z35o WHERE has([(CAST([10], 'Array(UInt8)'), toUInt8(0))], kt));
SELECT 'Z35 key-side nested Nullable has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM z35 WHERE has([(CAST([10], 'Array(UInt8)'), toUInt8(0))], kt)) WHERE explain ILIKE '%Parts: 3/3%';
SELECT 'Z35 key-side nested Nullable NOT has result',
    (SELECT count() FROM z35 WHERE NOT has([(CAST([10], 'Array(UInt8)'), toUInt8(0))], kt)) = (SELECT count() FROM z35o WHERE NOT has([(CAST([10], 'Array(UInt8)'), toUInt8(0))], kt));

-- Native widths collapse in a Field, signedness does not, and the 128/256-bit tags do not either:
-- this is exactly the boundary the composite identity rule has to draw.
SELECT 'Field width u8 vs u64', has([tuple(toUInt8(1))], tuple(toUInt64(1)));
SELECT 'Field width i8 vs i64', has([tuple(toInt8(1))], tuple(toInt64(1)));
SELECT 'Field signedness i32 vs u32', has([tuple(toInt32(1))], tuple(toUInt32(1)));
SELECT 'Field width u64 vs u128', has([tuple(toUInt64(1))], tuple(toUInt128(1)));
SELECT 'Field width u128 vs u256', has([tuple(toUInt128(1))], tuple(toUInt256(1)));
-- A scalar element takes the other rule (the preparation cast, which nulls instead of truncating), so
-- native-vs-128-bit DOES match there. The two rows below are the asymmetry that forbids unifying them.
SELECT 'Field scalar u64 vs u128', has([toUInt64(1)], toUInt128(1));

-- The boundary of the has()-only composite tolerance, asserted over the PACKED key that reaches it (the
-- cells further down assert the same boundary on the UNPACKED shape, which a different gate decides).
-- Each pair below is one the runtime `has` does NOT match, so admitting it would claim exactness for a
-- pair whose answer differs, and each must therefore still decline.
SELECT 'T33 packed signedness declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t33 WHERE has([tuple(toInt16(10), toInt16(0))], kt)) WHERE explain ILIKE '%element set%';
SELECT 'T33 packed signedness result',
    (SELECT count() FROM t33 WHERE has([tuple(toInt16(10), toInt16(0))], kt)) = (SELECT count() FROM t33o WHERE has([tuple(toInt16(10), toInt16(0))], kt));
SELECT 'T33 packed 128-bit declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t33 WHERE has([tuple(toUInt128(10), toUInt128(0))], kt)) WHERE explain ILIKE '%element set%';
SELECT 'T33 packed 128-bit result',
    (SELECT count() FROM t33 WHERE has([tuple(toUInt128(10), toUInt128(0))], kt)) = (SELECT count() FROM t33o WHERE has([tuple(toUInt128(10), toUInt128(0))], kt));

SELECT '--- composite has() over TWO key columns: the one shape where the composite rule decides ---';

-- With a two-column key the per-column checks see the UNPACKED scalars and admit any integer pair, so
-- the composite rule is the deciding gate here and these cells are what pin it. (With a PACKED tuple or
-- array key the per-column check sees the whole composite instead; for `has` it applies the same
-- `Field`-identity tolerance, so the two shapes agree - see the T33/A33 cells above.)

DROP TABLE IF EXISTS w2c; DROP TABLE IF EXISTS o2c;
CREATE TABLE w2c (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE o2c (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO w2c VALUES (10, 0); INSERT INTO w2c VALUES (50000, 0); INSERT INTO w2c VALUES (7, 7);
INSERT INTO o2c VALUES (10, 0), (50000, 0), (7, 7);

-- Width-only: the literal is `Array(Tuple(UInt16, UInt8))` against a `(UInt32, UInt32)` key, which the
-- runtime compares identically, so the atom must KEEP pruning.
SELECT 'W2C width-only has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w2c WHERE has([(10, 0), (50000, 0)], (a, b))) WHERE explain ILIKE '%element set%';
SELECT 'W2C width-only has',
    (SELECT count() FROM w2c WHERE has([(10, 0), (50000, 0)], (a, b))) = (SELECT count() FROM o2c WHERE has([(10, 0), (50000, 0)], (a, b)));

-- 128-bit: a distinct `Field` variant, so the runtime never matches the pair and the atom must decline.
SELECT 'W2C 128-bit has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM w2c WHERE has([tuple(toUInt128(10), toUInt128(0)), tuple(toUInt128(50000), toUInt128(0))], (a, b))) WHERE explain ILIKE '%element set%';
SELECT 'W2C 128-bit has',
    (SELECT count() FROM w2c WHERE has([tuple(toUInt128(10), toUInt128(0)), tuple(toUInt128(50000), toUInt128(0))], (a, b))) = (SELECT count() FROM o2c WHERE has([tuple(toUInt128(10), toUInt128(0)), tuple(toUInt128(50000), toUInt128(0))], (a, b)));
DROP TABLE w2c; DROP TABLE o2c;

-- Signedness, the other direction of the same rule: an `Int32` key against an unsigned literal.
