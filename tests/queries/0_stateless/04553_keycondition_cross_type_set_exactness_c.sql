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

SELECT '--- has(): reachable cross-type, governed by the same predicate ---';

DROP TABLE IF EXISTS h_t; DROP TABLE IF EXISTS h_o;
CREATE TABLE h_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE h_o (k UInt64) ENGINE = Memory;
INSERT INTO h_t VALUES (1), (2), (3);
INSERT INTO h_o VALUES (1), (2), (3);
SELECT 'has UInt64/Int32 result',
    (SELECT count() FROM h_t WHERE has([toInt32(1)], k)) = (SELECT count() FROM h_o WHERE has([toInt32(1)], k));
SELECT 'has UInt64/Int32 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h_t WHERE has([toInt32(1)], k)) WHERE explain ILIKE '%in 1-element set%';
SELECT 'has UInt64/Int64 -1 result',
    (SELECT count() FROM h_t WHERE has([toInt64(-1)], k)) = (SELECT count() FROM h_o WHERE has([toInt64(-1)], k));
SELECT 'has UInt64/UInt8 mixed result',
    (SELECT count() FROM h_t WHERE has([toUInt8(1), toUInt8(2)], k)) = (SELECT count() FROM h_o WHERE has([toUInt8(1), toUInt8(2)], k));

SELECT '--- consumers of exactness ---';

-- extractPlainRanges fast path over numbers(): the declined atom must not corrupt the answer.
SELECT 'numbers exact range', count() FROM numbers(3) WHERE number NOT IN (SELECT '01');

DROP TABLE IF EXISTS nk_t; DROP TABLE IF EXISTS nk_o;
CREATE TABLE nk_t (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
CREATE TABLE nk_o (k Nullable(UInt64)) ENGINE = Memory;
INSERT INTO nk_t VALUES (1), (2);
INSERT INTO nk_o VALUES (1), (2);
SELECT 'Nullable key',
    (SELECT count() FROM nk_t WHERE k NOT IN (SELECT '01')) = (SELECT count() FROM nk_o WHERE k NOT IN (SELECT '01'));

DROP TABLE IF EXISTS mm_t; DROP TABLE IF EXISTS mm_o;
CREATE TABLE mm_t (k UInt64, v UInt64, INDEX v_mm v TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY k;
CREATE TABLE mm_o (k UInt64, v UInt64) ENGINE = Memory;
INSERT INTO mm_t VALUES (1, 1), (2, 2);
INSERT INTO mm_o VALUES (1, 1), (2, 2);
SELECT 'minmax on non-PK column',
    (SELECT count() FROM mm_t WHERE v NOT IN (SELECT '01')) = (SELECT count() FROM mm_o WHERE v NOT IN (SELECT '01'));

-- transform_null_in = 1 takes a different runtime cast; its behaviour must be unchanged here.
DROP TABLE IF EXISTS tn_t; DROP TABLE IF EXISTS tn_o;
CREATE TABLE tn_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE tn_o (k UInt64) ENGINE = Memory;
INSERT INTO tn_t VALUES (1), (2);
INSERT INTO tn_o VALUES (1), (2);
SELECT 'transform_null_in=1', count() FROM tn_t WHERE k IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1;
SELECT 'transform_null_in=1 prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM tn_t WHERE k IN (SELECT toUInt8(1)) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%in 1-element set%';

SELECT '--- results still correct for pairs that now lose pruning ---';

DROP TABLE IF EXISTS pl_t; DROP TABLE IF EXISTS pl_o;
CREATE TABLE pl_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE pl_o (k UInt64) ENGINE = Memory;
INSERT INTO pl_t VALUES (1), (2);
INSERT INTO pl_o VALUES (1), (2);
SELECT c1, c2, c3 FROM (
    SELECT 1 AS ord, 'loses pruning UInt64/String 1' AS c1,
    (SELECT count() FROM pl_t WHERE k IN (SELECT '1')) = (SELECT count() FROM pl_o WHERE k IN (SELECT '1')) AS c2,
    (SELECT count() FROM pl_t WHERE k NOT IN (SELECT '1')) = (SELECT count() FROM pl_o WHERE k NOT IN (SELECT '1')) AS c3
    UNION ALL
    SELECT 2 AS ord, 'loses pruning UInt64/Float64 1.5' AS c1,
    (SELECT count() FROM pl_t WHERE k IN (SELECT toFloat64(1.5))) = (SELECT count() FROM pl_o WHERE k IN (SELECT toFloat64(1.5))) AS c2,
    (SELECT count() FROM pl_t WHERE k NOT IN (SELECT toFloat64(1.5))) = (SELECT count() FROM pl_o WHERE k NOT IN (SELECT toFloat64(1.5))) AS c3
) ORDER BY ord;
SELECT 'loses pruning UInt64/DateTime',
    (SELECT count() FROM pl_t WHERE k IN (SELECT toDateTime(1))) = (SELECT count() FROM pl_o WHERE k IN (SELECT toDateTime(1)));

SELECT '--- unchanged: identical-type float atoms (separate defect, not this fix) ---';

-- These assert master's CURRENT answers. The index/runtime float equality mismatch (-0.0 vs +0.0,
-- distinct NaN payloads) is a different root cause and is deliberately untouched: identical types
-- run no conversion, so a conversion-exactness rule has nothing to say about them.
DROP TABLE IF EXISTS fz_64;
CREATE TABLE fz_64 (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO fz_64 VALUES (-0.0), (0.0);
SELECT 'float signed zero subquery', count() FROM fz_64 WHERE k NOT IN (SELECT toFloat64(0.0));
SELECT 'float signed zero literal', count() FROM fz_64 WHERE k NOT IN (0.0);

DROP TABLE IF EXISTS fz_32;
CREATE TABLE fz_32 (k Float32) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO fz_32 VALUES (-0.0), (0.0);
SELECT 'float32 signed zero', count() FROM fz_32 WHERE k NOT IN (SELECT toFloat32(0.0));

DROP TABLE IF EXISTS fn_64;
CREATE TABLE fn_64 (k Float64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO fn_64 SELECT nan UNION ALL SELECT reinterpret(9221120237041090561::UInt64, 'Float64');
SELECT 'float NaN payloads', count() FROM fn_64 WHERE k NOT IN (SELECT nan);

DROP TABLE IF EXISTS ft_64;
CREATE TABLE ft_64 (a Float64, b UInt8) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO ft_64 VALUES (-0.0, 1), (0.0, 1);
SELECT 'float tuple key', count() FROM ft_64 WHERE (a, b) NOT IN (SELECT (toFloat64(0.0), toUInt8(1)));

SELECT '--- v21 H1: composite has() through the UNPACKING path ---';

DROP TABLE IF EXISTS h1; DROP TABLE IF EXISTS h1o;
CREATE TABLE h1 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE h1o (a UInt32, b UInt32) ENGINE = Memory;
INSERT INTO h1 VALUES (1, 1);
INSERT INTO h1 VALUES (2, 2);
INSERT INTO h1o VALUES (1, 1), (2, 2);
SELECT 'H1 composite has unpacked',
    (SELECT count() FROM h1 WHERE NOT has([tuple(toInt32(1), toInt32(1))], (a, b))) = (SELECT count() FROM h1o WHERE NOT has([tuple(toInt32(1), toInt32(1))], (a, b)));
-- The one-line reason: a composite is compared as ONE Field, so cross-signedness nested values are
-- unequal even though the unpacked scalars would be admitted.
SELECT 'H1 composite Field compare', has([tuple(toInt32(1), toInt32(1))], tuple(toUInt32(1), toUInt32(1)));
SELECT 'H1 scalar Field compare', has([toInt32(1)], toUInt32(1));
SELECT 'H1 cross-signedness has declines', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1 WHERE NOT has([tuple(toInt32(1), toInt32(1))], (a, b))) WHERE explain ILIKE '%element set%';
-- boundary: identical types keep pruning
SELECT 'H1 same-type has result',
    (SELECT count() FROM h1 WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b))) = (SELECT count() FROM h1o WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b)));
SELECT 'H1 same-type has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1 WHERE NOT has([tuple(toUInt32(1), toUInt32(1))], (a, b))) WHERE explain ILIKE '%Granules: 1/2%';
-- boundary: a width-only pair renders to the same `Field`, so the runtime compares it identically and
-- the atom stays exact. Asserting the granule reduction, not the atom, because a relaxed atom is still
-- printed while `can_be_false` is forced true before the negation.
SELECT 'H1 width-only has result',
    (SELECT count() FROM h1 WHERE NOT has([tuple(toUInt8(1), toUInt8(1))], (a, b))) = (SELECT count() FROM h1o WHERE NOT has([tuple(toUInt8(1), toUInt8(1))], (a, b)));
SELECT 'H1 width-only has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1 WHERE NOT has([tuple(toUInt8(1), toUInt8(1))], (a, b))) WHERE explain ILIKE '%Granules: 1/2%';
-- boundary: composite NOT IN over the same pair stays exact, because runtime `IN` casts the key
SELECT 'H1 composite IN result',
    (SELECT count() FROM h1 WHERE (a, b) NOT IN (SELECT (toInt32(1), toInt32(1)))) = (SELECT count() FROM h1o WHERE (a, b) NOT IN (SELECT (toInt32(1), toInt32(1))));
SELECT 'H1 composite IN prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1 WHERE (a, b) NOT IN (SELECT (toInt32(1), toInt32(1)))) WHERE explain ILIKE '%Granules: 1/2%';
-- boundary: SCALAR has() is unaffected and must keep pruning

DROP TABLE IF EXISTS h1s; DROP TABLE IF EXISTS h1so;
CREATE TABLE h1s (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE h1so (k UInt64) ENGINE = Memory;
INSERT INTO h1s VALUES (1), (2), (3);
INSERT INTO h1so VALUES (1), (2), (3);
SELECT 'H1 scalar has result',
    (SELECT count() FROM h1s WHERE has([toInt32(1)], k)) = (SELECT count() FROM h1so WHERE has([toInt32(1)], k)),
    (SELECT count() FROM h1s WHERE NOT has([toInt32(1)], k)) = (SELECT count() FROM h1so WHERE NOT has([toInt32(1)], k));
SELECT 'H1 scalar has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1s WHERE has([toInt32(1)], k)) WHERE explain ILIKE '%element set%';
SELECT 'H1 scalar NOT has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1s WHERE NOT has([toInt32(1)], k)) WHERE explain ILIKE '%Parts: 2/3%';
-- boundary: a composite KEY EXPRESSION under a SCALAR has() is not a composite comparison at all
DROP TABLE IF EXISTS h1x;
CREATE TABLE h1x (p String) ENGINE = MergeTree ORDER BY reverse(tuple(reverse(p), hex(p))) SETTINGS index_granularity = 1;
INSERT INTO h1x VALUES ('abc'), ('xyz');
SELECT 'H1 composite key expr scalar has prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM h1x WHERE has(['abc'], p) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain ILIKE '%element set%';

SELECT '--- v18 B1: a custom name over an integer must not skip the conversion-target check ---';

DROP TABLE IF EXISTS b1; DROP TABLE IF EXISTS b1o;
CREATE TABLE b1 (k UInt8) ENGINE = MergeTree ORDER BY toString(k) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE b1o (k UInt8) ENGINE = Memory;
INSERT INTO b1 VALUES (0);
INSERT INTO b1 VALUES (1);
INSERT INTO b1o VALUES (0), (1);
SELECT 'B1 Bool element over toString key',
    (SELECT count() FROM b1 WHERE k IN (SELECT true)) = (SELECT count() FROM b1o WHERE k IN (SELECT true));
-- the DAG output really differs, which is why a conversion runs and has to be checked
SELECT 'B1 toString(Bool) differs', toString(true) != toString(toUInt8(1));
-- localisation: without a key transform the same element is already correct

DROP TABLE IF EXISTS b1n; DROP TABLE IF EXISTS b1no;
CREATE TABLE b1n (k UInt8) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE b1no (k UInt8) ENGINE = Memory;
INSERT INTO b1n VALUES (0);
INSERT INTO b1n VALUES (1);
INSERT INTO b1no VALUES (0), (1);
SELECT 'B1 no key transform',
    (SELECT count() FROM b1n WHERE k IN (SELECT true)) = (SELECT count() FROM b1no WHERE k IN (SELECT true));
-- and the plain-integer twin on the SAME table keeps its atom, so this is not a blanket decline
SELECT 'B1 UInt8 twin prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM b1 WHERE k IN (SELECT toUInt8(1))) WHERE explain ILIKE '%element set%';
SELECT 'B1 UInt8 twin result',
    (SELECT count() FROM b1 WHERE k IN (SELECT toUInt8(1))) = (SELECT count() FROM b1o WHERE k IN (SELECT toUInt8(1)));
-- the scalar equals/notEquals path is a different atom kind and must not change
SELECT 'B1 scalar notEquals unchanged', count() FROM b1 WHERE k != true;

SELECT '--- v17 Z1/Z1b: a fast-path CAST that is injective on the key but collapses the element ---';

DROP TABLE IF EXISTS z1; DROP TABLE IF EXISTS z1o;
CREATE TABLE z1 (k UInt32) ENGINE = MergeTree ORDER BY (k::UInt64) PARTITION BY (k::UInt64);
CREATE TABLE z1o (k UInt32) ENGINE = Memory;
INSERT INTO z1 VALUES (1), (2);
INSERT INTO z1o VALUES (1), (2);
SELECT 'Z1 UInt32 key cast to UInt64',
    (SELECT count() FROM z1 WHERE k NOT IN (SELECT '01')) = (SELECT count() FROM z1o WHERE k NOT IN (SELECT '01'));

DROP TABLE IF EXISTS z1b; DROP TABLE IF EXISTS z1bo;
CREATE TABLE z1b (k UInt64) ENGINE = MergeTree ORDER BY (k::String) PARTITION BY (k::String);
CREATE TABLE z1bo (k UInt64) ENGINE = Memory;
INSERT INTO z1b VALUES (1), (2);
INSERT INTO z1bo VALUES (1), (2);
SELECT 'Z1b UInt64 key cast to String',
    (SELECT count() FROM z1b WHERE k NOT IN (SELECT '01')) = (SELECT count() FROM z1bo WHERE k NOT IN (SELECT '01'));

SELECT '--- v17 Z2: a non-injective key transform still over-prunes the POSITIVE direction ---';

-- `relaxed` only forces can_be_false, never widens can_be_true, so a relaxed atom does not protect
-- `IN`. The two rows must be in separate granules and `length` must SEPARATE the round-trip pair.

DROP TABLE IF EXISTS z2; DROP TABLE IF EXISTS z2o;
CREATE TABLE z2 (s String) ENGINE = MergeTree ORDER BY length(s) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE z2o (s String) ENGINE = Memory;
INSERT INTO z2 VALUES ('1');
INSERT INTO z2 VALUES ('01');
INSERT INTO z2o VALUES ('1'), ('01');
SELECT 'Z2 non-injective key, positive IN',
    (SELECT count() FROM z2 WHERE s IN (SELECT toUInt8(1))) = (SELECT count() FROM z2o WHERE s IN (SELECT toUInt8(1)));
SELECT 'Z2 negative direction control',
    (SELECT count() FROM z2 WHERE s NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM z2o WHERE s NOT IN (SELECT toUInt8(1)));
-- Z3 scope boundary: a transform that COLLAPSES the pair the same way the element cast does is
-- not a carrier in either direction. This is why 03762's moved block is correctness-neutral.

DROP TABLE IF EXISTS z3; DROP TABLE IF EXISTS z3o;
CREATE TABLE z3 (s String) ENGINE = MergeTree ORDER BY (s::UInt64) SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE z3o (s String) ENGINE = Memory;
INSERT INTO z3 VALUES ('1');
INSERT INTO z3 VALUES ('01');
INSERT INTO z3o VALUES ('1'), ('01');
SELECT 'Z3 collapsing transform both directions',
    (SELECT count() FROM z3 WHERE s IN (SELECT toUInt8(1))) = (SELECT count() FROM z3o WHERE s IN (SELECT toUInt8(1))),
    (SELECT count() FROM z3 WHERE s NOT IN (SELECT toUInt8(1))) = (SELECT count() FROM z3o WHERE s NOT IN (SELECT toUInt8(1)));

SELECT '--- v15: the set-transforming DAG carrier, and the fast-path spelling that is not one ---';

DROP TABLE IF EXISTS dg; DROP TABLE IF EXISTS dgo;
CREATE TABLE dg (k UInt64) ENGINE = MergeTree ORDER BY toString(k) PARTITION BY toString(k);
CREATE TABLE dgo (k UInt64) ENGINE = Memory;
INSERT INTO dg VALUES (1), (2);
INSERT INTO dgo VALUES (1), (2);
SELECT 'DAG carrier toString(k)',
    (SELECT count() FROM dg WHERE k NOT IN (SELECT '01')) = (SELECT count() FROM dgo WHERE k NOT IN (SELECT '01'));
-- a bare CAST takes the fast path, which converts to the CAST result type instead, so no collapse
-- happens and the atom must be KEPT
DROP TABLE IF EXISTS dgc;
CREATE TABLE dgc (k UInt64) ENGINE = MergeTree ORDER BY (k::String) PARTITION BY (k::String);
INSERT INTO dgc VALUES (1), (2);
SELECT 'DAG ::String twin prunes', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM dgc WHERE k IN (SELECT 'x')) WHERE explain ILIKE '%element set%';

SELECT '--- v14 has()/composite carriers: the same predicate fixes has() too ---';

DROP TABLE IF EXISTS c_g_has; DROP TABLE IF EXISTS o_g_has;
CREATE TABLE c_g_has (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_g_has (k UInt64) ENGINE = Memory;
INSERT INTO c_g_has VALUES (1), (2);
INSERT INTO o_g_has VALUES (1), (2);
SELECT 'G-has UInt64/Decimal64(1)',
    (SELECT count() FROM c_g_has WHERE NOT has([CAST(1.5, 'Decimal64(1)')], k)) = (SELECT count() FROM o_g_has WHERE NOT has([CAST(1.5, 'Decimal64(1)')], k));

DROP TABLE IF EXISTS c_v_has; DROP TABLE IF EXISTS o_v_has;
CREATE TABLE c_v_has (k Int64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE o_v_has (k Int64) ENGINE = Memory;
INSERT INTO c_v_has VALUES (1), (2);
INSERT INTO o_v_has VALUES (1), (2);
SELECT 'V-has Int64/Decimal(10,2)',
    (SELECT count() FROM c_v_has WHERE NOT has([CAST('1.50', 'Decimal(10,2)')], k)) = (SELECT count() FROM o_v_has WHERE NOT has([CAST('1.50', 'Decimal(10,2)')], k));
