-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: the assertions are about local `KeyCondition` part pruning; parallel replicas
--   reshape the plan into a `Union` with `ReadFromRemoteParallelReplicas` and duplicated `Indexes` blocks.
-- no-replicated-database: the DBReplicated job replaces engines and can change the plan shape.
-- no-random-merge-tree-settings: `Parts: N/M` prints the part counts around each pruning stage, so an
--   extra randomized stage (implicit column statistics) shifts both numbers.

-- Regression test for issue #111340: `transform_null_in = 1`, non-Nullable key column, `IN`/`NOT IN`
-- a subquery whose result is Nullable. Previously threw CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN (349).
--
-- Cases A, B, D, D2, C, C2 -- C16, E, F, H, H2, I, J, G, K cover, in order: the folded-default pair,
-- the numeric analogue and its type-level-granularity sibling, the cross-type superset direction and
-- its counterexample, the two silent-collapse families (text key against a different type / key with a
-- finer Decimal or DateTime64 scale, or a scaled key against a scale-less integer or float source)
-- with their exactness controls (injective numeric narrowing, identical text types, identical
-- composite element types, a finer-scale source, a scale-zero key), the `NOT has` caller, the
-- `LowCardinality(Nullable(T))` source, the multi-column set and its emptiness ordering, the
-- all-NULL empty set, the positive `IN` direction,
-- and the two shapes that must stay unchanged (`Tuple(Nullable, Nullable)` key, duplicate key mapping).

SET transform_null_in = 1;
SET explain_query_plan_default = 'legacy';

SELECT 'String key IN';
DROP TABLE IF EXISTS t_str SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_str (s String) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_str VALUES ('a'), ('b'), ('c');
SELECT s FROM t_str WHERE s IN (SELECT s FROM t_str UNION ALL SELECT NULL) ORDER BY s;

SELECT 'FixedString key IN';
DROP TABLE IF EXISTS t_fs SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_fs (s FixedString(2)) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_fs VALUES ('ab'), ('cd');
SELECT s FROM t_fs WHERE s IN (SELECT s FROM t_fs UNION ALL SELECT NULL) ORDER BY s;

SELECT 'Int64 key IN';
DROP TABLE IF EXISTS t_int SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_int (s Int64) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_int VALUES (1), (2), (3);
SELECT s FROM t_int WHERE s IN (SELECT s FROM t_int UNION ALL SELECT NULL) ORDER BY s;

SELECT 'LowCardinality(String) key IN';
DROP TABLE IF EXISTS t_lc SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_lc (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s;
INSERT INTO t_lc VALUES ('a'), ('b');
SELECT s FROM t_lc WHERE s IN (SELECT s FROM t_lc UNION ALL SELECT NULL) ORDER BY s;

SELECT 'Date key IN';
DROP TABLE IF EXISTS t_date SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_date (d Date) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_date VALUES ('2020-01-01'), ('2020-01-02');
SELECT d FROM t_date WHERE d IN (SELECT d FROM t_date UNION ALL SELECT NULL) ORDER BY d;

SELECT 'String key IN, transform_null_in=0';
SELECT s FROM t_str WHERE s IN (SELECT s FROM t_str UNION ALL SELECT NULL) ORDER BY s SETTINGS transform_null_in = 0;

SELECT 'Non-PK String column IN';
DROP TABLE IF EXISTS t_nopk SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_nopk (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nopk VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT s FROM t_nopk WHERE s IN (SELECT s FROM t_nopk UNION ALL SELECT NULL) ORDER BY s;

-- A NULL element of a Nullable source set can never match a non-Nullable key, so it is dropped from
-- the pruning set entirely. The remaining set is an exact image of the user predicate, so exact
-- `NOT IN` partition / minmax pruning is preserved. Two properties are asserted per case: the set
-- size (proves the NULL row was dropped rather than folded to the key default) and `Parts: 2/3`
-- (proves the atom stayed exact, so minmax could prune the third part).
SELECT 'String key NOT IN, partition pruning';
DROP TABLE IF EXISTS t_np SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_np (s String) ENGINE = MergeTree ORDER BY s PARTITION BY s;
INSERT INTO t_np VALUES ('a'), ('b'), ('');
-- Case A: the dropped NULL folds to '', which is already the value under test. The '' row must
-- still be returned, and pruning must be exact.
SELECT s FROM t_np WHERE s NOT IN (SELECT '' UNION ALL SELECT NULL) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE s NOT IN (SELECT '' UNION ALL SELECT NULL)) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE s NOT IN (SELECT '' UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 2/3%';

-- Case B: the dropped NULL would have folded to '', which is NOT in the set. The '' row must be
-- returned (this is the results half of #111340) and pruning must be exact.
SELECT s FROM t_np WHERE s NOT IN (SELECT 'a' UNION ALL SELECT NULL) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE s NOT IN (SELECT 'a' UNION ALL SELECT NULL)) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE s NOT IN (SELECT 'a' UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 2/3%';

SELECT 'Int64 key NOT IN, partition pruning';
DROP TABLE IF EXISTS t_ip SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_ip (s Int64) ENGINE = MergeTree ORDER BY s PARTITION BY s;
INSERT INTO t_ip VALUES (5), (7), (0);
-- Case D: numeric analogue of case A, the folded default would have been 0. `Int64 -> Int64` is
-- equality-preserving, so the atom stays exact and the third partition is pruned.
SELECT s FROM t_ip WHERE s NOT IN (SELECT CAST(0, 'Nullable(Int64)') UNION ALL SELECT NULL) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_ip WHERE s NOT IN (SELECT CAST(0, 'Nullable(Int64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_ip WHERE s NOT IN (SELECT CAST(0, 'Nullable(Int64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 2/3%';

-- Case D2: a bare integer literal is `Nullable(UInt8)`, and `canBeSafelyCast` conservatively rejects
-- `UInt8 -> Int64` (its unsigned branch only accepts an unsigned target), so the atom is relaxed even
-- though this particular conversion does round-trip. Results stay correct either way; only pruning is
-- given up. This documents the exactness gate's granularity: it is a type-level, not a value-level, test.
SELECT s FROM t_ip WHERE s NOT IN (SELECT 0 UNION ALL SELECT NULL) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_ip WHERE s NOT IN (SELECT 0 UNION ALL SELECT NULL)) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_ip WHERE s NOT IN (SELECT 0 UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

-- Case C: cross-type source. The NULL row is dropped, but `String -> UInt64` is not
-- equality-preserving (`canBeSafelyCast` is false for it): the set is built by casting set values into
-- the key type while runtime `IN` casts the key into the set type, so the set is only a superset image
-- of the predicate. The atom is therefore marked relaxed and nothing is pruned.
SELECT 'Cross-type UInt64 key NOT IN Nullable(String), relaxed';
DROP TABLE IF EXISTS t_ct SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_ct (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO t_ct VALUES (1), (2), (3);
SELECT k FROM t_ct WHERE k NOT IN (SELECT CAST('1', 'Nullable(String)') UNION ALL SELECT NULL) ORDER BY k;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_ct WHERE k NOT IN (SELECT CAST('1', 'Nullable(String)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_ct WHERE k NOT IN (SELECT CAST('1', 'Nullable(String)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

-- Case C2: the counterexample that makes the relaxation load-bearing. `'01'` converts to `1` without
-- overflowing, yet `toUInt64(1) IN (CAST('01', 'Nullable(String)'))` is false, so all three rows must be
-- returned. Treating the set as exact would prune the `k = 1` partition and drop that row.
SELECT k FROM t_ct WHERE k NOT IN (SELECT CAST('01', 'Nullable(String)') UNION ALL SELECT NULL) ORDER BY k;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_ct WHERE k NOT IN (SELECT CAST('01', 'Nullable(String)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_ct WHERE k NOT IN (SELECT CAST('01', 'Nullable(String)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

-- Case C3: the MIRROR of case C2 -- the same conversion in the other direction. Here the set-to-key
-- cast `UInt8 -> String` IS safe, so the atom used to be treated as exact, but runtime membership
-- casts the KEY into the set's type: both `'01'` and `'1'` become `1`, so the set built here (holding
-- only the canonical `'1'`) UNDER-approximates the predicate. `relaxed` cannot repair that, because it
-- only ever widens `can_be_false`, so the atom must decline entirely.
SELECT 'String key, numeric source, declined';
DROP TABLE IF EXISTS t_sk SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_sk (s String) ENGINE = MergeTree ORDER BY s PARTITION BY s;
INSERT INTO t_sk VALUES ('01'), ('1'), ('2');
-- BOTH '01' and '1' must come back; treating the set as exact prunes the '01' partition.
SELECT s FROM t_sk WHERE s IN (SELECT CAST(1, 'Nullable(UInt8)') UNION ALL SELECT NULL) ORDER BY s;
SELECT s FROM t_sk WHERE s NOT IN (SELECT CAST(1, 'Nullable(UInt8)') UNION ALL SELECT NULL) ORDER BY s;
-- Pin that the atom DECLINED rather than merely relaxed: a declined atom prints no set condition at
-- all, whereas a relaxed one still prints one with `Parts: 3/3`.
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_sk WHERE s IN (SELECT CAST(1, 'Nullable(UInt8)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_sk WHERE s IN (SELECT CAST(1, 'Nullable(UInt8)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

-- Case C4: the second silent-collapse family, a loss of Decimal SCALE. `accurateCast` maps both
-- 1.2345 and 1.2300 onto 1.23 without rejecting either, so a `Decimal(10, 4)` key against a
-- `Nullable(Decimal(10, 2))` set element collapses two distinct keys onto one set value. Note the
-- direction: the KEY carries the finer scale, because it is the key that is cast at runtime.
SELECT 'Decimal key, coarser-scale source, declined';
DROP TABLE IF EXISTS t_dk SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_dk (d Decimal(10, 4)) ENGINE = MergeTree ORDER BY d PARTITION BY d;
INSERT INTO t_dk VALUES (1.2345), (1.2300), (2.0000);
-- Both 1.23 and 1.2345 match the set value 1.23 at runtime, so both must come back, and `NOT IN`
-- must keep only the 2.0 row.
SELECT d FROM t_dk WHERE d IN (SELECT CAST(1.23, 'Nullable(Decimal(10, 2))') UNION ALL SELECT NULL) ORDER BY d;
SELECT d FROM t_dk WHERE d NOT IN (SELECT CAST(1.23, 'Nullable(Decimal(10, 2))') UNION ALL SELECT NULL) ORDER BY d;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dk WHERE d NOT IN (SELECT CAST(1.23, 'Nullable(Decimal(10, 2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dk WHERE d NOT IN (SELECT CAST(1.23, 'Nullable(Decimal(10, 2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

-- Case C7: the same scale-loss family through `DateTime64`, whose scale `tryGetDecimalScale` also
-- reports. It is the case that pins the decline as UNCONDITIONAL on the forward set-to-key cast:
-- `canBeSafelyCast` rejects `DateTime64 -> DateTime64` (its branch only accepts a `String` target),
-- so gating the decline on that cast would let this atom fall through and be marked merely relaxed,
-- which cannot protect the positive `IN` direction.
SELECT 'DateTime64 key, coarser-scale source, declined';
DROP TABLE IF EXISTS t_dt SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_dt (d DateTime64(4)) ENGINE = MergeTree ORDER BY d PARTITION BY d;
INSERT INTO t_dt VALUES ('2020-01-01 00:00:01.2345'), ('2020-01-01 00:00:01.2300'), ('2020-01-01 00:00:02.0000');
SELECT d FROM t_dt WHERE d IN (SELECT CAST('2020-01-01 00:00:01.23', 'Nullable(DateTime64(2))') UNION ALL SELECT NULL) ORDER BY d;
SELECT d FROM t_dt WHERE d NOT IN (SELECT CAST('2020-01-01 00:00:01.23', 'Nullable(DateTime64(2))') UNION ALL SELECT NULL) ORDER BY d;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dt WHERE d IN (SELECT CAST('2020-01-01 00:00:01.23', 'Nullable(DateTime64(2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dt WHERE d IN (SELECT CAST('2020-01-01 00:00:01.23', 'Nullable(DateTime64(2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

-- Cases C6, C8, C9: the text half of the silent-collapse family is keyed on the text types being
-- DIFFERENT, not on the set element being non-text. `String` and `FixedString(N)` do not share a
-- value representation: casting the KEY into `FixedString(N)` right-pads with '\0', while building
-- the set casts `FixedString(N)` into `String` trimming trailing zeros, so `'a'` and `'a\0'` collapse
-- onto one set value. Each case must decline; C6 additionally used to throw error 349 before this PR,
-- so leaving it exact would have converted a loud error into a silently wrong result.
SELECT 'String key, FixedString source, declined';
DROP TABLE IF EXISTS t_sf SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_sf (s String) ENGINE = MergeTree ORDER BY s PARTITION BY s;
-- `VALUES` cannot carry a raw NUL, so the padded twin goes in through `INSERT ... SELECT`.
INSERT INTO t_sf SELECT 'a';
INSERT INTO t_sf SELECT concat('a', char(0));
INSERT INTO t_sf SELECT 'b';
SELECT hex(s) FROM t_sf WHERE s IN (SELECT CAST('a', 'Nullable(FixedString(2))') UNION ALL SELECT NULL) ORDER BY s;
SELECT hex(s) FROM t_sf WHERE s NOT IN (SELECT CAST('a', 'Nullable(FixedString(2))') UNION ALL SELECT NULL) ORDER BY s;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_sf WHERE s IN (SELECT CAST('a', 'Nullable(FixedString(2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_sf WHERE s IN (SELECT CAST('a', 'Nullable(FixedString(2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

SELECT 'FixedString key, narrower FixedString source, declined';
DROP TABLE IF EXISTS t_f2 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_f2 (s FixedString(2)) ENGINE = MergeTree ORDER BY s PARTITION BY s;
INSERT INTO t_f2 VALUES ('a'), ('ab'), ('b');
SELECT hex(s) FROM t_f2 WHERE s IN (SELECT CAST('a', 'Nullable(FixedString(1))') UNION ALL SELECT NULL) ORDER BY s;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_f2 WHERE s IN (SELECT CAST('a', 'Nullable(FixedString(1))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_f2 WHERE s IN (SELECT CAST('a', 'Nullable(FixedString(1))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

SELECT 'FixedString key, wider FixedString source, declined';
DROP TABLE IF EXISTS t_f1 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_f1 (s FixedString(1)) ENGINE = MergeTree ORDER BY s PARTITION BY s;
INSERT INTO t_f1 VALUES ('a'), ('b'), ('c');
SELECT hex(s) FROM t_f1 WHERE s IN (SELECT CAST('a', 'Nullable(FixedString(2))') UNION ALL SELECT NULL) ORDER BY s;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_f1 WHERE s IN (SELECT CAST('a', 'Nullable(FixedString(2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_f1 WHERE s IN (SELECT CAST('a', 'Nullable(FixedString(2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

-- Case C10 (CONTROL, must NOT change): IDENTICAL text types share a representation, so no padding or
-- trimming happens and the atom stays exact. Together with cases A/B/F/J (`String`/`Nullable(String)`)
-- this is what stops the text arm from being "fixed" by declining every text pair.
SELECT 'Same-width FixedString source stays exact';
SELECT hex(s) FROM t_f2 WHERE s IN (SELECT CAST('ab', 'Nullable(FixedString(2))') UNION ALL SELECT NULL) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_f2 WHERE s IN (SELECT CAST('ab', 'Nullable(FixedString(2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%in 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_f2 WHERE s IN (SELECT CAST('ab', 'Nullable(FixedString(2))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 1/3%';

-- Case C11: the collapse test must recurse into composites, the way `canBeSafelyCast` -- the
-- predicate it complements -- already does. A `Tuple(String, UInt64)` key against a
-- `Tuple(FixedString(2), UInt64)` set element collapses on its FIRST element exactly as case C6 does,
-- while a top-level-only test sees two tuples and reports no collapse.
-- `enable_nullable_tuple_type` is set per statement so it does not leak into the rest of the file.
SELECT 'Tuple key, collapsing element type, declined';
DROP TABLE IF EXISTS t_tc SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_tc (k Tuple(String, UInt64)) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO t_tc SELECT tuple('a', 1);
INSERT INTO t_tc SELECT tuple(concat('a', char(0)), 1);
INSERT INTO t_tc SELECT tuple('b', 2);
SELECT hex(k.1) FROM t_tc WHERE k IN (SELECT CAST(tuple('a', 1), 'Nullable(Tuple(FixedString(2), UInt64))') UNION ALL SELECT NULL) ORDER BY k SETTINGS enable_nullable_tuple_type = 1;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_tc WHERE k IN (SELECT CAST(tuple('a', 1), 'Nullable(Tuple(FixedString(2), UInt64))') UNION ALL SELECT NULL) SETTINGS enable_nullable_tuple_type = 1) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_tc WHERE k IN (SELECT CAST(tuple('a', 1), 'Nullable(Tuple(FixedString(2), UInt64))') UNION ALL SELECT NULL) SETTINGS enable_nullable_tuple_type = 1) WHERE explain ILIKE '%Parts: 3/3%';
-- CONTROL: identical element types recurse to no collapse, so the atom stays exact and prunes. This is
-- what stops the recursion from being "fixed" by declining every composite pair.
SELECT hex(k.1) FROM t_tc WHERE k IN (SELECT CAST(tuple('b', 2), 'Nullable(Tuple(String, UInt64))') UNION ALL SELECT NULL) ORDER BY k SETTINGS enable_nullable_tuple_type = 1;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_tc WHERE k IN (SELECT CAST(tuple('b', 2), 'Nullable(Tuple(String, UInt64))') UNION ALL SELECT NULL) SETTINGS enable_nullable_tuple_type = 1) WHERE explain ILIKE '%in 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_tc WHERE k IN (SELECT CAST(tuple('b', 2), 'Nullable(Tuple(String, UInt64))') UNION ALL SELECT NULL) SETTINGS enable_nullable_tuple_type = 1) WHERE explain ILIKE '%Parts: 1/3%';

-- Cases C12 -- C16: the scale-loss family also fires when the set element reports NO scale at all,
-- and the two branches of `DecimalUtils::convertToImpl` that reach it lose different things, so each
-- needs its own case. An integer target takes the whole part (integer division by the scale
-- multiplier), while a float target divides in `Float64` with no strictness check whatsoever. Both
-- reject only on range overflow. These are wrong results on master too, where the atom is treated as
-- fully exact; before this case group the fix left them merely relaxed, which does not protect the
-- positive `IN` direction.
SELECT 'Decimal key, integer source, declined';
DROP TABLE IF EXISTS t_di SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_di (d Decimal(10, 4)) ENGINE = MergeTree ORDER BY d PARTITION BY d;
INSERT INTO t_di VALUES (1.0000), (1.2345), (2.0000);
-- Both 1.0 and 1.2345 have whole part 1, so both match the set value 1 at runtime.
SELECT d FROM t_di WHERE d IN (SELECT CAST(1, 'Nullable(UInt64)') UNION ALL SELECT NULL) ORDER BY d;
SELECT d FROM t_di WHERE d NOT IN (SELECT CAST(1, 'Nullable(UInt64)') UNION ALL SELECT NULL) ORDER BY d;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_di WHERE d IN (SELECT CAST(1, 'Nullable(UInt64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_di WHERE d IN (SELECT CAST(1, 'Nullable(UInt64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';
-- The signed sibling reaches the same branch.
SELECT d FROM t_di WHERE d IN (SELECT CAST(1, 'Nullable(Int64)') UNION ALL SELECT NULL) ORDER BY d;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_di WHERE d IN (SELECT CAST(1, 'Nullable(Int64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';

-- Case C13: the float branch, with a key scale of ZERO. This is the case that stops the arm from
-- being gated on `key_scale > 0`: no digits are dropped, the loss is float mantissa precision, and
-- 16777217 collapses onto 16777216 in `Float32`.
SELECT 'Decimal key, float source, declined';
DROP TABLE IF EXISTS t_df SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_df (d Decimal(20, 0)) ENGINE = MergeTree ORDER BY d PARTITION BY d;
INSERT INTO t_df VALUES (16777216), (16777217), (99);
SELECT d FROM t_df WHERE d IN (SELECT CAST(16777216, 'Nullable(Float32)') UNION ALL SELECT NULL) ORDER BY d;
SELECT d FROM t_df WHERE d NOT IN (SELECT CAST(16777216, 'Nullable(Float32)') UNION ALL SELECT NULL) ORDER BY d;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_df WHERE d IN (SELECT CAST(16777216, 'Nullable(Float32)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_df WHERE d IN (SELECT CAST(16777216, 'Nullable(Float32)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';
-- `Float64` collapses at its own mantissa boundary, so the arm must not be keyed on `Float32`.
SELECT 'Decimal key, Float64 source, declined';
DROP TABLE IF EXISTS t_dw SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_dw (d Decimal(30, 0)) ENGINE = MergeTree ORDER BY d PARTITION BY d;
INSERT INTO t_dw VALUES (9007199254740992), (9007199254740993), (99);
SELECT d FROM t_dw WHERE d IN (SELECT CAST(9007199254740992, 'Nullable(Float64)') UNION ALL SELECT NULL) ORDER BY d;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_dw WHERE d IN (SELECT CAST(9007199254740992, 'Nullable(Float64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';

-- Case C15: `Time64` shares the scale machinery, and unlike `DateTime64` it gets no help from
-- `Set::execute`, whose sub-second precision guard is keyed on `TypeIndex::DateTime64` alone. So the
-- integer branch really does collapse here, and the arm must reach every scaled key family rather
-- than `Decimal` only.
SELECT 'Time64 key, integer source, declined';
DROP TABLE IF EXISTS t_t64 SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_t64 (d Time64(4)) ENGINE = MergeTree ORDER BY d PARTITION BY d;
INSERT INTO t_t64 VALUES ('00:00:01.0000'), ('00:00:01.5000'), ('00:00:02.0000');
SELECT d FROM t_t64 WHERE d IN (SELECT CAST(1, 'Nullable(UInt64)') UNION ALL SELECT NULL) ORDER BY d;
SELECT d FROM t_t64 WHERE d NOT IN (SELECT CAST(1, 'Nullable(UInt64)') UNION ALL SELECT NULL) ORDER BY d;
SELECT count() = 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_t64 WHERE d IN (SELECT CAST(1, 'Nullable(UInt64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_t64 WHERE d IN (SELECT CAST(1, 'Nullable(UInt64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 3/3%';

-- Case C14 (CONTROL, must NOT change): a set scale FINER than the key's loses nothing, so the atom
-- stays exact and keeps pruning. This is what stops the scaled-key arm from being "fixed" by
-- declining every scaled key.
SELECT 'Finer-scale Decimal source stays exact';
SELECT d FROM t_di WHERE d IN (SELECT CAST(1, 'Nullable(Decimal(10, 6))') UNION ALL SELECT NULL) ORDER BY d;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_di WHERE d IN (SELECT CAST(1, 'Nullable(Decimal(10, 6))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%in 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_di WHERE d IN (SELECT CAST(1, 'Nullable(Decimal(10, 6))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 1/3%';

-- Case C16 (CONTROL, must NOT change): a key scale of ZERO makes the integer branch's division the
-- identity, so nothing collapses and the atom must stay exact. This is what stops the integer arm
-- from being keyed merely on "the key is a scaled type".
SELECT 'Scale-zero Decimal key, integer source, stays exact';
SELECT d FROM t_df WHERE d IN (SELECT CAST(16777216, 'Nullable(UInt64)') UNION ALL SELECT NULL) ORDER BY d;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_df WHERE d IN (SELECT CAST(16777216, 'Nullable(UInt64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%in 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT d FROM t_df WHERE d IN (SELECT CAST(16777216, 'Nullable(UInt64)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 1/3%';

-- Case C5 (CONTROL, must NOT change): an injective same-family numeric narrowing stays exact and
-- keeps pruning. `castColumnAccurate` REJECTS an out-of-range `UInt64 -> UInt32` value rather than
-- truncating it, so no two keys can share a set value and the atom is genuinely exact. This control
-- is what stops the exactness gate from being "fixed" by declining every cross-type conversion.
SELECT 'Injective numeric narrowing stays exact';
DROP TABLE IF EXISTS t_nn SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_nn (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
INSERT INTO t_nn VALUES (10), (50000), (90000);
SELECT k FROM t_nn WHERE k IN (SELECT CAST(50000, 'Nullable(UInt32)') UNION ALL SELECT NULL) ORDER BY k;
SELECT k FROM t_nn WHERE k NOT IN (SELECT CAST(50000, 'Nullable(UInt32)') UNION ALL SELECT NULL) ORDER BY k;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_nn WHERE k IN (SELECT CAST(50000, 'Nullable(UInt32)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%in 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_nn WHERE k IN (SELECT CAST(50000, 'Nullable(UInt32)') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 1/3%';

-- Case E: the `NOT has` sibling caller shares the same helper and must get the same treatment.
-- `optimize_rewrite_has_to_in = 0` keeps the query on the `has` path.
SELECT 'String key NOT has, partition pruning';
SELECT s FROM t_np WHERE NOT has([CAST('', 'Nullable(String)'), NULL], s) ORDER BY s SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE NOT has([CAST('', 'Nullable(String)'), NULL], s) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain ILIKE '%in 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE NOT has([CAST('', 'Nullable(String)'), NULL], s) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain ILIKE '%Parts: 2/3%';

-- Case F: `LowCardinality(Nullable(T))` source reaches the same block through the LowCardinality
-- unwrap, so the wrapper must behave like the bare Nullable source.
SELECT 'LowCardinality(Nullable(String)) source NOT IN, partition pruning';
SELECT s FROM t_np WHERE s NOT IN (SELECT CAST('', 'LowCardinality(Nullable(String))') UNION ALL SELECT NULL) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE s NOT IN (SELECT CAST('', 'LowCardinality(Nullable(String))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE s NOT IN (SELECT CAST('', 'LowCardinality(Nullable(String))') UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 2/3%';

-- Case H: multi-column set. A NULL in one component drops the whole row, so every set column stays
-- aligned; the surviving single tuple is exact and prunes.
SELECT 'Multi-column set NOT IN, one component NULL';
DROP TABLE IF EXISTS t_mc SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_mc (a String, b UInt64) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
INSERT INTO t_mc VALUES ('x', 1), ('y', 2), ('', 0);
SELECT a, b FROM t_mc WHERE (a, b) NOT IN (SELECT tuple(CAST('x', 'Nullable(String)'), CAST(1, 'Nullable(UInt64)')) UNION ALL SELECT tuple(CAST(NULL, 'Nullable(String)'), CAST(2, 'Nullable(UInt64)'))) ORDER BY a;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT a, b FROM t_mc WHERE (a, b) NOT IN (SELECT tuple(CAST('x', 'Nullable(String)'), CAST(1, 'Nullable(UInt64)')) UNION ALL SELECT tuple(CAST(NULL, 'Nullable(String)'), CAST(2, 'Nullable(UInt64)')))) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT a, b FROM t_mc WHERE (a, b) NOT IN (SELECT tuple(CAST('x', 'Nullable(String)'), CAST(1, 'Nullable(UInt64)')) UNION ALL SELECT tuple(CAST(NULL, 'Nullable(String)'), CAST(2, 'Nullable(UInt64)')))) WHERE explain ILIKE '%Parts: 2/3%';

-- Case H2: the emptiness half of the exactness decision must be taken AFTER the shared filter has been
-- applied to EVERY set column, because one shared filter is built across all of them. Here the FIRST
-- component's `String -> UInt64` conversion is not equality-preserving, so it marks the set approximate
-- while its own row is still surviving; the SECOND component's `'bad' -> UInt64` cast then fails and
-- drops that last row, leaving the pruning set EMPTY. An empty set is exact by construction (`NOT IN ()`
-- is universally true), so the atom must not stay relaxed. Deciding this per column, from the filter as
-- it stands mid-loop, cannot see the later column and leaves the empty set marked approximate.
-- Both key columns are numeric on purpose: the approximate component has to be processed BEFORE the
-- emptying one, which is what makes the ordering observable.
-- `Parts: 3/3` alone cannot pin this (it is also the relaxed output), so the load-bearing assertion is
-- the trivial-count optimization, which `PartitionPruner`'s strict mode switches off for a relaxed
-- condition: it is available for an exact empty set and unavailable for a relaxed one. That path is
-- reached through `totalRowsByPartitionPredicate`, which only the old analyzer uses for this shape, and
-- `optimize_trivial_count_query` is randomized, so both are pinned per statement.
SELECT 'Multi-column set, final set empty after the shared filter';
DROP TABLE IF EXISTS t_me SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_me (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY (a, b) PARTITION BY (a, b);
INSERT INTO t_me VALUES (1, 2), (3, 4), (0, 0);
SELECT a, b FROM t_me WHERE (a, b) NOT IN (SELECT tuple(CAST('1', 'Nullable(String)'), CAST('bad', 'Nullable(String)'))) ORDER BY a;
SELECT count() = 0 FROM t_me WHERE (a, b) IN (SELECT tuple(CAST('1', 'Nullable(String)'), CAST('bad', 'Nullable(String)')));
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT a, b FROM t_me WHERE (a, b) NOT IN (SELECT tuple(CAST('1', 'Nullable(String)'), CAST('bad', 'Nullable(String)')))) WHERE explain ILIKE '%notIn 0-element set%';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_me WHERE (a, b) NOT IN (SELECT tuple(CAST('1', 'Nullable(String)'), CAST('bad', 'Nullable(String)')))) WHERE explain ILIKE '%Optimized trivial count%' SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;
-- Control: a relaxed NON-empty set of the same cross-type shape must NOT get the optimization, so the
-- assertion above really distinguishes exact-empty from relaxed rather than always being true.
SELECT count() = 0 FROM (EXPLAIN SELECT count() FROM t_me WHERE (a, b) NOT IN (SELECT tuple(CAST('1', 'Nullable(String)'), CAST('2', 'Nullable(String)')))) WHERE explain ILIKE '%Optimized trivial count%' SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;

-- Case I: every source row is NULL, so the pruning set becomes empty. `NOT IN` an empty set is
-- always true, so no part may be pruned and all three rows are returned.
SELECT 'All-NULL source set NOT IN';
SELECT s FROM t_np WHERE s NOT IN (SELECT CAST(NULL, 'Nullable(String)') UNION ALL SELECT CAST(NULL, 'Nullable(String)')) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE s NOT IN (SELECT CAST(NULL, 'Nullable(String)') UNION ALL SELECT CAST(NULL, 'Nullable(String)'))) WHERE explain ILIKE '%notIn 0-element set%';

-- Case J: the positive `IN` direction is affected too. The set shrinks by the NULL row while the
-- results stay identical, and the exact set still prunes to the single matching partition.
SELECT 'String key IN, partition pruning';
SELECT s FROM t_np WHERE s IN (SELECT '' UNION ALL SELECT NULL) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE s IN (SELECT '' UNION ALL SELECT NULL)) WHERE explain ILIKE '%in 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_np WHERE s IN (SELECT '' UNION ALL SELECT NULL)) WHERE explain ILIKE '%Parts: 1/3%';

-- Case G: a `Tuple(Nullable(T), Nullable(T))` key does reach this block, but the set element type
-- is outer-NON-Nullable so no source-NULL row is ever produced and nothing changes for it. An
-- outer-NULL tuple element matches nothing; a `(NULL, NULL)` tuple element matches `(NULL, NULL)`.
SELECT 'Tuple(Nullable, Nullable) key unchanged';
DROP TABLE IF EXISTS t_tk SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_tk (k Tuple(Nullable(UInt32), Nullable(UInt32))) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
INSERT INTO t_tk VALUES ((1, 2)), ((NULL, NULL)), ((3, NULL));
SELECT k FROM t_tk WHERE k NOT IN (SELECT tuple(CAST(NULL, 'Nullable(UInt32)'), CAST(10, 'Nullable(UInt32)'))) ORDER BY k;
SELECT k FROM t_tk WHERE k IN (SELECT tuple(CAST(NULL, 'Nullable(UInt32)'), CAST(NULL, 'Nullable(UInt32)'))) ORDER BY k;
-- "Unchanged" must be pinned in the plan too, not only in the values: this shape is the STOP condition
-- for the outer-versus-nested argument the changed block relies on.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_tk WHERE k NOT IN (SELECT tuple(CAST(NULL, 'Nullable(UInt32)'), CAST(10, 'Nullable(UInt32)')))) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT k FROM t_tk WHERE k IN (SELECT tuple(CAST(NULL, 'Nullable(UInt32)'), CAST(NULL, 'Nullable(UInt32)')))) WHERE explain ILIKE '%in 1-element set%';

-- Case K: the pre-existing relaxation for a non 1:1 key mapping must survive. `tuple(i, i)` maps
-- both set elements onto one key column, so the atom is still relaxed and nothing is pruned.
SELECT 'Duplicate key mapping stays relaxed';
DROP TABLE IF EXISTS t_k SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_k (i UInt64) ENGINE = MergeTree ORDER BY i PARTITION BY i;
INSERT INTO t_k VALUES (1), (2), (3);
SELECT i FROM t_k WHERE tuple(i, i) NOT IN (tuple(1, 2)) ORDER BY i;
-- `Parts: 3/3` alone is also the output when set-index construction declines entirely, so pin that the
-- set condition is PRESENT with the deduplicated single-column size.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT i FROM t_k WHERE tuple(i, i) NOT IN (tuple(1, 2))) WHERE explain ILIKE '%notIn 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT i FROM t_k WHERE tuple(i, i) NOT IN (tuple(1, 2))) WHERE explain ILIKE '%Parts: 3/3%';

-- A Nullable key must keep working and keep using the set index; NULL on the left matches NULL
-- in the set under transform_null_in=1. This shape declines before the changed block.
SELECT 'Nullable(String) key IN';
DROP TABLE IF EXISTS t_nk SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_nk (s Nullable(String)) ENGINE = MergeTree ORDER BY s SETTINGS allow_nullable_key = 1;
INSERT INTO t_nk VALUES ('a'), ('b'), ('c'), (NULL);
SELECT s FROM t_nk WHERE s IN (SELECT s FROM t_nk) ORDER BY s;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_nk WHERE s IN (SELECT s FROM t_nk)) WHERE explain ILIKE '%in 4-element set%';

-- A `LowCardinality(Nullable(T))` source against a `Nullable(T)` key only reaches `canBeSafelyCast`'s
-- LowCardinality branch through the `has` caller, whose array element type keeps its `LowCardinality`
-- wrapper. Recursing against the LowCardinality-stripped TARGET (rather than the fully unwrapped one)
-- is what preserves the target's `Nullable`, so the cast is judged safe and a set condition is built.
-- Without that, the atom degrades to `Condition: true` and no partition is pruned at all.
SELECT 'LowCardinality(Nullable(String)) array, Nullable(String) key has';
DROP TABLE IF EXISTS t_nkp SETTINGS ignore_drop_queries_probability = 0;
CREATE TABLE t_nkp (s Nullable(String)) ENGINE = MergeTree ORDER BY s PARTITION BY s SETTINGS allow_nullable_key = 1;
INSERT INTO t_nkp VALUES ('a'), ('b'), (NULL);
SELECT s FROM t_nkp WHERE has([CAST('a', 'LowCardinality(Nullable(String))')], s) ORDER BY s SETTINGS optimize_rewrite_has_to_in = 0;
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_nkp WHERE has([CAST('a', 'LowCardinality(Nullable(String))')], s) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain ILIKE '%in 1-element set%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT s FROM t_nkp WHERE has([CAST('a', 'LowCardinality(Nullable(String))')], s) SETTINGS optimize_rewrite_has_to_in = 0) WHERE explain ILIKE '%Parts: 1/3%';

-- IN error semantics must be preserved: a column-count mismatch still throws.
SELECT 'Column count mismatch still rejected';
SELECT 1 WHERE 1 IN (SELECT 1, 2); -- { serverError NUMBER_OF_COLUMNS_DOESNT_MATCH }

DROP TABLE t_str;
DROP TABLE t_fs;
DROP TABLE t_int;
DROP TABLE t_lc;
DROP TABLE t_date;
DROP TABLE t_nopk;
DROP TABLE t_np;
DROP TABLE t_ip;
DROP TABLE t_ct;
DROP TABLE t_mc;
DROP TABLE t_tk;
DROP TABLE t_k;
DROP TABLE t_nk;
DROP TABLE t_nkp;
DROP TABLE t_me;
DROP TABLE t_sk;
DROP TABLE t_dk;
DROP TABLE t_dt;
DROP TABLE t_sf;
DROP TABLE t_f2;
DROP TABLE t_f1;
DROP TABLE t_tc;
DROP TABLE t_nn;
DROP TABLE t_di;
DROP TABLE t_df;
DROP TABLE t_dw;
DROP TABLE t_t64;
