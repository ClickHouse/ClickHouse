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

SELECT '--- attribute axis: parameters that `equals` treats as interchangeable stay exact ---';

-- `IDataType::equals` ignores the time zone of `DateTime`/`DateTime64` and the precision of
-- `Decimal`, while `getName` prints all three. Deciding exactness by name would decline these
-- pairs and silently lose pruning for the very common shape of a key that declares a time zone
-- against a set element that does not. Each pair below must keep its atom, and the neighbouring
-- pair that differs in a parameter `equals` DOES compare must still decline.

DROP TABLE IF EXISTS at_tu; DROP TABLE IF EXISTS ao_tu;
CREATE TABLE at_tu (t Tuple(UInt8, UInt8)) ENGINE = MergeTree ORDER BY t SETTINGS index_granularity = 1;
CREATE TABLE ao_tu (t Tuple(UInt8, UInt8)) ENGINE = Memory;
INSERT INTO at_tu VALUES ((0, 1)), ((1, 1)), ((7, 1));
INSERT INTO ao_tu VALUES ((0, 1)), ((1, 1)), ((7, 1));
SELECT 'attr Tuple(UInt8,UInt8)/Tuple(Bool,UInt8) declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM at_tu WHERE t IN (SELECT tuple(CAST(1, 'Bool'), toUInt8(1)))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'attr Tuple(UInt8,UInt8)/Tuple(Bool,UInt8)',
    (SELECT count() FROM at_tu WHERE t IN (SELECT tuple(CAST(1, 'Bool'), toUInt8(1)))) = (SELECT count() FROM ao_tu WHERE t IN (SELECT tuple(CAST(1, 'Bool'), toUInt8(1))));
DROP TABLE at_tu; DROP TABLE ao_tu;

SELECT '--- an actual NULL in a nullable set element (the cross-type cast rewrites it to the nested default) ---';

-- A source NULL surviving into the prepared set as the nested default lives in the `Nullable`-source
-- branch, which this change does not touch, so the `transform_null_in = 1` shapes are not asserted
-- here. What stays is the pair of controls proving that branch is left alone.

DROP TABLE IF EXISTS nn_t; DROP TABLE IF EXISTS nn_o;
CREATE TABLE nn_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE nn_o (k UInt64) ENGINE = Memory;
INSERT INTO nn_t VALUES (0), (1), (2);
INSERT INTO nn_o VALUES (0), (1), (2);

-- Control, NOT a carrier: at the default `transform_null_in = 0` the set itself strips nullability and
-- drops the NULL row (`Set::setHeader`), so the element type reaching the index is a plain `UInt8`, the
-- set is empty and the atom is legitimately exact. It must keep saying `0-element set`.
SELECT 'null-elem NOT IN stays exact', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM nn_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)'))) WHERE explain ILIKE '%0-element set%';
SELECT 'null-elem NOT IN',
    (SELECT count() FROM nn_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)'))) = (SELECT count() FROM nn_o WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')));
DROP TABLE nn_t; DROP TABLE nn_o;

-- Keep-pruning control: the identity arm must be untouched, so a `Nullable(UInt8)` key against a
-- `Nullable(UInt8)` element still prunes even though the element may hold NULL.

DROP TABLE IF EXISTS nk_t; DROP TABLE IF EXISTS nk_o;
CREATE TABLE nk_t (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE nk_o (k Nullable(UInt8)) ENGINE = Memory;
INSERT INTO nk_t VALUES (0), (1), (2);
INSERT INTO nk_o VALUES (0), (1), (2);
SELECT 'null-elem identity keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM nk_t WHERE k IN (SELECT CAST(1, 'Nullable(UInt8)'))) WHERE explain ILIKE '%in 1-element set%';
SELECT 'null-elem identity',
    (SELECT count() FROM nk_t WHERE k IN (SELECT CAST(1, 'Nullable(UInt8)'))) = (SELECT count() FROM nk_o WHERE k IN (SELECT CAST(1, 'Nullable(UInt8)'))),
    (SELECT count() FROM nk_t WHERE k NOT IN (SELECT CAST(1, 'Nullable(UInt8)'))) = (SELECT count() FROM nk_o WHERE k NOT IN (SELECT CAST(1, 'Nullable(UInt8)')));
DROP TABLE nk_t; DROP TABLE nk_o;

SELECT '--- the lossy conversion path, not the element type, is what forfeits exactness ---';

-- What forfeits exactness is the CONVERSION, not the element type: a `Nullable` element that can be
-- cast with plain `castColumn` (`canBeSafelyCast`) keeps the prepared set a faithful image and must
-- KEEP pruning, while a cross-type conversion that does not preserve equality must not be claimed
-- exact whatever the element type is. The oracle columns are controls: both engines must return the
-- same rows whether or not an atom is built.

DROP TABLE IF EXISTS lp_t; DROP TABLE IF EXISTS lp_o;
CREATE TABLE lp_t (k UInt8) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE lp_o (k UInt8) ENGINE = Memory;
INSERT INTO lp_t VALUES (0), (1), (2);
INSERT INTO lp_o VALUES (0), (1), (2);

-- A literal array holding both a value and a NULL has element type `Array(Nullable(UInt8))`, which is
-- the minimal form of the family `03733`'s `has([10, 50000, 90000, NULL, NULL], toUInt64(id + 2))`
-- block instantiates. `has` must stay correct as well as `NOT has`.
SELECT 'lossy mixed array NOT has declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM lp_t WHERE NOT has([toUInt8(1), NULL], k)) WHERE explain ILIKE '%element set%';
SELECT 'lossy mixed array NOT has',
    (SELECT count() FROM lp_t WHERE NOT has([toUInt8(1), NULL], k)) = (SELECT count() FROM lp_o WHERE NOT has([toUInt8(1), NULL], k)),
    (SELECT count() FROM lp_t WHERE has([toUInt8(1), NULL], k)) = (SELECT count() FROM lp_o WHERE has([toUInt8(1), NULL], k));
DROP TABLE lp_t; DROP TABLE lp_o;

-- The cross-type mixed array: element type `Array(Nullable(UInt32))` against a `UInt64` key.

DROP TABLE IF EXISTS lw_t; DROP TABLE IF EXISTS lw_o;
CREATE TABLE lw_t (k UInt64) ENGINE = MergeTree ORDER BY k PARTITION BY k;
CREATE TABLE lw_o (k UInt64) ENGINE = Memory;
INSERT INTO lw_t VALUES (0), (1), (2);
INSERT INTO lw_o VALUES (0), (1), (2);
SELECT 'lossy cross-type array NOT has declines', count() = 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM lw_t WHERE NOT has([toUInt32(1), NULL], k)) WHERE explain ILIKE '%element set%';
SELECT 'lossy cross-type array NOT has',
    (SELECT count() FROM lw_t WHERE NOT has([toUInt32(1), NULL], k)) = (SELECT count() FROM lw_o WHERE NOT has([toUInt32(1), NULL], k)),
    (SELECT count() FROM lw_t WHERE has([toUInt32(1), NULL], k)) = (SELECT count() FROM lw_o WHERE has([toUInt32(1), NULL], k));
DROP TABLE lw_t; DROP TABLE lw_o;

-- Keep-pruning side of the same boundary. A NULLABLE key takes the `canBeSafelyCast` exit, so every
-- shape here must still say `element set`; a gate keyed on the element type would decline all of them
-- and silently cost pruning on sound queries.

DROP TABLE IF EXISTS sp8_t; DROP TABLE IF EXISTS sp8_o;
CREATE TABLE sp8_t (k Nullable(UInt8)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE sp8_o (k Nullable(UInt8)) ENGINE = Memory;
INSERT INTO sp8_t VALUES (0), (1), (2), (NULL);
INSERT INTO sp8_o VALUES (0), (1), (2), (NULL);

-- Strengthens the identity control above, which only ever passed a non-NULL value through the wrapper.
SELECT 'safe identity actual NULL keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sp8_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%Granules: 3/4%';
SELECT 'safe identity actual NULL IN keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sp8_t WHERE k IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%element set%';
SELECT 'safe identity actual NULL',
    (SELECT count() FROM sp8_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) = (SELECT count() FROM sp8_o WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1),
    (SELECT count() FROM sp8_t WHERE k IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) = (SELECT count() FROM sp8_o WHERE k IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1);
DROP TABLE sp8_t; DROP TABLE sp8_o;

-- The same, cross-type: a `Nullable(UInt64)` key against a `Nullable(UInt8)` element. `canBeSafelyCast`
-- holds because the target accepts NULL, so the source NULL is preserved and pruning is sound.

DROP TABLE IF EXISTS sp64_t; DROP TABLE IF EXISTS sp64_o;
CREATE TABLE sp64_t (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE sp64_o (k Nullable(UInt64)) ENGINE = Memory;
INSERT INTO sp64_t VALUES (0), (1), (2), (NULL);
INSERT INTO sp64_o VALUES (0), (1), (2), (NULL);
SELECT 'safe cross-type NOT IN keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sp64_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%Granules: 3/4%';
SELECT 'safe cross-type NOT IN',
    (SELECT count() FROM sp64_t WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1) = (SELECT count() FROM sp64_o WHERE k NOT IN (SELECT CAST(NULL, 'Nullable(UInt8)')) SETTINGS transform_null_in = 1);
SELECT 'safe cross-type array NOT has keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sp64_t WHERE NOT has([toUInt8(1), NULL], k)) WHERE explain ILIKE '%Granules: 3/4%';
SELECT 'safe cross-type array NOT has',
    (SELECT count() FROM sp64_t WHERE NOT has([toUInt8(1), NULL], k)) = (SELECT count() FROM sp64_o WHERE NOT has([toUInt8(1), NULL], k));
DROP TABLE sp64_t; DROP TABLE sp64_o;

-- A composite nullable key: both tuple elements take the safe exit, so the tuple atom keeps pruning.

DROP TABLE IF EXISTS sptu_t; DROP TABLE IF EXISTS sptu_o;
CREATE TABLE sptu_t (a Nullable(UInt8), b Nullable(UInt8)) ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 1, allow_nullable_key = 1;
CREATE TABLE sptu_o (a Nullable(UInt8), b Nullable(UInt8)) ENGINE = Memory;
INSERT INTO sptu_t VALUES (0, 0), (1, 1), (NULL, 1), (2, NULL);
INSERT INTO sptu_o VALUES (0, 0), (1, 1), (NULL, 1), (2, NULL);
SELECT 'safe nullable tuple keeps pruning', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM sptu_t WHERE (a, b) NOT IN (SELECT tuple(CAST(NULL, 'Nullable(UInt8)'), CAST(1, 'Nullable(UInt8)'))) SETTINGS transform_null_in = 1) WHERE explain ILIKE '%Granules: 3/4%';
SELECT 'safe nullable tuple',
    (SELECT count() FROM sptu_t WHERE (a, b) NOT IN (SELECT tuple(CAST(NULL, 'Nullable(UInt8)'), CAST(1, 'Nullable(UInt8)'))) SETTINGS transform_null_in = 1) = (SELECT count() FROM sptu_o WHERE (a, b) NOT IN (SELECT tuple(CAST(NULL, 'Nullable(UInt8)'), CAST(1, 'Nullable(UInt8)'))) SETTINGS transform_null_in = 1);
DROP TABLE sptu_t; DROP TABLE sptu_o;

SELECT '--- a nested container under the key type: the target must be rejected, not thrown on ---';

-- `castColumnAccurateOrNull` validates its target RECURSIVELY and throws for a type that cannot carry
-- a NULL, so testing only the outer type lets that throw escape to the user for a `Tuple`-wrapped
-- `Array`/`Map`. Both shapes below must answer like the `Memory` oracle instead of failing the query.

DROP TABLE IF EXISTS nc_a; DROP TABLE IF EXISTS nc_ao;
CREATE TABLE nc_a (kt Tuple(Array(UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
CREATE TABLE nc_ao (kt Tuple(Array(UInt8))) ENGINE = Memory;
INSERT INTO nc_a VALUES (tuple([1])), (tuple([2]));
INSERT INTO nc_ao VALUES (tuple([1])), (tuple([2]));
SELECT 'nested Array narrowing',
    (SELECT count() FROM nc_a WHERE has([tuple([toUInt16(300)])], kt)) = (SELECT count() FROM nc_ao WHERE has([tuple([toUInt16(300)])], kt)),
    (SELECT count() FROM nc_a WHERE NOT has([tuple([toUInt16(300)])], kt)) = (SELECT count() FROM nc_ao WHERE NOT has([tuple([toUInt16(300)])], kt)),
    (SELECT count() FROM nc_a WHERE kt NOT IN (SELECT tuple([toUInt16(1)]))) = (SELECT count() FROM nc_ao WHERE kt NOT IN (SELECT tuple([toUInt16(1)])));
-- The must-not-regress partner: a same-width nested container still resolves through the early
-- `canBeSafelyCast` exit, so it must keep its atom.
SELECT 'nested Array same width keeps atom', count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM nc_a WHERE has([tuple([toUInt8(1)])], kt)) WHERE explain ILIKE '%element set%';
SELECT 'nested Array same width',
    (SELECT count() FROM nc_a WHERE has([tuple([toUInt8(1)])], kt)) = (SELECT count() FROM nc_ao WHERE has([tuple([toUInt8(1)])], kt));
DROP TABLE nc_a; DROP TABLE nc_ao;

DROP TABLE IF EXISTS nc_m; DROP TABLE IF EXISTS nc_mo;
CREATE TABLE nc_m (kt Tuple(Map(UInt8, UInt8))) ENGINE = MergeTree ORDER BY kt SETTINGS index_granularity = 1;
CREATE TABLE nc_mo (kt Tuple(Map(UInt8, UInt8))) ENGINE = Memory;
INSERT INTO nc_m VALUES (tuple(map(1, 1))), (tuple(map(2, 2)));
INSERT INTO nc_mo VALUES (tuple(map(1, 1))), (tuple(map(2, 2)));
SELECT 'nested Map narrowing',
    (SELECT count() FROM nc_m WHERE has([tuple(map(toUInt16(300), toUInt16(300)))], kt)) = (SELECT count() FROM nc_mo WHERE has([tuple(map(toUInt16(300), toUInt16(300)))], kt)),
    (SELECT count() FROM nc_m WHERE NOT has([tuple(map(toUInt16(300), toUInt16(300)))], kt)) = (SELECT count() FROM nc_mo WHERE NOT has([tuple(map(toUInt16(300), toUInt16(300)))], kt));
DROP TABLE nc_m; DROP TABLE nc_mo;
