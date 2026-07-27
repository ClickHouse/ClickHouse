-- Tags: no-replicated-database, no-parallel-replicas, no-random-merge-tree-settings
-- no-parallel-replicas: the assertions are about local `KeyCondition` part pruning; parallel replicas
--   reshape the plan into a `Union` with `ReadFromRemoteParallelReplicas` and duplicated `Indexes` blocks.
-- no-replicated-database: the DBReplicated job replaces engines and can change the plan shape.
-- no-random-merge-tree-settings: `Parts: N/M` prints the part counts around each pruning stage, so an
--   extra randomized stage (implicit column statistics) shifts both numbers.

-- Regression test for issue #111340: `transform_null_in = 1`, non-Nullable key column, `IN`/`NOT IN`
-- a subquery whose result is Nullable. Previously threw CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN (349).

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
