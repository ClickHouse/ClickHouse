-- Tags: no-parallel-replicas, no-replicated-database
-- no-parallel-replicas: the EXPLAIN assertion below requires the exact-count projection to
--   short-circuit on the initiator, which is suppressed when reading from remote replicas.
-- no-replicated-database: EXPLAIN output differs for a replicated database.

-- `SELECT count()` answered from the implicit exact-count projection reads a granule's key range
-- instead of its rows. A NULL key value is analysed as the `+inf` stand-in of the `NULLS LAST` order,
-- and a comparison is NULL - so `WHERE` rejects the row - for such a key, which the two-valued range
-- algebra cannot express: a granule of NULLs looked like a definite match of a negated comparison, so
-- `count()` counted rows that the same query without the projection, and `SELECT *`, do not return.

SET optimize_use_projections = 1, optimize_use_implicit_projections = 1;

DROP TABLE IF EXISTS t_nullable_key;
CREATE TABLE t_nullable_key (id Nullable(String), s Nullable(String)) ENGINE = MergeTree ORDER BY (id, s)
SETTINGS allow_nullable_key = 1, index_granularity = 1;
INSERT INTO t_nullable_key VALUES ('s', 's'), (NULL, 's1'), (NULL, NULL);

SELECT 'the rows a negated equality really returns';
SELECT id, s FROM t_nullable_key WHERE id != '';

SELECT 'count of the same condition';
SELECT count() FROM t_nullable_key WHERE id != '';
SELECT count() FROM t_nullable_key WHERE id != '' SETTINGS optimize_use_implicit_projections = 0;

SELECT 'and of a negated set';
SELECT count() FROM t_nullable_key WHERE id NOT IN ('');
SELECT count() FROM t_nullable_key WHERE id NOT IN ('') SETTINGS optimize_use_implicit_projections = 0;

SELECT 'and of a negated pattern';
SELECT count() FROM t_nullable_key WHERE id NOT LIKE 'z%';
SELECT count() FROM t_nullable_key WHERE id NOT LIKE 'z%' SETTINGS optimize_use_implicit_projections = 0;

-- More NULLs than values, so a count that trusts the key range is off by more than one row.
DROP TABLE IF EXISTS t_nullable_key_wide;
CREATE TABLE t_nullable_key_wide (id Nullable(UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO t_nullable_key_wide SELECT if(number % 4 = 0, NULL, number) FROM numbers(16);

SELECT 'a key of mostly NULLs';
SELECT count() FROM t_nullable_key_wide WHERE id != 1;
SELECT count() FROM t_nullable_key_wide WHERE id != 1 SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_nullable_key_wide WHERE id IS NULL;
SELECT count() FROM t_nullable_key_wide WHERE id IS NOT NULL;

-- A key that holds no NULL, so no row is rejected by three-valued logic.
DROP TABLE IF EXISTS t_nullable_key_without_nulls;
CREATE TABLE t_nullable_key_without_nulls (id Nullable(UInt32)) ENGINE = MergeTree ORDER BY id
SETTINGS allow_nullable_key = 1, index_granularity = 2;
INSERT INTO t_nullable_key_without_nulls SELECT number FROM numbers(16);

SELECT 'a nullable key without NULLs';
SELECT count() FROM t_nullable_key_without_nulls WHERE id != 1;
SELECT count() FROM t_nullable_key_without_nulls WHERE id != 1 SETTINGS optimize_use_implicit_projections = 0;

-- The exact-count projection is still used where NULL cannot be involved.
DROP TABLE IF EXISTS t_key;
CREATE TABLE t_key (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_key SELECT number FROM numbers(16);

SELECT 'the exact count of a non-nullable key';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_key WHERE id != 100) WHERE explain ILIKE '%_exact_count_projection%';
SELECT count() FROM t_key WHERE id != 100;

DROP TABLE t_key;
DROP TABLE t_nullable_key_without_nulls;
DROP TABLE t_nullable_key_wide;
DROP TABLE t_nullable_key;

-- With `transform_null_in`, set membership compares a NULL as an ordinary element and so answers
-- true or false for a NULL key value. The range algebra describes that exactly, so these counts must
-- match the same query without the projection; the cases above keep the three-valued form conservative.
SET transform_null_in = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_null_set;
CREATE TABLE t_null_set (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY x
SETTINGS allow_nullable_key = 1, index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_null_set VALUES (1), (2), (3), (4), (NULL), (NULL), (NULL);

SELECT 'a set holding only NULL';
-- `projections = 1` reports that the exact-count projection was USED, which the plain `EXPLAIN`
-- above does not: it lists the projection as a candidate even when the count is read from the rows.
SELECT count() > 0 FROM (EXPLAIN projections = 1 SELECT count() FROM t_null_set WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))')))) WHERE explain ILIKE '%exact count optimization is applied%' SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS optimize_use_implicit_projections = 0;

SELECT 'its negation';
SELECT count() FROM t_null_set WHERE x NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set WHERE x NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS optimize_use_implicit_projections = 0;

SELECT 'over the lightweight primary index';
SELECT count() > 0 FROM (EXPLAIN projections = 1 SELECT count() FROM t_null_set WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))')))) WHERE explain ILIKE '%exact count optimization is applied%' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_null_set WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_null_set WHERE x NOT IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS use_lightweight_primary_key_index_analysis = 1;

-- `has` compares a NULL array element the same way, and does so without `transform_null_in`.
SELECT 'a NULL element of a has() array';
SELECT count() FROM t_null_set WHERE has(CAST([NULL], 'Array(Nullable(UInt64))'), x) SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set WHERE has(CAST([NULL], 'Array(Nullable(UInt64))'), x) SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_null_set WHERE has(CAST([NULL], 'Array(Nullable(UInt64))'), x) SETTINGS optimize_use_implicit_projections = 0;

-- A reversed key stores its NULLs physically first rather than last.
DROP TABLE IF EXISTS t_null_set_desc;
CREATE TABLE t_null_set_desc (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY x DESC
SETTINGS allow_nullable_key = 1, index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_null_set_desc VALUES (1), (2), (3), (4), (NULL), (NULL), (NULL);

SELECT 'a reversed key';
SELECT count() FROM t_null_set_desc WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set_desc WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_null_set_desc WHERE x IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS optimize_use_implicit_projections = 0;

DROP TABLE t_null_set_desc;

-- Exactness stops at a monotonic wrapper on the key: the wrapper is not applied to the NULL stand-in
-- bound, so the range algebra cannot see that it redefines the predicate for a NULL row - a `CAST` to
-- a non-Nullable type refuses that row outright. Every key value below is NULL, so no bound is finite
-- and the wrapper is skipped throughout; the projection must not answer what reading the rows raises.
DROP TABLE IF EXISTS t_null_set_all;
CREATE TABLE t_null_set_all (x Nullable(UInt64)) ENGINE = MergeTree ORDER BY x
SETTINGS allow_nullable_key = 1, index_granularity = 1, add_minmax_index_for_numeric_columns = 0;
INSERT INTO t_null_set_all VALUES (NULL), (NULL), (NULL);

SELECT 'a wrapped key must still raise';
SELECT count() FROM t_null_set_all WHERE CAST(x, 'UInt64') NOT IN (0) SETTINGS use_lightweight_primary_key_index_analysis = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT count() FROM t_null_set_all WHERE CAST(x, 'UInt64') NOT IN (0) SETTINGS use_lightweight_primary_key_index_analysis = 1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT count() FROM t_null_set_all WHERE CAST(x, 'UInt64') NOT IN (0) SETTINGS optimize_use_implicit_projections = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

SELECT 'and so must one under has()';
SELECT count() FROM t_null_set_all WHERE NOT has(CAST([NULL], 'Array(Nullable(UInt64))'), CAST(x, 'UInt64')) SETTINGS use_lightweight_primary_key_index_analysis = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT count() FROM t_null_set_all WHERE NOT has(CAST([NULL], 'Array(Nullable(UInt64))'), CAST(x, 'UInt64')) SETTINGS use_lightweight_primary_key_index_analysis = 1; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT count() FROM t_null_set_all WHERE NOT has(CAST([NULL], 'Array(Nullable(UInt64))'), CAST(x, 'UInt64')) SETTINGS optimize_use_implicit_projections = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

-- A bare key takes the exemption on a negated set too: no set element equals the NULL stand-in, so every
-- granule of this all-NULL key answers definitely and the projection has nothing exact without it.
SELECT 'a bare key under a negated set';
SELECT count() > 0 FROM (EXPLAIN projections = 1 SELECT count() FROM t_null_set_all WHERE x NOT IN (0)) WHERE explain ILIKE '%_exact_count_projection%' SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() > 0 FROM (EXPLAIN projections = 1 SELECT count() FROM t_null_set_all WHERE x NOT IN (0)) WHERE explain ILIKE '%_exact_count_projection%' SETTINGS use_lightweight_primary_key_index_analysis = 1;
SELECT count() FROM t_null_set_all WHERE x NOT IN (0) SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set_all WHERE x NOT IN (0) SETTINGS optimize_use_implicit_projections = 0;

-- The `global` spellings are separate entries of the key condition's atom table, so they take the
-- exemption separately: these two arms are the same reads as above under `GLOBAL IN` and `GLOBAL NOT IN`.
SELECT 'the global spelling of a set holding only NULL';
SELECT count() > 0 FROM (EXPLAIN projections = 1 SELECT count() FROM t_null_set WHERE x GLOBAL IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))')))) WHERE explain ILIKE '%exact count optimization is applied%' SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set WHERE x GLOBAL IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set WHERE x GLOBAL IN (SELECT arrayJoin(CAST([NULL], 'Array(Nullable(UInt64))'))) SETTINGS optimize_use_implicit_projections = 0;

SELECT 'and of its negation over a bare key';
SELECT count() > 0 FROM (EXPLAIN projections = 1 SELECT count() FROM t_null_set_all WHERE x GLOBAL NOT IN (0)) WHERE explain ILIKE '%_exact_count_projection%' SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set_all WHERE x GLOBAL NOT IN (0) SETTINGS use_lightweight_primary_key_index_analysis = 0;
SELECT count() FROM t_null_set_all WHERE x GLOBAL NOT IN (0) SETTINGS optimize_use_implicit_projections = 0;

DROP TABLE t_null_set_all;
DROP TABLE t_null_set;
