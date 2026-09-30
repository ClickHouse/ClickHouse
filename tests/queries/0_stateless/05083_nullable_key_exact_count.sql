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
