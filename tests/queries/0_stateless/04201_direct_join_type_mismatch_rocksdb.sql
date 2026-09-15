-- Tags: use-rocksdb
-- Regression test for STID 3436-4175.
--
-- Bug: when the right key of an INNER/LEFT JOIN against an EmbeddedRocksDB table
-- has a name matching the storage's primary key but a different type, the legacy
-- USING-join planner path inserted an inline cast that preserved the column name.
-- `tryDirectJoin` then matched the (post-cast) column name against the storage's
-- primary key name and chose `DirectKeyValueJoin`, which passed the supertype-cast
-- key to `StorageEmbeddedRocksDB::getByKeys`. That triggered the
-- "Primary key type mismatch" `LOGICAL_ERROR`.
--
-- Fix: in `tryDirectJoin`, after the name match, also verify the right key's plan
-- type matches the storage's primary key type (modulo `Nullable`/`LowCardinality`
-- wrapping, matching `getByKeys`'s own equivalence). On mismatch, decline direct
-- join so the planner falls back to `HashJoin`, which handles type conversion.

DROP TABLE IF EXISTS rdb_pk_type;
DROP TABLE IF EXISTS t_uint64_nullable;
DROP TABLE IF EXISTS t_int64;
DROP TABLE IF EXISTS t_decimal;
DROP TABLE IF EXISTS t_uint32;

CREATE TABLE rdb_pk_type (key UInt32, value String) ENGINE = EmbeddedRocksDB PRIMARY KEY (key);
INSERT INTO rdb_pk_type VALUES (1, 'a'), (2, 'b'), (3, 'c');

-- Case 1: right key Nullable(UInt64) vs storage UInt32 (the original AST fuzzer pattern).
-- Without the fix this triggered LOGICAL_ERROR "Primary key type mismatch, expected UInt32, got Nullable(UInt64)."
CREATE TABLE t_uint64_nullable (k Nullable(UInt64)) ENGINE = TinyLog;
INSERT INTO t_uint64_nullable VALUES (1), (2), (3), (4);

SELECT 'case 1 new planner';
SELECT * FROM (SELECT k AS key FROM t_uint64_nullable) AS t
INNER JOIN rdb_pk_type USING (key)
ORDER BY key ASC NULLS FIRST
SETTINGS query_plan_use_new_logical_join_step = 1;

SELECT 'case 1 legacy planner';
SELECT * FROM (SELECT k AS key FROM t_uint64_nullable) AS t
INNER JOIN rdb_pk_type USING (key)
ORDER BY key ASC NULLS FIRST
SETTINGS query_plan_use_new_logical_join_step = 0;

-- Case 2: right key Int64 (different signedness) vs storage UInt32.
CREATE TABLE t_int64 (k Int64) ENGINE = TinyLog;
INSERT INTO t_int64 VALUES (1), (2), (3), (4);

SELECT 'case 2 legacy planner';
SELECT * FROM (SELECT k AS key FROM t_int64) AS t
INNER JOIN rdb_pk_type USING (key)
ORDER BY key
SETTINGS query_plan_use_new_logical_join_step = 0;

-- Case 3: right key Decimal vs storage UInt32. Cast under USING produces a Decimal
-- supertype; the type mismatch must not trip the LOGICAL_ERROR.
CREATE TABLE t_decimal (k Decimal(18, 7)) ENGINE = TinyLog;
INSERT INTO t_decimal VALUES (1), (2), (3), (4);

SELECT 'case 3 legacy planner';
SELECT * FROM (SELECT k AS key FROM t_decimal) AS t
INNER JOIN rdb_pk_type USING (key)
ORDER BY key
SETTINGS query_plan_use_new_logical_join_step = 0;

-- Case 4: matching types — `DirectKeyValueJoin` must still be picked.
-- Verifies the type check is not overly aggressive.
CREATE TABLE t_uint32 (k UInt32) ENGINE = TinyLog;
INSERT INTO t_uint32 VALUES (1), (2), (3), (4);

SELECT 'case 4 legacy planner (matching types, direct join still picked)';
SELECT * FROM (SELECT k AS key FROM t_uint32) AS t
INNER JOIN rdb_pk_type USING (key)
ORDER BY key
SETTINGS query_plan_use_new_logical_join_step = 0;

SELECT 'case 4 explain shows DirectKeyValueJoin';
SELECT trim(explain)
FROM (
    EXPLAIN actions = 1
    SELECT * FROM (SELECT k AS key FROM t_uint32) AS t
    INNER JOIN rdb_pk_type USING (key)
    SETTINGS query_plan_use_new_logical_join_step = 0
)
WHERE explain LIKE '%Algorithm:%';

-- The remaining cases lock in the `LEFT JOIN ... USING (key)` strictness paths
-- (`allowed_left` accepts `ANY`/`ALL`/`SEMI`/`ANTI` in `tryDirectJoin`). Each case
-- triggers the same `tryDirectJoin` -> `StorageEmbeddedRocksDB::getByKeys` path
-- that crashed for the `INNER JOIN` cases above; without the fix they all abort
-- with the "Primary key type mismatch" exception.

-- Case 5: LEFT JOIN (default `ALL` strictness) with mismatched type.
SELECT 'case 5 LEFT JOIN legacy planner';
SELECT key, value FROM (SELECT k AS key FROM t_uint64_nullable) AS t
LEFT JOIN rdb_pk_type USING (key)
ORDER BY key ASC NULLS FIRST
SETTINGS query_plan_use_new_logical_join_step = 0;

-- Case 6: LEFT ANY JOIN with mismatched type — exercises the `Any` strictness path
-- (the canonical strictness for direct key-value lookups).
SELECT 'case 6 LEFT ANY JOIN legacy planner';
SELECT key, value FROM (SELECT k AS key FROM t_uint64_nullable) AS t
LEFT ANY JOIN rdb_pk_type USING (key)
ORDER BY key ASC NULLS FIRST
SETTINGS query_plan_use_new_logical_join_step = 0;

-- Case 7: LEFT SEMI JOIN with mismatched type — exercises the `Semi` strictness path.
SELECT 'case 7 LEFT SEMI JOIN legacy planner';
SELECT key FROM (SELECT k AS key FROM t_uint64_nullable) AS t
LEFT SEMI JOIN rdb_pk_type USING (key)
ORDER BY key ASC NULLS FIRST
SETTINGS query_plan_use_new_logical_join_step = 0;

-- Case 8: LEFT ANTI JOIN with mismatched type — exercises the `Anti` strictness path.
SELECT 'case 8 LEFT ANTI JOIN legacy planner';
SELECT key FROM (SELECT k AS key FROM t_uint64_nullable) AS t
LEFT ANTI JOIN rdb_pk_type USING (key)
ORDER BY key ASC NULLS FIRST
SETTINGS query_plan_use_new_logical_join_step = 0;

-- Case 9: matching types LEFT JOIN — `DirectKeyValueJoin` must still be picked
-- so the type check is not overly aggressive on the LEFT path either.
SELECT 'case 9 LEFT JOIN legacy planner (matching types, direct join still picked)';
SELECT key, value FROM (SELECT k AS key FROM t_uint32) AS t
LEFT JOIN rdb_pk_type USING (key)
ORDER BY key
SETTINGS query_plan_use_new_logical_join_step = 0;

SELECT 'case 9 explain shows DirectKeyValueJoin';
SELECT trim(explain)
FROM (
    EXPLAIN actions = 1
    SELECT * FROM (SELECT k AS key FROM t_uint32) AS t
    LEFT JOIN rdb_pk_type USING (key)
    SETTINGS query_plan_use_new_logical_join_step = 0
)
WHERE explain LIKE '%Algorithm:%';

DROP TABLE rdb_pk_type;
DROP TABLE t_uint64_nullable;
DROP TABLE t_int64;
DROP TABLE t_decimal;
DROP TABLE t_uint32;
