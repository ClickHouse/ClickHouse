-- Tags: no-fasttest, use-rocksdb
-- Tag no-fasttest: in fasttest ENABLE_LIBRARIES=0, so the rocksdb engine is not built.

-- A key-value primary key that cannot be inside Nullable must not be pushed through
-- accurateCastOrNull. The set element differs from the key only by a nested Nullable and holds no
-- NULL, so filter pushdown declines and the full scan answers the same as an unindexed table.

DROP TABLE IF EXISTS 04701_rdb_tuple;
DROP TABLE IF EXISTS 04701_mem_tuple;

CREATE TABLE 04701_rdb_tuple (k Tuple(Array(String)), v String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (k);
CREATE TABLE 04701_mem_tuple (k Tuple(Array(String)), v String) ENGINE = Memory;

INSERT INTO 04701_rdb_tuple VALUES ((['a']), 'x'), ((['b']), 'y');
INSERT INTO 04701_mem_tuple VALUES ((['a']), 'x'), ((['b']), 'y');

SELECT count() FROM 04701_rdb_tuple WHERE k IN (SELECT CAST(tuple(['a']), 'Tuple(Array(Nullable(String)))'));
SELECT count() FROM 04701_mem_tuple WHERE k IN (SELECT CAST(tuple(['a']), 'Tuple(Array(Nullable(String)))'));
SELECT count() FROM 04701_rdb_tuple WHERE k IN (SELECT tuple(['a']));

DROP TABLE 04701_rdb_tuple;
DROP TABLE 04701_mem_tuple;

DROP TABLE IF EXISTS 04701_rdb_array;
DROP TABLE IF EXISTS 04701_mem_array;

CREATE TABLE 04701_rdb_array (k Array(String), v String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (k);
CREATE TABLE 04701_mem_array (k Array(String), v String) ENGINE = Memory;

INSERT INTO 04701_rdb_array VALUES (['a'], 'x'), (['b'], 'y');
INSERT INTO 04701_mem_array VALUES (['a'], 'x'), (['b'], 'y');

SELECT count() FROM 04701_rdb_array WHERE k IN (SELECT CAST(['a'], 'Array(Nullable(String))'));
SELECT count() FROM 04701_mem_array WHERE k IN (SELECT CAST(['a'], 'Array(Nullable(String))'));

DROP TABLE 04701_rdb_array;
DROP TABLE 04701_mem_array;

DROP TABLE IF EXISTS 04701_rdb_map;
DROP TABLE IF EXISTS 04701_mem_map;

CREATE TABLE 04701_rdb_map (k Map(String, String), v String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (k);
CREATE TABLE 04701_mem_map (k Map(String, String), v String) ENGINE = Memory;

INSERT INTO 04701_rdb_map VALUES (map('a', '1'), 'x'), (map('b', '2'), 'y');
INSERT INTO 04701_mem_map VALUES (map('a', '1'), 'x'), (map('b', '2'), 'y');

SELECT count() FROM 04701_rdb_map WHERE k IN (SELECT CAST(map('a', '1'), 'Map(String, Nullable(String))'));
SELECT count() FROM 04701_mem_map WHERE k IN (SELECT CAST(map('a', '1'), 'Map(String, Nullable(String))'));

-- A container key loses its point lookup, which the read type reports directly.
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM 04701_rdb_map WHERE k IN (SELECT CAST(map('a', '1'), 'Map(String, Nullable(String))'))) WHERE explain LIKE '%ReadType%';

DROP TABLE 04701_rdb_map;
DROP TABLE 04701_mem_map;

-- A scalar key keeps its point lookup: the guard above must not widen to targets accurateCastOrNull
-- does accept.

DROP TABLE IF EXISTS 04701_rdb_scalar;

CREATE TABLE 04701_rdb_scalar (k String, v String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (k);

INSERT INTO 04701_rdb_scalar VALUES ('a', 'x'), ('b', 'y');

SELECT count() FROM 04701_rdb_scalar WHERE k IN (SELECT CAST('a', 'Nullable(String)'));
SELECT count() FROM 04701_rdb_scalar WHERE k IN ('a');
SELECT count() FROM 04701_rdb_scalar WHERE k IN (SELECT CAST(NULL, 'Nullable(String)'));
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM 04701_rdb_scalar WHERE k IN ('a')) WHERE explain LIKE '%ReadType%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM 04701_rdb_scalar WHERE k IN (SELECT CAST('a', 'Nullable(String)'))) WHERE explain LIKE '%ReadType%';
-- A Nullable set element is unwrapped by the set itself, so the two assertions above take the safe
-- branch. A Dynamic element reaches the guarded branch and is what reports a guard widened too far.
SELECT count() FROM 04701_rdb_scalar WHERE k IN (SELECT CAST('a', 'Dynamic'));
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM 04701_rdb_scalar WHERE k IN (SELECT CAST('a', 'Dynamic'))) WHERE explain LIKE '%ReadType%';

DROP TABLE 04701_rdb_scalar;

-- A LowCardinality(Nullable(String)) key can hold a NULL of its own, but still cannot be wrapped in
-- Nullable, so the guard must ask the cast about the target rather than only about NULL capability.

DROP TABLE IF EXISTS 04701_rdb_lc_nullable;

CREATE TABLE 04701_rdb_lc_nullable (k LowCardinality(Nullable(String)), v String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (k);

INSERT INTO 04701_rdb_lc_nullable VALUES ('a', 'x'), ('b', 'y');

SELECT count() FROM 04701_rdb_lc_nullable WHERE k IN (SELECT CAST('a', 'Dynamic'));
SELECT count() FROM 04701_rdb_lc_nullable WHERE k IN (SELECT CAST(NULL, 'Dynamic'));
SELECT count() FROM 04701_rdb_lc_nullable WHERE k IN (SELECT CAST('a', 'Variant(String, UInt8)'));
SELECT count() FROM 04701_rdb_lc_nullable WHERE k IN (SELECT CAST('a', 'Nullable(String)'));
SELECT count() FROM 04701_rdb_lc_nullable WHERE k IN ('a');
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM 04701_rdb_lc_nullable WHERE k IN ('a')) WHERE explain LIKE '%ReadType%';
-- The counts above stay correct through a full scan, so the read type is asserted on the two
-- carriers themselves.
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM 04701_rdb_lc_nullable WHERE k IN (SELECT CAST('a', 'Dynamic'))) WHERE explain LIKE '%ReadType%';
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM 04701_rdb_lc_nullable WHERE k IN (SELECT CAST('a', 'Variant(String, UInt8)'))) WHERE explain LIKE '%ReadType%';

DROP TABLE 04701_rdb_lc_nullable;

-- A LowCardinality(String) key is only an encoding of String, so the cast target is stripped and the
-- point lookup survives an element the key type cannot be cast to safely.

DROP TABLE IF EXISTS 04701_rdb_lc;
DROP TABLE IF EXISTS 04701_mem_lc;

CREATE TABLE 04701_rdb_lc (k LowCardinality(String), v String)
ENGINE = EmbeddedRocksDB PRIMARY KEY (k);
CREATE TABLE 04701_mem_lc (k LowCardinality(String), v String) ENGINE = Memory;

INSERT INTO 04701_rdb_lc VALUES ('a', 'x'), ('b', 'y');
INSERT INTO 04701_mem_lc VALUES ('a', 'x'), ('b', 'y');

-- Selecting the value proves the stripped cast target still produces the declared key encoding: a
-- wrong encoding would find no key on the point-lookup plan.
SELECT v FROM 04701_rdb_lc WHERE k IN (SELECT CAST('a', 'Dynamic'));
SELECT v FROM 04701_mem_lc WHERE k IN (SELECT CAST('a', 'Dynamic'));
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM 04701_rdb_lc WHERE k IN (SELECT CAST('a', 'Dynamic'))) WHERE explain LIKE '%ReadType%';
SELECT v FROM 04701_rdb_lc WHERE k IN (SELECT CAST('b', 'Variant(String, UInt8)'));
SELECT trimLeft(explain) FROM (EXPLAIN actions = 1 SELECT count() FROM 04701_rdb_lc WHERE k IN (SELECT CAST('b', 'Variant(String, UInt8)'))) WHERE explain LIKE '%ReadType%';
SELECT count() FROM 04701_rdb_lc WHERE k IN (SELECT CAST(NULL, 'Dynamic'));
SELECT count() FROM 04701_mem_lc WHERE k IN (SELECT CAST(NULL, 'Dynamic'));

DROP TABLE 04701_rdb_lc;
DROP TABLE 04701_mem_lc;
