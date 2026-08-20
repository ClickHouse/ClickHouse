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

DROP TABLE 04701_rdb_scalar;
