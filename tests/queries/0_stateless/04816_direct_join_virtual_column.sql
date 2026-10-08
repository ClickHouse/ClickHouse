-- Tags: use-rocksdb
-- Direct join published a storage data column under a right-side virtual column's name.

DROP TABLE IF EXISTS t_04816_rocks;
DROP TABLE IF EXISTS t_04816_src;
DROP DICTIONARY IF EXISTS d_04816;

CREATE TABLE t_04816_rocks (k LowCardinality(String), v String) ENGINE = EmbeddedRocksDB PRIMARY KEY k;
INSERT INTO t_04816_rocks VALUES ('KEYA', 'VALA');

CREATE TABLE t_04816_src (k Nullable(UInt64), v String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04816_src VALUES (1, 'one');
CREATE DICTIONARY d_04816 (k Nullable(UInt64), v String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 't_04816_src' DB currentDatabase())) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(0);

SELECT dd.v, dd._table FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
LEFT JOIN t_04816_rocks AS dd ON t.pref = dd.k;
SELECT dd.v, dd._table FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
LEFT JOIN t_04816_rocks AS dd ON t.pref = dd.k SETTINGS join_algorithm = 'hash';

SELECT dd.v, dd._table FROM (SELECT CAST(1, 'Nullable(UInt64)') AS pref) AS t
LEFT JOIN d_04816 AS dd ON t.pref = dd.k;
SELECT dd.v, dd._table FROM (SELECT CAST(1, 'Nullable(UInt64)') AS pref) AS t
LEFT JOIN d_04816 AS dd ON t.pref = dd.k SETTINGS join_algorithm = 'hash';

SELECT dd.v, dd._database = currentDatabase() FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
LEFT JOIN t_04816_rocks AS dd ON t.pref = dd.k;
SELECT dd.v, dd._database = currentDatabase() FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
LEFT JOIN t_04816_rocks AS dd ON t.pref = dd.k SETTINGS join_algorithm = 'hash';

SELECT dd.v, dd._table FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
INNER JOIN t_04816_rocks AS dd ON t.pref = dd.k;
SELECT dd.v, dd._table FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
INNER JOIN t_04816_rocks AS dd ON t.pref = dd.k SETTINGS join_algorithm = 'hash';

SELECT dd.v FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
LEFT JOIN t_04816_rocks AS dd ON t.pref = dd.k;

SELECT dd.v, dd._table FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
LEFT JOIN t_04816_rocks AS dd ON t.pref = dd.k SETTINGS join_algorithm = 'direct'; -- { serverError NOT_IMPLEMENTED }

SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT dd.v FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
LEFT JOIN t_04816_rocks AS dd ON t.pref = dd.k) WHERE explain ILIKE '%Algorithm: DirectKeyValueJoin%';
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT dd.v, dd._table FROM (SELECT CAST('KEYA', 'LowCardinality(String)') AS pref) AS t
LEFT JOIN t_04816_rocks AS dd ON t.pref = dd.k) WHERE explain ILIKE '%Algorithm: DirectKeyValueJoin%';

DROP DICTIONARY d_04816;
DROP TABLE t_04816_src;
DROP TABLE t_04816_rocks;
