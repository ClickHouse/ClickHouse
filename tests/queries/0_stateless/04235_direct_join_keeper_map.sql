-- Tags: no-fasttest, no-ordinary-database

DROP TABLE IF EXISTS km_str SYNC;
DROP TABLE IF EXISTS km_str_null SYNC;
DROP TABLE IF EXISTS km_str_lc SYNC;
DROP TABLE IF EXISTS t_str;
DROP TABLE IF EXISTS t_null;

CREATE TABLE km_str (key String, value String) Engine = KeeperMap('/' || currentDatabase() || '/04235_km') PRIMARY KEY (key);
INSERT INTO km_str VALUES ('a', 'A'), ('b', 'B'), ('c', 'C');

CREATE TABLE t_str (k String) ENGINE = TinyLog;
INSERT INTO t_str VALUES ('a'), ('b'), ('c'), ('d');

CREATE TABLE t_null (k Nullable(String)) ENGINE = TinyLog;
INSERT INTO t_null VALUES ('a'), ('b'), ('c'), ('d'), (NULL);

SELECT 'plain inner';
SELECT key, value FROM (SELECT k AS key FROM t_str) AS t
INNER JOIN km_str USING (key) ORDER BY key;

SELECT 'plain left';
SELECT key, value FROM (SELECT k AS key FROM t_str) AS t
LEFT JOIN km_str USING (key) ORDER BY key;

SELECT 'nullable left inner';
SELECT key, value FROM (SELECT k AS key FROM t_null) AS t
INNER JOIN km_str USING (key) ORDER BY key;

SELECT 'nullable left left';
SELECT key, value FROM (SELECT k AS key FROM t_null) AS t
LEFT JOIN km_str USING (key) ORDER BY key NULLS LAST, value;

SELECT 'explain';
SELECT trim(explain) FROM (
    EXPLAIN actions = 1
    SELECT key, value FROM (SELECT k AS key FROM t_str) AS t
    INNER JOIN km_str USING (key)
) WHERE explain LIKE '%Algorithm:%';

DROP TABLE km_str SYNC;
DROP TABLE t_str;
DROP TABLE t_null;
