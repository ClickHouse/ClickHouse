-- Tags: no-s3-storage

SET use_uncompressed_cache = 0;

SELECT '====array====';
DROP TABLE IF EXISTS t_arr;
CREATE TABLE t_arr (a Array(UInt32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_arr VALUES ([1]) ([]) ([1, 2, 3]) ([1, 2]);

SYSTEM DROP MARK CACHE;
SELECT a.size0 FROM t_arr;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['FileOpen']
FROM system.query_log
WHERE (type = 'QueryFinish') AND (lower(query) LIKE lower('SELECT a.size0 FROM %t_arr%'))
    AND current_database = currentDatabase();

SELECT '====tuple====';
DROP TABLE IF EXISTS t_tup;
CREATE TABLE t_tup (t Tuple(s String, u UInt32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_tup VALUES (('foo', 1)) (('bar', 2)) (('baz', 42));

SYSTEM DROP MARK CACHE;
SELECT t.s FROM t_tup;

SYSTEM DROP MARK CACHE;
SELECT t.u FROM t_tup;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['FileOpen']
FROM system.query_log
WHERE (type = 'QueryFinish') AND (lower(query) LIKE lower('SELECT t._ FROM %t_tup%'))
    AND current_database = currentDatabase();

SELECT '====nullable====';
DROP TABLE IF EXISTS t_nul;
CREATE TABLE t_nul (n Nullable(UInt32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_nul VALUES (1) (NULL) (2) (NULL);

SYSTEM DROP MARK CACHE;
SELECT n.null FROM t_nul;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['FileOpen']
FROM system.query_log
WHERE (type = 'QueryFinish') AND (lower(query) LIKE lower('SELECT n.null FROM %t_nul%'))
    AND current_database = currentDatabase();

SELECT '====map====';
SET allow_experimental_map_type = 1;
DROP TABLE IF EXISTS t_map;
CREATE TABLE t_map (m Map(String, UInt32)) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_map VALUES (map('a', 1, 'b', 2)) (map('a', 3, 'c', 4)), (map('b', 5, 'c', 6));

--- will read 4 files: keys.bin, keys.mrk2, size0.bin, size0.mrk2
SYSTEM DROP MARK CACHE;
SELECT m.keys FROM t_map;

SYSTEM DROP MARK CACHE;
SELECT m.values FROM t_map;

SYSTEM FLUSH LOGS;
SELECT ProfileEvents['FileOpen']
FROM system.query_log
WHERE (type = 'QueryFinish') AND (lower(query) LIKE lower('SELECT m.% FROM %t_map%'))
    AND current_database = currentDatabase();

DROP TABLE t_arr;
DROP TABLE t_nul;
DROP TABLE t_tup;
DROP TABLE t_map;
