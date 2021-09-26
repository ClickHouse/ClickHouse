SET send_logs_level = 'fatal';

DROP DATABASE IF EXISTS db_01115;
CREATE DATABASE db_01115;

USE db_01115;

DROP DICTIONARY IF EXISTS dict_flat;
DROP DICTIONARY IF EXISTS dict_hashed;
DROP DICTIONARY IF EXISTS dict_complex_cache;

CREATE TABLE t1 (key UInt64, a UInt8, b String, c Float64) ENGINE = MergeTree() ORDER BY key;
INSERT INTO t1 SELECT number, number, toString(number), number from numbers(4);

CREATE DICTIONARY dict_flat (key UInt64 DEFAULT 0, a UInt8 DEFAULT 42, b String DEFAULT 'x', c Float64 DEFAULT 42.0)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 't1' PASSWORD '' DB 'db_01115'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

CREATE DICTIONARY db_01115.dict_hashed (key UInt64 DEFAULT 0, a UInt8 DEFAULT 42, b String DEFAULT 'x', c Float64 DEFAULT 42.0)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 't1' DB 'db_01115'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(HASHED());

CREATE DICTIONARY dict_complex_cache (key UInt64 DEFAULT 0, a UInt8 DEFAULT 42, b String DEFAULT 'x', c Float64 DEFAULT 42.0)
PRIMARY KEY key, b
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 't1' DB 'db_01115'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 1));

SET join_use_nulls = 0;

SELECT 'flat: left on';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 LEFT JOIN dict_flat d ON s1.key = d.key ORDER BY s1.key;
SELECT 'flat: left';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 LEFT JOIN dict_flat d USING(key) ORDER BY key;
SELECT 'flat: any left';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 ANY LEFT JOIN dict_flat d USING(key) ORDER BY key;
SELECT 'flat: semi left';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 SEMI JOIN dict_flat d USING(key) ORDER BY key;
SELECT 'flat: anti left';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 ANTI JOIN dict_flat d USING(key) ORDER BY key;
SELECT 'flat: inner';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 JOIN dict_flat d USING(key);
SELECT 'flat: inner on';
SELECT * FROM (SELECT number AS k FROM numbers(100)) s1 JOIN dict_flat d ON k = key ORDER BY k;

SET join_use_nulls = 1;

SELECT 'hashed: left on';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 LEFT JOIN dict_hashed d ON s1.key = d.key ORDER BY s1.key;
SELECT 'hashed: left';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 LEFT JOIN dict_hashed d USING(key) ORDER BY key;
SELECT 'hashed: any left';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 ANY LEFT JOIN dict_hashed d USING(key) ORDER BY key;
SELECT 'hashed: semi left';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 SEMI JOIN dict_hashed d USING(key) ORDER BY key;
SELECT 'hashed: anti left';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 ANTI JOIN dict_hashed d USING(key) ORDER BY key;
SELECT 'hashed: inner';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 JOIN dict_hashed d USING(key);
SELECT 'hashed: inner on';
SELECT * FROM (SELECT number AS k FROM numbers(100)) s1 JOIN dict_hashed d ON k = key ORDER BY k;

SELECT 'complex_cache (smoke)';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 LEFT JOIN dict_complex_cache d ON s1.key = d.key ORDER BY s1.key;

SELECT 'not optimized (smoke)';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 RIGHT JOIN dict_flat d USING(key) ORDER BY key;
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 RIGHT JOIN dict_flat d ON s1.key = d.key ORDER BY d.key;
SELECT '-';
SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s1 FULL JOIN dict_flat d USING(key) ORDER BY s1.key, d.key;
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 ANY INNER JOIN dict_flat d USING(key) ORDER BY s1.key;
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 ANY RIGHT JOIN dict_flat d USING(key) ORDER BY s1.key;
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 SEMI RIGHT JOIN dict_flat d USING(key) ORDER BY s1.key;
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 ANTI RIGHT JOIN dict_flat d USING(key) ORDER BY s1.key;

SET join_use_nulls = 0;

SELECT 'issue 23002';

SET join_algorithm = 'auto';
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 LEFT JOIN dict_flat d ON s1.key = d.key ORDER BY s1.key;
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 ANY LEFT JOIN dict_flat d USING(key) ORDER BY key;
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 RIGHT JOIN dict_flat d ON s1.key = d.key ORDER BY d.key;

SET join_algorithm = 'partial_merge';
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 LEFT JOIN dict_flat d ON s1.key = d.key ORDER BY s1.key;
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(5)) s1 ANY LEFT JOIN dict_flat d USING(key) ORDER BY key;
SELECT '-';
SELECT * FROM (SELECT number AS key FROM numbers(2)) s1 RIGHT JOIN dict_flat d ON s1.key = d.key ORDER BY d.key;


DROP DICTIONARY dict_flat;
DROP DICTIONARY dict_hashed;
DROP DICTIONARY dict_complex_cache;

DROP TABLE t1;
DROP DATABASE IF EXISTS db_01115;
