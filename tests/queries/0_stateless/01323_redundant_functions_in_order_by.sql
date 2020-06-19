DROP DATABASE IF EXISTS db_01115;
CREATE DATABASE db_01115 Engine = Ordinary;

USE db_01115;

DROP DICTIONARY IF EXISTS dict_flat;

CREATE TABLE t1 (key UInt64, a UInt8, b String, c Float64) ENGINE = MergeTree() ORDER BY key;
INSERT INTO t1 SELECT number, number, toString(number), number from numbers(4);

CREATE DICTIONARY dict_flat (key UInt64 DEFAULT 0, a UInt8 DEFAULT 42, b String DEFAULT 'x', c Float64 DEFAULT 42.0)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 't1' PASSWORD '' DB 'db_01115'))
LIFETIME(MIN 1 MAX 10)
LAYOUT(FLAT());

set enable_debug_queries = 1;
set optimize_redundant_functions_in_order_by = 1;

SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN dict_flat d USING(key) ORDER BY s.key, d.key;
analyze SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
analyze SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
analyze SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN dict_flat d USING(key) ORDER BY s.key, d.key;

set optimize_redundant_functions_in_order_by = 0;

SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN dict_flat d USING(key) ORDER BY s.key, d.key;
analyze SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY x, exp(x));
analyze SELECT groupArray(x) from (SELECT number as x FROM numbers(3) ORDER BY exp(x), x);
analyze SELECT * FROM (SELECT number + 2 AS key FROM numbers(4)) s FULL JOIN dict_flat d USING(key) ORDER BY s.key, d.key;

DROP DICTIONARY dict_flat;

DROP TABLE t1;
DROP DATABASE IF EXISTS db_01115;
