SELECT 'interpret_expressions=0';
SET input_format_values_interpret_expressions = 0;

SELECT 'Map(String, Float32) - basic insert and order by key';
DROP TABLE IF EXISTS t_map_v0;
CREATE TABLE t_map_v0 (m Map(String, Float32)) ENGINE = Memory;
INSERT INTO t_map_v0 VALUES ('{\'key1\':1, \'key2\':10}');
INSERT INTO t_map_v0 VALUES ('{\'key1\':2, \'key2\':20}');
SELECT m FROM t_map_v0 ORDER BY m['key1'];
DROP TABLE t_map_v0;

SELECT 'Map with nested quoting (key contains apostrophe)';
DROP TABLE IF EXISTS t_map_quote_v0;
CREATE TABLE t_map_quote_v0 (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_map_quote_v0 VALUES ('{\'it\\\'s\':1}');
SELECT m FROM t_map_quote_v0;
DROP TABLE t_map_quote_v0;

SELECT 'Empty Map';
DROP TABLE IF EXISTS t_map_empty_v0;
CREATE TABLE t_map_empty_v0 (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_map_empty_v0 VALUES ('{}');
SELECT m FROM t_map_empty_v0;
DROP TABLE t_map_empty_v0;

SELECT 'Array(UInt32) - with empty array';
DROP TABLE IF EXISTS t_arr_v0;
CREATE TABLE t_arr_v0 (a Array(UInt32)) ENGINE = Memory;
INSERT INTO t_arr_v0 VALUES ('[1,2,3]');
INSERT INTO t_arr_v0 VALUES ('[]');
SELECT a FROM t_arr_v0 ORDER BY length(a);
DROP TABLE t_arr_v0;

SELECT 'Tuple(String, UInt32)';
DROP TABLE IF EXISTS t_tup_v0;
CREATE TABLE t_tup_v0 (t Tuple(String, UInt32)) ENGINE = Memory;
INSERT INTO t_tup_v0 VALUES ('(\'hello\', 42)');
SELECT t FROM t_tup_v0;
DROP TABLE t_tup_v0;

SELECT 'Multiple columns: one complex, one simple';
DROP TABLE IF EXISTS t_multi_v0;
CREATE TABLE t_multi_v0 (id UInt32, m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_multi_v0 VALUES (1, '{\'a\':1}');
INSERT INTO t_multi_v0 VALUES (2, '{\'b\':2}');
SELECT id, m FROM t_multi_v0 ORDER BY id;
DROP TABLE t_multi_v0;

SELECT 'Array of Maps (nested complex types)';
DROP TABLE IF EXISTS t_arr_map_v0;
CREATE TABLE t_arr_map_v0 (a Array(Map(String, UInt32))) ENGINE = Memory;
INSERT INTO t_arr_map_v0 VALUES ('[{\'a\':1}, {\'b\':2}]');
SELECT a FROM t_arr_map_v0;
DROP TABLE t_arr_map_v0;

SELECT 'Map with Array values (nested complex types)';
DROP TABLE IF EXISTS t_map_arr_v0;
CREATE TABLE t_map_arr_v0 (m Map(String, Array(UInt32))) ENGINE = Memory;
INSERT INTO t_map_arr_v0 VALUES ('{\'key\':[1,2,3]}');
SELECT m FROM t_map_arr_v0;
DROP TABLE t_map_arr_v0;

SELECT 'Map with LowCardinality key type';
DROP TABLE IF EXISTS t_map_lc_v0;
CREATE TABLE t_map_lc_v0 (m Map(LowCardinality(String), UInt32)) ENGINE = Memory;
INSERT INTO t_map_lc_v0 VALUES ('{\'a\':1}'), ('{\'b\':2}');
SELECT m FROM t_map_lc_v0 ORDER BY toString(m);
DROP TABLE t_map_lc_v0;

SELECT 'Nested (Array(Tuple(...)) internally)';
DROP TABLE IF EXISTS t_nested_v0;
CREATE TABLE t_nested_v0 (n Nested(key String, value UInt32)) ENGINE = Memory;
INSERT INTO t_nested_v0 VALUES ('[\'a\',\'b\']', '[1,2]');
SELECT n.key, n.value FROM t_nested_v0;
DROP TABLE t_nested_v0;

SELECT 'Multiple rows in single INSERT (batch)';
DROP TABLE IF EXISTS t_batch_v0;
CREATE TABLE t_batch_v0 (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_batch_v0 VALUES ('{\'a\':1}'), ('{\'b\':2}'), ('{\'c\':3}');
SELECT m FROM t_batch_v0 ORDER BY toString(m);
DROP TABLE t_batch_v0;

SELECT 'SQL-style doubled single quote escape (Map)';
DROP TABLE IF EXISTS t_map_sql_v0;
CREATE TABLE t_map_sql_v0 (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_map_sql_v0 VALUES ('{''key1'':1, ''key2'':2}');
SELECT m FROM t_map_sql_v0;
DROP TABLE t_map_sql_v0;

SELECT 'SQL-style doubled single quote escape (Array)';
DROP TABLE IF EXISTS t_arr_sql_v0;
CREATE TABLE t_arr_sql_v0 (a Array(String)) ENGINE = Memory;
INSERT INTO t_arr_sql_v0 VALUES ('[''a'', ''b'', ''c'']');
SELECT a FROM t_arr_sql_v0;
DROP TABLE t_arr_sql_v0;

SELECT 'SQL-style doubled single quote escape (Tuple)';
DROP TABLE IF EXISTS t_tup_sql_v0;
CREATE TABLE t_tup_sql_v0 (t Tuple(String, UInt32)) ENGINE = Memory;
INSERT INTO t_tup_sql_v0 VALUES ('(''hello'', 42)');
SELECT t FROM t_tup_sql_v0;
DROP TABLE t_tup_sql_v0;

-- ============================================================
SELECT 'interpret_expressions=1';
SET input_format_values_interpret_expressions = 1;

SELECT 'Map(String, Float32) - basic insert and order by key';
DROP TABLE IF EXISTS t_map_v1;
CREATE TABLE t_map_v1 (m Map(String, Float32)) ENGINE = Memory;
INSERT INTO t_map_v1 VALUES ('{\'key1\':1, \'key2\':10}');
INSERT INTO t_map_v1 VALUES ('{\'key1\':2, \'key2\':20}');
SELECT m FROM t_map_v1 ORDER BY m['key1'];
DROP TABLE t_map_v1;

SELECT 'Map with nested quoting (key contains apostrophe)';
DROP TABLE IF EXISTS t_map_quote_v1;
CREATE TABLE t_map_quote_v1 (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_map_quote_v1 VALUES ('{\'it\\\'s\':1}');
SELECT m FROM t_map_quote_v1;
DROP TABLE t_map_quote_v1;

SELECT 'Empty Map';
DROP TABLE IF EXISTS t_map_empty_v1;
CREATE TABLE t_map_empty_v1 (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_map_empty_v1 VALUES ('{}');
SELECT m FROM t_map_empty_v1;
DROP TABLE t_map_empty_v1;

SELECT 'Array(UInt32) - with empty array';
DROP TABLE IF EXISTS t_arr_v1;
CREATE TABLE t_arr_v1 (a Array(UInt32)) ENGINE = Memory;
INSERT INTO t_arr_v1 VALUES ('[1,2,3]');
INSERT INTO t_arr_v1 VALUES ('[]');
SELECT a FROM t_arr_v1 ORDER BY length(a);
DROP TABLE t_arr_v1;

SELECT 'Tuple(String, UInt32)';
DROP TABLE IF EXISTS t_tup_v1;
CREATE TABLE t_tup_v1 (t Tuple(String, UInt32)) ENGINE = Memory;
INSERT INTO t_tup_v1 VALUES ('(\'hello\', 42)');
SELECT t FROM t_tup_v1;
DROP TABLE t_tup_v1;

SELECT 'Multiple columns: one complex, one simple';
DROP TABLE IF EXISTS t_multi_v1;
CREATE TABLE t_multi_v1 (id UInt32, m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_multi_v1 VALUES (1, '{\'a\':1}');
INSERT INTO t_multi_v1 VALUES (2, '{\'b\':2}');
SELECT id, m FROM t_multi_v1 ORDER BY id;
DROP TABLE t_multi_v1;

SELECT 'Array of Maps (nested complex types)';
DROP TABLE IF EXISTS t_arr_map_v1;
CREATE TABLE t_arr_map_v1 (a Array(Map(String, UInt32))) ENGINE = Memory;
INSERT INTO t_arr_map_v1 VALUES ('[{\'a\':1}, {\'b\':2}]');
SELECT a FROM t_arr_map_v1;
DROP TABLE t_arr_map_v1;

SELECT 'Map with Array values (nested complex types)';
DROP TABLE IF EXISTS t_map_arr_v1;
CREATE TABLE t_map_arr_v1 (m Map(String, Array(UInt32))) ENGINE = Memory;
INSERT INTO t_map_arr_v1 VALUES ('{\'key\':[1,2,3]}');
SELECT m FROM t_map_arr_v1;
DROP TABLE t_map_arr_v1;

SELECT 'Map with LowCardinality key type';
DROP TABLE IF EXISTS t_map_lc_v1;
CREATE TABLE t_map_lc_v1 (m Map(LowCardinality(String), UInt32)) ENGINE = Memory;
INSERT INTO t_map_lc_v1 VALUES ('{\'a\':1}'), ('{\'b\':2}');
SELECT m FROM t_map_lc_v1 ORDER BY toString(m);
DROP TABLE t_map_lc_v1;

SELECT 'Nested (Array(Tuple(...)) internally)';
DROP TABLE IF EXISTS t_nested_v1;
CREATE TABLE t_nested_v1 (n Nested(key String, value UInt32)) ENGINE = Memory;
INSERT INTO t_nested_v1 VALUES ('[\'a\',\'b\']', '[1,2]');
SELECT n.key, n.value FROM t_nested_v1;
DROP TABLE t_nested_v1;

SELECT 'Multiple rows in single INSERT (batch)';
DROP TABLE IF EXISTS t_batch_v1;
CREATE TABLE t_batch_v1 (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_batch_v1 VALUES ('{\'a\':1}'), ('{\'b\':2}'), ('{\'c\':3}');
SELECT m FROM t_batch_v1 ORDER BY toString(m);
DROP TABLE t_batch_v1;

SELECT 'SQL-style doubled single quote escape (Map)';
DROP TABLE IF EXISTS t_map_sql_v1;
CREATE TABLE t_map_sql_v1 (m Map(String, UInt32)) ENGINE = Memory;
INSERT INTO t_map_sql_v1 VALUES ('{''key1'':1, ''key2'':2}');
SELECT m FROM t_map_sql_v1;
DROP TABLE t_map_sql_v1;

SELECT 'SQL-style doubled single quote escape (Array)';
DROP TABLE IF EXISTS t_arr_sql_v1;
CREATE TABLE t_arr_sql_v1 (a Array(String)) ENGINE = Memory;
INSERT INTO t_arr_sql_v1 VALUES ('[''a'', ''b'', ''c'']');
SELECT a FROM t_arr_sql_v1;
DROP TABLE t_arr_sql_v1;

SELECT 'SQL-style doubled single quote escape (Tuple)';
DROP TABLE IF EXISTS t_tup_sql_v1;
CREATE TABLE t_tup_sql_v1 (t Tuple(String, UInt32)) ENGINE = Memory;
INSERT INTO t_tup_sql_v1 VALUES ('(''hello'', 42)');
SELECT t FROM t_tup_sql_v1;
DROP TABLE t_tup_sql_v1;

-- ============================================================
SELECT 'Native syntax - verify no regression';
DROP TABLE IF EXISTS t_native;
CREATE TABLE t_native (m Map(String, Float32)) ENGINE = Memory;
INSERT INTO t_native VALUES ({'key1':1, 'key2':10});
SELECT m FROM t_native;
DROP TABLE t_native;
