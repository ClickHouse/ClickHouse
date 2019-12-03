SELECT '==== TEST JSONCompactEachRow OUTPUT ====';
DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS test_table_2;
CREATE TABLE test_table (value UInt8, name String) ENGINE = MergeTree() ORDER BY value;
INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM test_table FORMAT JSONCompactEachRow;
/* Check Totals */
SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONCompactEachRow;

SELECT '==== TEST JSONCompactEachRowWithNamesAndTypes OUTPUT ====';
SELECT * FROM test_table FORMAT JSONCompactEachRowWithNamesAndTypes;
/* Check Totals */
SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONCompactEachRowWithNamesAndTypes;
DROP TABLE IF EXISTS test_table;

SELECT '==== TEST JSONCompactEachRow INPUT ====';
SET input_format_null_as_default = 0;
CREATE TABLE test_table (v1 String, v2 UInt8, v3 DEFAULT v2 * 2, v4 UInt8 DEFAULT 8) ENGINE = MergeTree() ORDER BY v2;
INSERT INTO test_table FORMAT JSONCompactEachRow ["first", 1, "2", null] ["second", 2, null, 6];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
TRUNCATE TABLE test_table;
/* Check input_format_null_as_default = 1 */
SET input_format_null_as_default = 1;
INSERT INTO test_table FORMAT JSONCompactEachRow ["first", 1, "2", null] ["second", 2, null, 6];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
/* Check Nested */
CREATE TABLE test_table_2 (v1 UInt8, n Nested(id UInt8, name String)) ENGINE = MergeTree() ORDER BY v1;
INSERT INTO test_table_2 FORMAT JSONCompactEachRow [16, [15, 16, null], ["first", "second", "third"]];
SELECT * FROM test_table_2 FORMAT JSONCompactEachRowWithNamesAndTypes;
TRUNCATE TABLE test_table_2;
SELECT '==== TEST JSONCompactEachRowWithNamesAndTypes INPUT ====';
SET input_format_null_as_default = 0;
INSERT INTO test_table FORMAT JSONCompactEachRowWithNamesAndTypes ["v1", "v2", "v3", "v4"]["String","UInt8","UInt16","UInt8"]["first", 1, "2", null]["second", 2, null, 6];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
TRUNCATE TABLE test_table;
/* Check input_format_null_as_default = 1 */
/*
SET input_format_null_as_default = 1;
INSERT INTO test_table FORMAT JSONCompactEachRowWithNamesAndTypes ["v1", "v2", "v3", "v4"]["String","UInt8","UInt16","UInt8"]["first", 1, "2", null] ["second", 2, null, 6];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
*/
/* Check Header */
TRUNCATE TABLE test_table;
SET input_format_skip_unknown_fields = 1;
INSERT INTO test_table FORMAT JSONCompactEachRowWithNamesAndTypes ["v1", "v2", "invalid_column"]["String", "UInt8", "UInt8"]["first", 1, 32]["second", 2, "64"];
SELECT * FROM test_table FORMAT JSONCompactEachRow;
/* Check Nested */
INSERT INTO test_table_2 FORMAT JSONCompactEachRowWithNamesAndTypes ["v1", "n.id", "n.name"]["UInt8", "Array(UInt8)", "Array(String)"][16, [15, 16, null], ["first", "second", "third"]];
SELECT * FROM test_table_2 FORMAT JSONCompactEachRowWithNamesAndTypes;

DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS test_table_2;
