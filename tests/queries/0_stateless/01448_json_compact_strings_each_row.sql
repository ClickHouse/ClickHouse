-- Tags: no-fasttest

DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS test_table_2;
SET input_format_null_as_default = 0;
SELECT 1;
/* Check JSONCompactStringsEachRow Output */
CREATE TABLE test_table (value UInt8, name String) ENGINE = MergeTree() ORDER BY value;
INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM test_table FORMAT JSONCompactStringsEachRow;
SELECT 2;
/* Check Totals */
SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONCompactStringsEachRow;
SELECT 3;
/* Check JSONCompactStringsEachRowWithNames and JSONCompactStringsEachRowWithNamesAndTypes Output */
SELECT * FROM test_table FORMAT JSONCompactStringsEachRowWithNamesAndTypes;
SELECT '----------';
SELECT * FROM test_table FORMAT JSONCompactStringsEachRowWithNames;
SELECT 4;
/* Check Totals */
SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONCompactStringsEachRowWithNamesAndTypes;
DROP TABLE IF EXISTS test_table;
SELECT 5;
/* Check JSONCompactStringsEachRow Input */
CREATE TABLE test_table (v1 String, v2 UInt8, v3 DEFAULT v2 * 16, v4 UInt8 DEFAULT 8) ENGINE = MergeTree() ORDER BY v2;
INSERT INTO test_table FORMAT JSONCompactStringsEachRow ["first", "1", "2", "3"] ["second", "2", "3", "6"];

SELECT * FROM test_table FORMAT JSONCompactStringsEachRow;
TRUNCATE TABLE test_table;
SELECT 6;
/* Check input_format_null_as_default = 1 */
SET input_format_null_as_default = 1;
INSERT INTO test_table FORMAT JSONCompactStringsEachRow ["first", "1", "2", "ᴺᵁᴸᴸ"] ["second", "2", "null", "6"];

SELECT * FROM test_table FORMAT JSONCompactStringsEachRow;
TRUNCATE TABLE test_table;
SELECT 7;
/* Check Nested */
CREATE TABLE test_table_2 (v1 UInt8, n Nested(id UInt8, name String)) ENGINE = MergeTree() ORDER BY v1;
INSERT INTO test_table_2 FORMAT JSONCompactStringsEachRow ["16", "[15, 16, 17]", "['first', 'second', 'third']"];

SELECT * FROM test_table_2 FORMAT JSONCompactStringsEachRow;
TRUNCATE TABLE test_table_2;
SELECT 8;
/* Check JSONCompactStringsEachRowWithNames and JSONCompactStringsEachRowWithNamesAndTypes Input */
SET input_format_null_as_default = 0;
INSERT INTO test_table FORMAT JSONCompactStringsEachRowWithNamesAndTypes ["v1", "v2", "v3", "v4"]["String","UInt8","UInt16","UInt8"]["first", "1", "2", "3"]["second", "2", "3", "6"];

INSERT INTO test_table FORMAT JSONCompactStringsEachRowWithNames ["v1", "v2", "v3", "v4"]["first", "1", "2", "3"]["second", "2", "3", "6"];

SELECT * FROM test_table FORMAT JSONCompactStringsEachRow;
TRUNCATE TABLE test_table;
SELECT 9;
/* Check input_format_null_as_default = 1 */
SET input_format_null_as_default = 1;
INSERT INTO test_table FORMAT JSONCompactStringsEachRowWithNamesAndTypes ["v1", "v2", "v3", "v4"]["String","UInt8","UInt16","UInt8"]["first", "1", "2", "null"] ["second", "2", "null", "6"];

INSERT INTO test_table FORMAT JSONCompactStringsEachRowWithNames ["v1", "v2", "v3", "v4"]["first", "1", "2", "null"] ["second", "2", "null", "6"];

SELECT * FROM test_table FORMAT JSONCompactStringsEachRow;
SELECT 10;
/* Check Header */
TRUNCATE TABLE test_table;
SET input_format_skip_unknown_fields = 1;
INSERT INTO test_table FORMAT JSONCompactStringsEachRowWithNamesAndTypes ["v1", "v2", "invalid_column"]["String", "UInt8", "UInt8"]["first", "1", "32"]["second", "2", "64"];

INSERT INTO test_table FORMAT JSONCompactStringsEachRowWithNames ["v1", "v2", "invalid_column"]["first", "1", "32"]["second", "2", "64"];

SELECT * FROM test_table FORMAT JSONCompactStringsEachRow;
SELECT 11;
TRUNCATE TABLE test_table;
INSERT INTO test_table FORMAT JSONCompactStringsEachRowWithNamesAndTypes ["v4", "v2", "v3"]["UInt8", "UInt8", "UInt16"]["1", "2", "3"]

INSERT INTO test_table FORMAT JSONCompactStringsEachRowWithNames ["v4", "v2", "v3"]["1", "2", "3"]

SELECT * FROM test_table FORMAT JSONCompactStringsEachRowWithNamesAndTypes;
SELECT '---------';
SELECT * FROM test_table FORMAT JSONCompactStringsEachRowWithNames;
SELECT 12;
/* Check Nested */
INSERT INTO test_table_2 FORMAT JSONCompactStringsEachRowWithNamesAndTypes ["v1", "n.id", "n.name"]["UInt8", "Array(UInt8)", "Array(String)"]["16", "[15, 16, 17]", "['first', 'second', 'third']"];

INSERT INTO test_table_2 FORMAT JSONCompactStringsEachRowWithNames ["v1", "n.id", "n.name"]["16", "[15, 16, 17]", "['first', 'second', 'third']"];

SELECT * FROM test_table_2 FORMAT JSONCompactStringsEachRowWithNamesAndTypes;
SELECT '---------';
SELECT * FROM test_table_2 FORMAT JSONCompactStringsEachRowWithNames;

DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS test_table_2;
