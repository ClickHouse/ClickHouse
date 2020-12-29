DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS test_table_2;
SELECT 1;
/* Check JSONStringsEachRow Output */
CREATE TABLE test_table (value UInt8, name String) ENGINE = MergeTree() ORDER BY value;
INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');
SELECT * FROM test_table FORMAT JSONStringsEachRow;
SELECT 2;
/* Check Totals */
SELECT name, count() AS c FROM test_table GROUP BY name WITH TOTALS ORDER BY name FORMAT JSONStringsEachRow;
SELECT 3;
/* Check JSONStringsEachRowWithProgress Output */
SELECT 1 as a FROM system.one FORMAT JSONStringsEachRowWithProgress;
SELECT 4;
/* Check Totals */
SELECT 1 as a FROM system.one GROUP BY a WITH TOTALS ORDER BY a FORMAT JSONStringsEachRowWithProgress;
DROP TABLE IF EXISTS test_table;
SELECT 5;
/* Check JSONStringsEachRow Input */
CREATE TABLE test_table (v1 String, v2 UInt8, v3 DEFAULT v2 * 16, v4 UInt8 DEFAULT 8) ENGINE = MergeTree() ORDER BY v2;
INSERT INTO test_table FORMAT JSONStringsEachRow {"v1": "first", "v2": "1", "v3": "2", "v4": "NULL"} {"v1": "second", "v2": "2", "v3": "null", "v4": "6"};
SELECT * FROM test_table FORMAT JSONStringsEachRow;
TRUNCATE TABLE test_table;
SELECT 6;
/* Check input_format_null_as_default = 1 */
SET input_format_null_as_default = 1;
INSERT INTO test_table FORMAT JSONStringsEachRow {"v1": "first", "v2": "1", "v3": "2", "v4": "ᴺᵁᴸᴸ"} {"v1": "second", "v2": "2", "v3": "null", "v4": "6"};
SELECT * FROM test_table FORMAT JSONStringsEachRow;
TRUNCATE TABLE test_table;
SELECT 7;
/* Check Nested */
CREATE TABLE test_table_2 (v1 UInt8, n Nested(id UInt8, name String)) ENGINE = MergeTree() ORDER BY v1;
INSERT INTO test_table_2 FORMAT JSONStringsEachRow {"v1": "16", "n.id": "[15, 16, 17]", "n.name": "['first', 'second', 'third']"};
SELECT * FROM test_table_2 FORMAT JSONStringsEachRow;
TRUNCATE TABLE test_table_2;

DROP TABLE IF EXISTS test_table;
DROP TABLE IF EXISTS test_table_2;
