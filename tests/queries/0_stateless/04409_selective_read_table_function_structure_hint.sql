DROP TABLE IF EXISTS test_04409;

INSERT INTO FUNCTION file(currentDatabase() || '_04409_selective_read.csv', 'CSV', 'c1 String, c2 UInt32')
SELECT 'not_uint', 42
SETTINGS engine_file_truncate_on_insert = 1;

CREATE TABLE test_04409 (c2 UInt32) ENGINE = Memory;

INSERT INTO test_04409
SELECT c2
FROM file(currentDatabase() || '_04409_selective_read.csv', 'CSV')
SETTINGS use_structure_from_insertion_table_in_table_functions = 2;

SELECT c2 FROM test_04409;

DROP TABLE test_04409;
