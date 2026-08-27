DROP TABLE IF EXISTS test_json_each_row_with_names;
CREATE TABLE test_json_each_row_with_names (id UInt32, name String) ENGINE = Memory();

-- Check JSONEachRowWithNames output
INSERT INTO test_json_each_row_with_names VALUES (1, 'David'), (2, 'Julie');
SELECT * FROM test_json_each_row_with_names ORDER BY id FORMAT JSONEachRowWithNames;

SELECT '----------';

-- Check JSONEachRowWithNamesAndTypes output
SELECT * FROM test_json_each_row_with_names ORDER BY id FORMAT JSONEachRowWithNamesAndTypes;

SELECT '----------';

-- Check JSONEachRowWithNames input
TRUNCATE TABLE test_json_each_row_with_names;
INSERT INTO test_json_each_row_with_names FORMAT JSONEachRowWithNames
["id", "name"]
{"id": 1, "name": "David"}
{"id": 2, "name": "Julie"};

SELECT * FROM test_json_each_row_with_names ORDER BY id;

SELECT '----------';

-- Check JSONEachRowWithNamesAndTypes input
TRUNCATE TABLE test_json_each_row_with_names;
INSERT INTO test_json_each_row_with_names FORMAT JSONEachRowWithNamesAndTypes
["id", "name"]
["UInt32", "String"]
{"id": 1, "name": "David"}
{"id": 2, "name": "Julie"};

SELECT * FROM test_json_each_row_with_names ORDER BY id;

SELECT '----------';

-- Check type validation
TRUNCATE TABLE test_json_each_row_with_names;
INSERT INTO test_json_each_row_with_names FORMAT JSONEachRowWithNamesAndTypes
["id", "name"]
["String", "String"]
{"id": 1, "name": "David"}; -- { serverError INCORRECT_DATA }

DROP TABLE test_json_each_row_with_names;
