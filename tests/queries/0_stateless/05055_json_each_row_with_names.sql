DROP TABLE IF EXISTS test_json_each_row_with_names;
CREATE TABLE test_json_each_row_with_names (id UInt32, name String) ENGINE = Memory;

INSERT INTO test_json_each_row_with_names VALUES (1, 'David'), (2, 'Julie');

SELECT '--- JSONEachRowWithNames output';
SELECT * FROM test_json_each_row_with_names ORDER BY id FORMAT JSONEachRowWithNames;

SELECT '--- JSONEachRowWithNamesAndTypes output';
SELECT * FROM test_json_each_row_with_names ORDER BY id FORMAT JSONEachRowWithNamesAndTypes;

SELECT '--- JSONStringsEachRowWithNames output';
SELECT * FROM test_json_each_row_with_names ORDER BY id FORMAT JSONStringsEachRowWithNames;

SELECT '--- JSONStringsEachRowWithNamesAndTypes output';
SELECT * FROM test_json_each_row_with_names ORDER BY id FORMAT JSONStringsEachRowWithNamesAndTypes;

SELECT '--- JSONEachRowWithNames input';
TRUNCATE TABLE test_json_each_row_with_names;
INSERT INTO test_json_each_row_with_names FORMAT JSONEachRowWithNames
["id", "name"]
{"id": 1, "name": "David"}
{"id": 2, "name": "Julie"};

SELECT * FROM test_json_each_row_with_names ORDER BY id;

SELECT '--- JSONEachRowWithNamesAndTypes input';
TRUNCATE TABLE test_json_each_row_with_names;
INSERT INTO test_json_each_row_with_names FORMAT JSONEachRowWithNamesAndTypes
["id", "name"]
["UInt32", "String"]
{"id": 1, "name": "David"}
{"id": 2, "name": "Julie"};

SELECT * FROM test_json_each_row_with_names ORDER BY id;

SELECT '--- JSONStringsEachRowWithNamesAndTypes input';
TRUNCATE TABLE test_json_each_row_with_names;
INSERT INTO test_json_each_row_with_names FORMAT JSONStringsEachRowWithNamesAndTypes
["id", "name"]
["UInt32", "String"]
{"id": "1", "name": "David"}
{"id": "2", "name": "Julie"};

SELECT * FROM test_json_each_row_with_names ORDER BY id;

SELECT '--- the data rows still carry the names, so a subset of columns can be read';
TRUNCATE TABLE test_json_each_row_with_names;
INSERT INTO test_json_each_row_with_names FORMAT JSONEachRowWithNames
["name"]
{"name": "David"}
{"name": "Julie"};

SELECT * FROM test_json_each_row_with_names ORDER BY name;

SELECT '--- the types from the header are checked against the destination';
TRUNCATE TABLE test_json_each_row_with_names;
INSERT INTO test_json_each_row_with_names FORMAT JSONEachRowWithNamesAndTypes
["id", "name"]
["String", "String"]
{"id": 1, "name": "David"}; -- { serverError INCORRECT_DATA }

SELECT '--- a header row with fewer types than names is rejected';
INSERT INTO test_json_each_row_with_names FORMAT JSONEachRowWithNamesAndTypes
["id", "name"]
["UInt32"]
{"id": 1, "name": "David"}; -- { serverError INCORRECT_DATA }

SELECT '--- schema inference takes the types from the header row';
DESCRIBE format(JSONEachRowWithNamesAndTypes, $$["id", "arr"]
["UInt32", "Array(UInt64)"]
{"id": 1, "arr": [1, 2]}
$$);

SELECT '--- without a types row the schema is inferred from the data rows';
DESCRIBE format(JSONEachRowWithNames, $$["id", "arr"]
{"id": 1, "arr": [1, 2]}
$$) SETTINGS schema_inference_make_columns_nullable = 1;

SELECT '--- parallel parsing splits object rows on their own boundaries';
DROP TABLE IF EXISTS test_json_each_row_with_names_file;
CREATE TABLE test_json_each_row_with_names_file (id UInt64, arr Array(UInt64))
ENGINE = File(JSONEachRowWithNamesAndTypes);

SELECT '--- a JSON array of rows cannot contain the header rows';
INSERT INTO FUNCTION file('05055_json_each_row_with_names.json', JSONEachRowWithNames)
SETTINGS output_format_json_array_of_rows = 1, engine_file_truncate_on_insert = 1
SELECT 1 AS x; -- { serverError BAD_ARGUMENTS }

INSERT INTO test_json_each_row_with_names_file SELECT number, [number, number + 1] FROM numbers(1000);

SELECT count(), sum(id), sum(arr[2]) FROM test_json_each_row_with_names_file
SETTINGS input_format_parallel_parsing = 1, min_chunk_bytes_for_parallel_parsing = 1, max_threads = 4;

DROP TABLE test_json_each_row_with_names_file;
DROP TABLE test_json_each_row_with_names;
