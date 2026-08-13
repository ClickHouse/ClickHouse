INSERT INTO FUNCTION file(currentDatabase() || '_04267_selective_read_json_compact_each_row.json', 'JSONCompactEachRow', 'c1 String, c2 UInt32, c3 String, c4 UInt32')
SELECT 'not_uint', 42, 'also_not_uint', 84
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2, c4
FROM file(currentDatabase() || '_04267_selective_read_json_compact_each_row.json', 'JSONCompactEachRow', 'c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32');

SELECT *
FROM file(currentDatabase() || '_04267_selective_read_json_compact_each_row.json', 'JSONCompactEachRow', 'c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }

INSERT INTO FUNCTION file(currentDatabase() || '_04267_selective_read_json_compact_strings_each_row.json', 'JSONCompactStringsEachRow', 'c1 String, c2 UInt32, c3 String, c4 UInt32')
SELECT 'not_uint', 42, 'also_not_uint', 84
SETTINGS engine_file_truncate_on_insert = 1;

SELECT c2, c4
FROM file(currentDatabase() || '_04267_selective_read_json_compact_strings_each_row.json', 'JSONCompactStringsEachRow', 'c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32');

SELECT *
FROM file(currentDatabase() || '_04267_selective_read_json_compact_strings_each_row.json', 'JSONCompactStringsEachRow', 'c1 UInt32, c2 UInt32, c3 UInt32, c4 UInt32'); -- { serverError UNEXPECTED_DATA_AFTER_PARSED_VALUE }
