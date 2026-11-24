-- Tags: no-async-insert

-- Note on `error N` vs `clientError N` annotations below:
-- Ambiguous-JSON-field detection raises error 117 (`INCORRECT_DATA`). The error can be
-- reported client-side (legacy path, when `send_table_structure_on_insert_with_inline_data` = 1
-- the client converts `FORMAT JSONEachRow` data to a native block before sending) or
-- server-side (inline path, setting = 0, the server parses the inline JSON directly).
-- The annotations below use `error 117` rather than the stricter `clientError 117` to
-- accept both reporting paths. If a future maintainer pins this test to
-- `send_table_structure_on_insert_with_inline_data` = 1, the stricter `clientError 117`
-- can be restored.

-- Test auto case
DROP TABLE IF EXISTS json_test;

CREATE TABLE json_test (id Int, name String, NaMe String);

SET input_format_column_name_matching_mode='auto';

INSERT INTO json_test FORMAT JSONEachRow {"id": 0, "name": "aa", "NaMe": "bb"}

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test auto case ambiguity
CREATE TABLE json_test (age Int, AGE Int);

SET input_format_column_name_matching_mode='auto';

INSERT INTO json_test FORMAT JSONEachRow {"age": 0, "AGE": 10};

INSERT INTO json_test FORMAT JSONEachRow {"AgE": 1, "aGe": 20}; -- { error 117 }

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test auto case -- corner case
CREATE TABLE json_test (age Int, AGE Int);

SET input_format_column_name_matching_mode='auto';

INSERT INTO json_test FORMAT JSONEachRow {"age": 0, "AGE": 10};

INSERT INTO json_test FORMAT JSONEachRow {"AGE": 10, "age": 0};

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test match case
CREATE TABLE json_test (age Int, AGE Int, name String, NaMe String);

SET input_format_column_name_matching_mode='match_case';

INSERT INTO json_test FORMAT JSONEachRow {"age": 0, "AGE": 10, "name": "a", "NaMe": "b"} {"name": "aa", "age": 1, "NaMe": "bb", "AGE": 20};

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test ignore case
CREATE TABLE json_test (id Int, age Int);

SET input_format_column_name_matching_mode='ignore_case';

INSERT INTO json_test FORMAT JSONEachRow {"ID": 0, "AGE": 10} {"Id": 1, "AgE": 100};

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test ignore case ambiguity
CREATE TABLE json_test (AGE Int, age Int, id Int);

SET input_format_column_name_matching_mode='ignore_case';

INSERT INTO json_test FORMAT JSONEachRow {"age": 0, "AGE": 10}; -- { error 117 }

INSERT INTO json_test FORMAT JSONEachRow {"id": 1000};

INSERT INTO json_test FORMAT JSONEachRow {"id": 0, "age": 10}; -- { error 117 }

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test two input columns that map to the same table column (auto case match):
-- the exactly matching key wins over the case-insensitively matching one, regardless of the order.
CREATE TABLE json_test (id Int);

SET input_format_column_name_matching_mode='auto';
SET input_format_json_ignore_unnecessary_fields=false;

INSERT INTO json_test FORMAT JSONEachRow {"ID": 444, "id": 123};
INSERT INTO json_test FORMAT JSONEachRow {"id": 124, "ID": 444};

SET input_format_json_ignore_unnecessary_fields=true;

INSERT INTO json_test FORMAT JSONEachRow {"ID": 444, "id": 125};
INSERT INTO json_test FORMAT JSONEachRow {"id": 126, "ID": 444};

-- Two keys that match the same table column only case-insensitively are ambiguous
INSERT INTO json_test FORMAT JSONEachRow {"ID": 444, "Id": 555}; -- { error 117 }

SET input_format_json_ignore_unnecessary_fields=false;

INSERT INTO json_test FORMAT JSONEachRow {"ID": 444, "Id": 555}; -- { error 117 }

SELECT * FROM json_test ORDER BY id;

DROP TABLE json_test;

-- Test reading a subset of columns from a file with two keys that differ only in case (auto case match)
SET input_format_column_name_matching_mode='auto';
SET input_format_json_ignore_unnecessary_fields=true;

INSERT INTO FUNCTION file(currentDatabase() || '_04027.jsonl', 'LineAsString', 'x String') SELECT '{"a":1,"A":2}' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_04027_rev.jsonl', 'LineAsString', 'x String') SELECT '{"A":2,"a":1}' SETTINGS engine_file_truncate_on_insert = 1;

SELECT a, A FROM file(currentDatabase() || '_04027.jsonl', 'JSONEachRow') ORDER BY ALL;
SELECT A FROM file(currentDatabase() || '_04027.jsonl', 'JSONEachRow') ORDER BY ALL;
SELECT a FROM file(currentDatabase() || '_04027.jsonl', 'JSONEachRow') ORDER BY ALL;
SELECT count() FROM file(currentDatabase() || '_04027.jsonl', 'JSONEachRow') WHERE A = 2;
SELECT A FROM file(currentDatabase() || '_04027_rev.jsonl', 'JSONEachRow') ORDER BY ALL;
SELECT a FROM file(currentDatabase() || '_04027_rev.jsonl', 'JSONEachRow') ORDER BY ALL;
SELECT A FROM file(currentDatabase() || '_04027.jsonl', 'JSONEachRow') ORDER BY ALL SETTINGS input_format_json_ignore_unnecessary_fields = 0;
SELECT A FROM file(currentDatabase() || '_04027.jsonl', 'JSONEachRow') ORDER BY ALL SETTINGS input_format_json_ignore_unnecessary_fields = 0, input_format_skip_duplicated_fields = 1;
SELECT a FROM file(currentDatabase() || '_04027_rev.jsonl', 'JSONEachRow') ORDER BY ALL SETTINGS input_format_json_ignore_unnecessary_fields = 0;

-- Test ambiguity when two input columns map to the same table column (ignore case)
CREATE TABLE json_test (id Int);

SET input_format_column_name_matching_mode='ignore_case';
SET input_format_json_ignore_unnecessary_fields=false;

INSERT INTO json_test FORMAT JSONEachRow {"ID": 444, "id": 123}; -- { error 117 }

SET input_format_json_ignore_unnecessary_fields=true;

INSERT INTO json_test FORMAT JSONEachRow {"ID": 444, "id": 123};

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test ambiguity with nested fields (auto)

CREATE TABLE json_test (id Int, user Tuple(age Int), USER Tuple(name String));

SET input_format_column_name_matching_mode='auto';

INSERT INTO json_test FORMAT JSONEachRow {"ID": 0, "user": {"age": 20}, "USER": {"name": "bbbb"}};

INSERT INTO json_test FORMAT JSONEachRow {"ID": 0, "uSeR": {"age": 25}, "UsEr": {"name": "dddd"}}; -- { error 117 }

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test ambiguity with nested fields (ignore_case)

CREATE TABLE json_test (id Int, user Tuple(age Int, name String));

SET input_format_column_name_matching_mode='ignore_case';

INSERT INTO json_test FORMAT JSONEachRow {"ID": 0, "user": {"age": 20, "name": "Alfred"}};

SELECT * FROM json_test;

DROP TABLE json_test;

CREATE TABLE json_test (user Tuple(age Int, name String), UsEr Tuple(age Int, name String));

SET input_format_column_name_matching_mode='ignore_case';

INSERT INTO json_test FORMAT JSONEachRow {"user": {"age": 20, "name": "Alfred"}}; -- { error 117 }

SELECT * FROM json_test;

DROP TABLE json_test;

-- Test ambiguity with nested fields (auto, input_format_import_nested_json=1)

CREATE TABLE json_test (id Int, "user.age" Int, "user.name" String);

SET input_format_column_name_matching_mode='auto';
SET input_format_import_nested_json=1;

INSERT INTO json_test FORMAT JSONEachRow {"ID": 1, "user": {"aGe": 55, "NAME": "fffff"}};

INSERT INTO json_test FORMAT JSONEachRow {"id": 2, "USER": {"AGE": 65, "NAME": "ggggg"}};

-- The exactly matching `user` wins over the case-insensitively matching `USER`
INSERT INTO json_test FORMAT JSONEachRow {"user": {"age": 20, "name": "Alfred"}, "USER": {"age": 30, "name": "Elias"}};

INSERT INTO json_test FORMAT JSONEachRow {"id": 3, "USER": {"age": 30, "name": "Elias"}, "user": {"age": 40, "name": "Frank"}};

SELECT * FROM json_test ORDER BY id;

DROP TABLE json_test;

-- Test ambiguity with nested fields (ignore_case, input_format_import_nested_json=1)

CREATE TABLE json_test (id Int, "user.age" Int, "user.name" String);

SET input_format_column_name_matching_mode='ignore_case';
SET input_format_import_nested_json=1;

INSERT INTO json_test FORMAT JSONEachRow {"ID": 1, "USER": {"aGe": 20, "NAME": "Alfred"}};

INSERT INTO json_test FORMAT JSONEachRow {"user": {"age": 20, "name": "Alfred"}, "USER": {"age": 30, "name": "Elias"}}; -- { error 117 }

SELECT * FROM json_test;

DROP TABLE json_test;
