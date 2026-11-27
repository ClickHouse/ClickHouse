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

-- Test ambiguity when two input columns map to the same table column (auto case match)
CREATE TABLE json_test (id Int);

SET input_format_column_name_matching_mode='auto';
SET input_format_json_ignore_unnecessary_fields=false;

INSERT INTO json_test FORMAT JSONEachRow {"ID": 444, "id": 123}; -- { error 117 }

SET input_format_json_ignore_unnecessary_fields=true;

INSERT INTO json_test FORMAT JSONEachRow {"ID": 444, "id": 123};

SELECT * FROM json_test;

DROP TABLE json_test;

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

INSERT INTO json_test FORMAT JSONEachRow {"user": {"age": 20, "name": "Alfred"}, "USER": {"age": 30, "name": "Elias"}}; -- { error 117 }

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
