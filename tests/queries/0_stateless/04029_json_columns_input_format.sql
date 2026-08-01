-- Tags: no-async-insert

-- Test auto case
DROP TABLE IF EXISTS test;

CREATE TABLE test (id Int, ID Int, NAME String, NaMe String);

SET input_format_column_name_matching_mode='auto';

INSERT INTO test FORMAT JSONColumns {
    "id": [0, 1, 2],
    "ID": [10, 20, 30],
    "NAME": ["a", "b", "c"],
    "NaMe": ["yy", "zz", "xx"]
}

SELECT * FROM test;

DROP TABLE test;

-- Test ambiguity for automatic column name matching
DROP TABLE IF EXISTS test;

CREATE TABLE test (id Int, iD Int, name String, nAmE String);

SET input_format_column_name_matching_mode='auto';

INSERT INTO test FORMAT JSONColumns {
    "id": [0, 1, 2],
    "ID": [10, 20, 30],
    "NAME": ["a", "b", "c"],
    "NaMe": ["yy", "zz", "xx"]
} -- { clientError 117 }

SELECT * FROM test;

DROP TABLE test;

-- Test match case
DROP TABLE IF EXISTS test;

CREATE TABLE test (id Int, ID Int, NAME String, NaMe String);

SET input_format_column_name_matching_mode='match_case';

INSERT INTO test FORMAT JSONColumns{
    "id": [0, 1, 2],
    "ID": [10, 20, 30],
    "NAME": ["a", "b", "c"],
    "NaMe": ["yy", "zz", "xx"],
    "not_used": ["55", "55", "55"]
}

SELECT * FROM test;

DROP TABLE test;

-- Test ignore case
DROP TABLE IF EXISTS test;

CREATE TABLE test (ID Int, name String);

SET input_format_column_name_matching_mode='ignore_case';

INSERT INTO test FORMAT JSONColumns{
    "id": [0, 1, 2],
    "NAME": ["a", "b", "c"]
}

SELECT * FROM test;

DROP TABLE test;

-- Test ambiguity for ignore case column name matching
DROP TABLE IF EXISTS test;

CREATE TABLE test (id Int, ID Int, NAME String, NaMe String);

SET input_format_column_name_matching_mode='ignore_case';

INSERT INTO test FORMAT JSONColumns {
    "id": [0, 1, 2],
    "ID": [10, 20, 30],
    "NAME": ["a", "b", "c"],
    "NaMe": ["yy", "zz", "xx"]
}; -- { clientError 117 }

SELECT * FROM test;

DROP TABLE test;

-- Test ambiguity when two input columns map to the same table column (auto case match)
DROP TABLE IF EXISTS test;

CREATE TABLE test (id Int);

SET input_format_column_name_matching_mode='auto';

INSERT INTO test FORMAT JSONColumns {
    "id": [0, 1, 2],
    "ID": [10, 20, 30]
} -- { clientError 117 }

SELECT * FROM test;

DROP TABLE test;

-- Test ambiguity when two input columns map to the same table column (ignore case)
CREATE TABLE json_test (id Int);

SET input_format_column_name_matching_mode='ignore_case';

INSERT INTO json_test FORMAT JSONColumns {"ID": [444], "id": [123]} -- { clientError 117 }

SELECT * FROM json_test;

DROP TABLE json_test;
