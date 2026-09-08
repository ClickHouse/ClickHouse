-- A column of a table function can be selected more than once in `INSERT SELECT`.
-- The structure hint taken from the insertion table must not break on that.

SET optimize_trivial_insert_select = 0;

INSERT INTO TABLE FUNCTION file(concat(database(), '.data_05023.jsonl')) SELECT 1.5 AS c SETTINGS engine_file_truncate_on_insert = 1;

DROP TABLE IF EXISTS test_05023_same_type;
CREATE TABLE test_05023_same_type (x Float64, y Float64) ENGINE = Memory;

-- The insert table columns agree on the type, so it is used as a hint for `c`.
SET use_structure_from_insertion_table_in_table_functions = 2;
INSERT INTO test_05023_same_type (x, y) SELECT c AS x, c AS y FROM file(concat(database(), '.data_05023.jsonl'));

SET use_structure_from_insertion_table_in_table_functions = 1;
INSERT INTO test_05023_same_type (x, y) SELECT c, c FROM file(concat(database(), '.data_05023.jsonl'));

SELECT * FROM test_05023_same_type;

DROP TABLE test_05023_same_type;

DROP TABLE IF EXISTS test_05023_different_types;
CREATE TABLE test_05023_different_types (x Int64, y Float64) ENGINE = Memory;

-- The insert table columns disagree on the type of `c`, so the hint is ambiguous:
-- in the automatic mode the structure is inferred from the data instead.
SET use_structure_from_insertion_table_in_table_functions = 2;
INSERT INTO test_05023_different_types (x, y) SELECT c, c FROM file(concat(database(), '.data_05023.jsonl'));

SELECT * FROM test_05023_different_types;

-- ... and when the hint is mandatory, the ambiguity is reported.
SET use_structure_from_insertion_table_in_table_functions = 1;
INSERT INTO test_05023_different_types (x, y) SELECT c, c FROM file(concat(database(), '.data_05023.jsonl')); -- { serverError ILLEGAL_COLUMN }

DROP TABLE test_05023_different_types;
