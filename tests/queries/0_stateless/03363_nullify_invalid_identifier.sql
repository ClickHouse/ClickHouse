CREATE TEMPORARY TABLE test_nullify_tab (id UInt32, name String) ENGINE = Memory;
INSERT INTO test_nullify_tab(id, name) VALUES(42, 'foo');
INSERT INTO test_nullify_tab(id, name) VALUES(17, 'bar');
INSERT INTO test_nullify_tab(id, name) VALUES(4711, 'cologne');

-- "what" does not exist and will be nullified
SET max_threads=1, nullify_invalid_identifier=1;
SELECT id, name, what FROM test_nullify_tab;
SELECT id, what, name FROM test_nullify_tab;
SELECT boo FROM (SELECT id FROM test_nullify_tab LIMIT 1);
SELECT id, is_this FROM (SELECT id, what AS is_this FROM test_nullify_tab LIMIT 1);
WITH data AS (SELECT id, what, name FROM test_nullify_tab WHERE id == 17) SELECT id, what, name FROM data;
WITH data AS (SELECT id AS new_id, what AS new_what, name AS new_name FROM test_nullify_tab WHERE id == 4711) SELECT new_id, new_what, new_name FROM data;
SELECT COUNT(*) FROM test_nullify_tab WHERE what IS NULL;