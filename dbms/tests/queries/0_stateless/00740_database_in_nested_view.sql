DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_view;
DROP TABLE IF EXISTS test.test_nested_view;
DROP TABLE IF EXISTS test.test_joined_view;

USE test;
CREATE VIEW test AS SELECT 1 AS N;
CREATE VIEW test_view AS SELECT * FROM test;
CREATE VIEW test_nested_view AS SELECT * FROM (SELECT * FROM test);
CREATE VIEW test_joined_view AS SELECT * FROM test ANY LEFT JOIN test USING N;

SELECT * FROM test_view;
SELECT * FROM test_nested_view;
SELECT * FROM test_joined_view;

USE default;
SELECT * FROM test.test_view;
SELECT * FROM test.test_nested_view;
SELECT * FROM test.test_joined_view;

DROP TABLE IF EXISTS test.test;
DROP TABLE IF EXISTS test.test_view;
DROP TABLE IF EXISTS test.test_nested_view;
