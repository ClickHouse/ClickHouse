-- { echo }
SET implicit_select = 1, implicit_table_at_top_level = 'test', enable_analyzer = 1;
DROP TABLE IF EXISTS test;
CREATE TABLE test (s String) ENGINE = Memory;
INSERT INTO test VALUES ('Hello'), ('World');

s;
count();
1;
*;
SELECT *, (SELECT 1);
SELECT * FROM (SELECT *);
SELECT * UNION ALL SELECT *;

DROP TABLE test;
