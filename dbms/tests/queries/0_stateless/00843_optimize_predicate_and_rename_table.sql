SET enable_optimize_predicate_expression = 1;

DROP TABLE IF EXISTS test.test1;
DROP TABLE IF EXISTS test.test2;
DROP TABLE IF EXISTS test.view;

CREATE TABLE test.test1 (a UInt8) ENGINE = Memory;
INSERT INTO test.test1 VALUES (1);

CREATE VIEW test.view AS SELECT * FROM test.test1;
SELECT * FROM test.view;
RENAME TABLE test.test1 TO test.test2;
SELECT * FROM test.view; -- { serverError 60 }
RENAME TABLE test.test2 TO test.test1;
SELECT * FROM test.view;

DROP TABLE test.test1;
DROP TABLE test.view;
