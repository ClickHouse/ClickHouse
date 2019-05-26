SET enable_optimize_predicate_expression = 1;

DROP TABLE IF EXISTS test1;
DROP TABLE IF EXISTS test2;
DROP TABLE IF EXISTS view;

CREATE TABLE test1 (a UInt8) ENGINE = Memory;
INSERT INTO test1 VALUES (1);

CREATE VIEW view AS SELECT * FROM test1;
SELECT * FROM view;
RENAME TABLE test1 TO test2;
SELECT * FROM view; -- { serverError 60 }
RENAME TABLE test2 TO test1;
SELECT * FROM view;

DROP TABLE test1;
DROP TABLE view;
