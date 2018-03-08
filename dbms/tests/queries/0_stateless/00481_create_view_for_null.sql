DROP TABLE IF EXISTS test.null;
DROP TABLE IF EXISTS test.null_view;

CREATE TABLE test.null (x UInt8) ENGINE = Null;
CREATE VIEW test.null_view AS SELECT * FROM test.null;
INSERT INTO test.null VALUES (1);

SELECT * FROM test.null;
SELECT * FROM test.null_view;

DROP TABLE test.null;
DROP TABLE test.null_view;
