DROP TABLE IF EXISTS test.null;
DROP TABLE IF EXISTS test.null_view;

CREATE TABLE test.null (x UInt8) ENGINE = Null;
CREATE VIEW test.null_view AS SELECT * FROM test.null WHERE toUInt64(x) IN (SELECT number FROM system.numbers);
INSERT INTO test.null VALUES (1);

DROP TABLE test.null;
DROP TABLE test.null_view;
