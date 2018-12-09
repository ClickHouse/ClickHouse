DROP TABLE IF EXISTS test.t;
DROP TABLE IF EXISTS test.td;

CREATE TABLE test.t (i UInt32) engine = Log;
CREATE MATERIALIZED VIEW test.tv engine = Log AS SELECT * from test.t;
INSERT INTO test.t SELECT 1;
DROP TABLE test.t;
CREATE TABLE test.t (j UInt32) engine = Log;
INSERT INTO test.t SELECT 1;
SELECT * FROM test.tv;

DROP TABLE IF EXISTS test.t;
DROP TABLE IF EXISTS test.tv;
