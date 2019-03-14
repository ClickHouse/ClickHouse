DROP TABLE IF EXISTS test.t;

CREATE OR REPLACE VIEW test.t (number UInt64) AS SELECT number FROM system.numbers;
SHOW CREATE TABLE test.t;

CREATE OR REPLACE VIEW test.t AS SELECT number+1 AS next_number FROM system.numbers;
SHOW CREATE TABLE test.t;

DROP TABLE test.t;
