SET send_logs_level = 'none';

DROP TABLE IF EXISTS test.t;
DROP TABLE IF EXISTS test.v;

CREATE TABLE test.t(a Int) ENGINE = Memory;
CREATE VIEW test.v AS SELECT * FROM test.t;

DROP TABLE test.t;
SELECT * FROM test.v; -- {serverError 60}

DROP TABLE test.v;