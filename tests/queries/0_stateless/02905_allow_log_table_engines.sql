DROP TABLE IF EXISTS test_engine_log;
DROP TABLE IF EXISTS test_engine_tinylog;
DROP TABLE IF EXISTS test_engine_stripelog;

CREATE TABLE test_engine_log(foo String) ENGINE = Log;  -- { serverError 344 }
CREATE TABLE test_engine_tinylog(foo String) ENGINE = TinyLog;  -- { serverError 344 }
CREATE TABLE test_engine_stipelog(foo String) ENGINE = StripeLog;  -- { serverError 344 }

SET allow_table_engine_log=1, allow_table_engine_tinylog=1, allow_table_engine_stripelog=1;

CREATE TABLE test_engine_log(foo String) ENGINE = Log;
CREATE TABLE test_engine_tinylog(foo String) ENGINE = TinyLog;
CREATE TABLE test_engine_stripelog(foo String) ENGINE = StripeLog;

INSERT INTO test_engine_log VALUES('a');
INSERT INTO test_engine_tinylog VALUES('b');
INSERT INTO test_engine_stripelog VALUES('c');

SELECT * FROM test_engine_log;
SELECT * FROM test_engine_tinylog;
SELECT * FROM test_engine_stripelog;

DROP TABLE test_engine_log;
DROP TABLE test_engine_tinylog;
DROP TABLE test_engine_stripelog;
