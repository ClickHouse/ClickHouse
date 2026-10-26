-- Regression test: reading subcolumns of ALIAS columns from StorageLog
-- should not cause a LOGICAL_ERROR exception.
-- https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=98367&sha=9acc57e1ae51dba197e0d9b12743fb3804683bc4&name_0=PR&name_1=BuzzHouse%20%28arm_asan%29

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_log_alias;
CREATE TABLE t_log_alias (c0 Array(String) ALIAS [toString(_table)], c1 Int64) ENGINE = Log;
INSERT INTO t_log_alias (c1) VALUES (1), (2), (3);

SELECT c0 FROM t_log_alias;
SELECT c0.size0 FROM t_log_alias;

DROP TABLE t_log_alias;

-- Same test with TinyLog
DROP TABLE IF EXISTS t_tinylog_alias;
CREATE TABLE t_tinylog_alias (c0 Array(String) ALIAS [toString(_table)], c1 Int64) ENGINE = TinyLog;
INSERT INTO t_tinylog_alias (c1) VALUES (1), (2), (3);

SELECT c0 FROM t_tinylog_alias;
SELECT c0.size0 FROM t_tinylog_alias;

DROP TABLE t_tinylog_alias;
