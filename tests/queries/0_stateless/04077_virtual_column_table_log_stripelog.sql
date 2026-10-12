-- Test _table virtual column for Log, TinyLog, and StripeLog engines.
-- { echoOn }

DROP TABLE IF EXISTS t_log;
DROP TABLE IF EXISTS t_tinylog;
DROP TABLE IF EXISTS t_stripelog;
DROP TABLE IF EXISTS t_merge;

CREATE TABLE t_log (key Int, value Int) ENGINE = Log;
CREATE TABLE t_tinylog (key Int, value Int) ENGINE = TinyLog;
CREATE TABLE t_stripelog (key Int, value Int) ENGINE = StripeLog;

INSERT INTO t_log VALUES (1, 10), (2, 20);
INSERT INTO t_tinylog VALUES (3, 30), (4, 40);
INSERT INTO t_stripelog VALUES (5, 50), (6, 60);

SELECT _table, key, value FROM t_log ORDER BY key;
SELECT _table, key, value FROM t_tinylog ORDER BY key;

-- Only virtual columns requested (no physical columns)
SELECT _table FROM t_log ORDER BY _table;
SELECT _table FROM t_stripelog ORDER BY _table;
SELECT count(_table) FROM t_log WHERE _table = 't_log' GROUP BY _table;
SELECT count(_table) FROM t_tinylog WHERE _table = 't_tinylog' GROUP BY _table;
SELECT count(_table) FROM t_stripelog WHERE _table = 't_stripelog' GROUP BY _table;

-- Test that a real column named _table takes priority over the virtual one
DROP TABLE IF EXISTS t_log_override;
CREATE TABLE t_log_override (_table String, value Int) ENGINE = Log;
INSERT INTO t_log_override VALUES ('custom', 100);
SELECT _table, value FROM t_log_override;

DROP TABLE t_log_override;
DROP TABLE t_log;
DROP TABLE t_tinylog;
DROP TABLE t_stripelog;
