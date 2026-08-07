-- Test that alias columns work correctly together with virtual columns.
-- { echoOn }

DROP TABLE IF EXISTS t_log_alias;
CREATE TABLE t_log_alias (key Int, value Int, doubled ALIAS value * 2) ENGINE = Log;
INSERT INTO t_log_alias VALUES (1, 10), (2, 20);
SELECT _table, key, doubled FROM t_log_alias ORDER BY key;
SELECT _table, doubled FROM t_log_alias ORDER BY doubled;
DROP TABLE t_log_alias;

DROP TABLE IF EXISTS t_tinylog_alias;
CREATE TABLE t_tinylog_alias (key Int, value Int, doubled ALIAS value * 2) ENGINE = TinyLog;
INSERT INTO t_tinylog_alias VALUES (1, 10), (2, 20);
SELECT _table, key, doubled FROM t_tinylog_alias ORDER BY key;
SELECT _table, doubled FROM t_tinylog_alias ORDER BY doubled;
DROP TABLE t_tinylog_alias;

DROP TABLE IF EXISTS t_stripelog_alias;
CREATE TABLE t_stripelog_alias (key Int, value Int, doubled ALIAS value * 2) ENGINE = StripeLog;
INSERT INTO t_stripelog_alias VALUES (3, 30), (4, 40);
SELECT _table, key, doubled FROM t_stripelog_alias ORDER BY key;
SELECT _table, doubled FROM t_stripelog_alias ORDER BY doubled;
DROP TABLE t_stripelog_alias;
