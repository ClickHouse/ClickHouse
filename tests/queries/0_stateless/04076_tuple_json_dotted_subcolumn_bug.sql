-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/101271
-- Selecting a Tuple element whose name contains a dot (e.g. `a.b`) caused a
-- server exception when another element named `a` has a JSON type, because the
-- prefix match on `a` would set `res` before the exact match on `a.b` could.
DROP TABLE IF EXISTS t_json_crash;
CREATE TABLE t_json_crash (t Tuple(a JSON, `a.b` UInt32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_json_crash VALUES (('{"b": 999}', 42));
SELECT t.`a.b` FROM t_json_crash;
DROP TABLE IF EXISTS t_json_crash;
