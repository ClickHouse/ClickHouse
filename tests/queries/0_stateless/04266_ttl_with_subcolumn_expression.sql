-- Tags: no-random-merge-tree-settings

-- Test that TTL expressions referencing subcolumns work correctly.
-- Previously, executeExpressionAndGetColumn used getByName which failed
-- for subcolumns not present as top-level columns in the block.
-- We only verify that INSERT succeeds (no NOT_FOUND_COLUMN_IN_BLOCK exception)
-- and data is readable. TTL deletion itself is timing-dependent so we don't
-- assert specific row counts.

-- Case 1: Tuple subcolumn in TTL expression
DROP TABLE IF EXISTS t_ttl_subcol;
CREATE TABLE t_ttl_subcol (t Tuple(a UInt32, d DateTime), val Int32) ENGINE = MergeTree ORDER BY val TTL t.d + INTERVAL 1 MONTH;
INSERT INTO t_ttl_subcol VALUES ((1, '2000-01-01'), 10), ((2, '2099-01-01'), 20);
SELECT t.a, val FROM t_ttl_subcol WHERE t.a = 2;
DROP TABLE t_ttl_subcol;

-- Case 2: Tuple subcolumn with function in TTL expression
DROP TABLE IF EXISTS t_ttl_subcol_func;
CREATE TABLE t_ttl_subcol_func (t Tuple(a UInt32, d DateTime), val Int32) ENGINE = MergeTree ORDER BY val TTL toDate(t.d) + INTERVAL 1 MONTH;
INSERT INTO t_ttl_subcol_func VALUES ((1, '2000-01-01 12:00:00'), 10), ((2, '2099-01-01 12:00:00'), 20);
SELECT t.a, val FROM t_ttl_subcol_func WHERE t.a = 2;
DROP TABLE t_ttl_subcol_func;

-- Case 3: JSON subcolumn in TTL expression
DROP TABLE IF EXISTS t_ttl_json_subcol;
CREATE TABLE t_ttl_json_subcol (json JSON(d DateTime), val Int32) ENGINE = MergeTree ORDER BY val TTL json.d + INTERVAL 1 MONTH;
INSERT INTO t_ttl_json_subcol VALUES ('{"d" : "2000-01-01"}', 10), ('{"d" : "2099-01-01"}', 20);
SELECT json.d, val FROM t_ttl_json_subcol WHERE val = 20;
DROP TABLE t_ttl_json_subcol;
