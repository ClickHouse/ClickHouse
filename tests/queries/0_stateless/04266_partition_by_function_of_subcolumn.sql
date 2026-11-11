-- Tags: no-random-merge-tree-settings

-- Test that PARTITION BY with a function applied to a subcolumn works correctly.
-- Previously, executePartitionByExpression did not materialize subcolumns
-- before executing the expression, causing NOT_FOUND_COLUMN_IN_BLOCK.

-- Case 1: Tuple subcolumn with arithmetic function in PARTITION BY
DROP TABLE IF EXISTS t_subcol_func_part;
CREATE TABLE t_subcol_func_part (t Tuple(a UInt32, b UInt32), val Int32) ENGINE = MergeTree ORDER BY val PARTITION BY (t.a + t.b);
INSERT INTO t_subcol_func_part VALUES ((1, 2), 10), ((3, 4), 20), ((1, 2), 30);
SELECT t.a, t.b, t.a + t.b AS part_key, val FROM t_subcol_func_part ORDER BY val;
DROP TABLE t_subcol_func_part;

-- Case 2: JSON subcolumn with toYear function in PARTITION BY
DROP TABLE IF EXISTS t_json_func_part;
CREATE TABLE t_json_func_part (json JSON(d Date)) ENGINE = MergeTree ORDER BY tuple() PARTITION BY toYear(json.d);
INSERT INTO t_json_func_part SELECT '{"d" : "2020-03-15"}';
INSERT INTO t_json_func_part SELECT '{"d" : "2021-07-20"}';
SELECT json.d, toYear(json.d) AS yr FROM t_json_func_part ORDER BY json.d;
DROP TABLE t_json_func_part;

-- Case 3: Nullable subcolumn with function in PARTITION BY
DROP TABLE IF EXISTS t_nullable_func_part;
CREATE TABLE t_nullable_func_part (c0 Array(Nullable(Int32)), c1 Int32) ENGINE = MergeTree() PARTITION BY length(c0.null) ORDER BY c1;
INSERT INTO t_nullable_func_part VALUES ([], 1), ([1], 2), ([NULL, 1], 3);
SELECT c0, length(c0.null) AS null_len, c1 FROM t_nullable_func_part ORDER BY c1;
DROP TABLE t_nullable_func_part;
