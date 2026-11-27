-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/17860
-- Joining tables by a virtual column (e.g. `_part_index`) used to throw
-- `Not found column _part_index in block`. It works with the analyzer.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (a Int64, b Int64) ENGINE = MergeTree() PARTITION BY a ORDER BY a;
CREATE TABLE t1 (a Int64, b Int64) ENGINE = MergeTree() PARTITION BY a ORDER BY a;

-- `PARTITION BY a` with distinct values puts every row into its own part,
-- so `_part_index` is 0, 1, 2 and the join across tables is non-trivial.
INSERT INTO t0 VALUES (0, 10), (1, 11), (2, 12);
INSERT INTO t1 VALUES (0, 100), (1, 101), (2, 102);

SELECT t0.a, t0.b, t1.b, _part_index FROM t0 JOIN t1 USING _part_index ORDER BY _part_index;

SELECT '--- global join ---';
SELECT a FROM t0 GLOBAL JOIN t1 USING _part_index ORDER BY a;

DROP TABLE t0;
DROP TABLE t1;
