-- Tags: no-fasttest, no-old-analyzer
-- Tag no-fasttest: `make_distributed_plan` is not exercised there.
-- Tag no-old-analyzer: the fix is in the planner, see 05077.

-- The positions of repeated GROUP BY keys travel with the serialized `Cube` and `Rollup` steps, so
-- a plan that is written out and read back has to expand the same grouping sets as one that is not.
-- Losing the payload is visible rather than silent: it collapses the sets back onto the
-- deduplicated keys, which is the bug this test's sibling covers.
-- https://github.com/ClickHouse/ClickHouse/issues/117904

-- `make_distributed_plan` refuses an aggregation carrying `max_rows_to_group_by`, and the CI users
-- profile sets it (`tests/config/users.d/limits.yaml`), so pin it back to its default here rather
-- than lose the coverage. Several tests in this directory pin it for their own reasons already.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS t_repeated_keys_distributed;

CREATE TABLE t_repeated_keys_distributed (a UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_repeated_keys_distributed SELECT number % 4 FROM numbers(100);

SELECT 'cube over a repeated key, in and out of a serialized plan';
SELECT count() FROM (SELECT a, count() AS c FROM t_repeated_keys_distributed GROUP BY CUBE(a, a));
SELECT count() FROM (SELECT a, count() AS c FROM t_repeated_keys_distributed GROUP BY CUBE(a, a))
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT 'rollup over a repeated key';
SELECT count() FROM (SELECT a, count() AS c FROM t_repeated_keys_distributed GROUP BY ROLLUP(a, a));
SELECT count() FROM (SELECT a, count() AS c FROM t_repeated_keys_distributed GROUP BY ROLLUP(a, a))
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT 'GROUPING over a repeated key';
SELECT count() FROM (
    SELECT a, count() AS c, GROUPING(a) AS g FROM t_repeated_keys_distributed GROUP BY CUBE(a, a))
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT 'no repetition is unchanged either way';
SELECT count() FROM (SELECT a, count() AS c FROM t_repeated_keys_distributed GROUP BY CUBE(a));
SELECT count() FROM (SELECT a, count() AS c FROM t_repeated_keys_distributed GROUP BY CUBE(a))
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

DROP TABLE t_repeated_keys_distributed;
