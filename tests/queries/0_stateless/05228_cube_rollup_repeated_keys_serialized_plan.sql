-- Tags: no-fasttest
-- no-fasttest: `make_distributed_plan` is not exercised there.

-- The positions of repeated `GROUP BY` keys are part of the serialized `Cube` and `Rollup` steps.

-- `make_distributed_plan` refuses an aggregation with `max_rows_to_group_by`, which the CI profile sets.
SET max_rows_to_group_by = 0;

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO tab SELECT number % 4, number % 2 FROM numbers(100);

SELECT a, b, count() AS c FROM tab GROUP BY ROLLUP(a, b, a) ORDER BY a, b, c
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

SELECT a, b, count() AS c, GROUPING(a, b) AS g FROM tab GROUP BY CUBE(a, b, a) ORDER BY g, a, b, c
SETTINGS make_distributed_plan = 1, distributed_plan_execute_locally = 1;

DROP TABLE tab;
