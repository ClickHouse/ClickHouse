-- Regression test for heap-use-after-free in GraphiteRollupSortedAlgorithm.
-- Ticket: https://github.com/ClickHouse/ClickHouse/issues/98523
-- When the Time column uses a non-DateTime type (e.g. Int128), an exception
-- during mergeBlock leaves an aggregate state created. On destruction,
-- GraphiteRollupMergedData accessed freed memory from already-destroyed params.

SET joined_subquery_requires_alias = 0;
SET allow_suspicious_low_cardinality_types = 1;
SET optimize_trivial_insert_select = 0;
SET optimize_on_insert = 1;

DROP TABLE IF EXISTS test_graphite_fuzz;

CREATE TABLE test_graphite_fuzz
(
    `key` LowCardinality(UInt256),
    `Path` LowCardinality(String),
    `Time` Int128,
    `Value` Float64,
    `Version` UInt32,
    `col` Int64
)
ENGINE = GraphiteMergeTree('graphite_rollup')
ORDER BY key
SETTINGS index_granularity = 10;

INSERT INTO test_graphite_fuzz
WITH dates AS
(
    SELECT
        toStartOfDay(toDateTime(now('UTC'), 'UTC')) AS today,
        today - INTERVAL 3 DAY AS older_date
)
SELECT 1 AS key, 'sum_1' AS s, today - number * 60 - 30, number, 1, number FROM dates, numbers(300)
UNION ALL SELECT 2, 'sum_1', today - number * 60 - 30, number, 1, number FROM dates, numbers(300)
UNION ALL SELECT 1, 'sum_2', today - number * 60 - 30, number, 1, number FROM dates, numbers(300)
UNION ALL SELECT 2, 'sum_2', today - number * 60 - 30, number, 1, number FROM dates, numbers(300)
UNION ALL SELECT 1, 'max_1', today - number * 60 - 30, number, 1, number FROM dates, numbers(300)
UNION ALL SELECT 2, 'max_1', today - number * 60 - 30, number, 1, number FROM dates, numbers(300)
UNION ALL SELECT 1, 'max_2', today - number * 60 - 30, number, 1, number FROM dates, numbers(300)
UNION ALL SELECT 2, 'max_2', today - number * 60 - 30, number, 1, number FROM dates, numbers(300)
UNION ALL SELECT 1 AS key, 'sum_1' AS s, older_date - number * 60 - 30, number, 1, number FROM dates, numbers(1200)
UNION ALL SELECT 2, 'sum_1', older_date - number * 60 - 30, number, 1, number FROM dates, numbers(1200)
UNION ALL SELECT 1, 'sum_2', older_date - number * 60 - 30, number, 1, number FROM dates, numbers(1200)
UNION ALL SELECT 2, 'sum_2', older_date - number * 60 - 30, number, 1, number FROM dates, numbers(1200)
UNION ALL SELECT 1, 'max_1', older_date - number * 60 - 30, number, 1, number FROM dates, numbers(1200)
UNION ALL SELECT 2, 'max_1', older_date - number * 60 - 30, number, 1, number FROM dates, numbers(1200)
UNION ALL SELECT 1, 'max_2', older_date - number * 60 - 30, number, 1, number FROM dates, numbers(1200)
UNION ALL SELECT 2, 'max_2', older_date - number * 60 - 30, number, 1, number FROM dates, numbers(1200); -- { serverError BAD_GET }

DROP TABLE IF EXISTS test_graphite_fuzz;
