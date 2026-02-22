-- Tags: no-parallel-replicas
-- ^ at this moment, EXPLAIN with parallel replicas may run the subqueries,
-- while this behavior is incorrect, the test focuses on a different thing,
-- so we disable it under parallel replicas.

-- Test for UBSan issue in join order optimization when estimated row count overflows UInt64
-- The issue occurs when converting a very large double to UInt64 in estimateJoinCardinality
-- https://github.com/ClickHouse/ClickHouse/pull/94704

DROP TABLE IF EXISTS data_03812;

CREATE TABLE data_03812 (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO data_03812 VALUES (1, 1), (2, 2);

SET max_rows_to_read = 0;
set ignore_format_null_for_explain = 0;

EXPLAIN PLAN
SELECT 1
FROM data_03812 AS t1
ALL INNER JOIN (
    SELECT number FROM system.numbers LIMIT 9223372036854775806
) AS t2 ON 1
FORMAT Null;

DROP TABLE data_03812;
