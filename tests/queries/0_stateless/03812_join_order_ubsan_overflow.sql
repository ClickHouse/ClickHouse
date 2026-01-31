-- Test for UBSan issue in join order optimization when estimated row count overflows UInt64
-- The issue occurs when converting a very large double to UInt64 in estimateJoinCardinality
-- https://github.com/ClickHouse/ClickHouse/pull/94704

DROP TABLE IF EXISTS data_03812;

CREATE TABLE data_03812 (key UInt64, value UInt64) ENGINE = MergeTree ORDER BY key;
INSERT INTO data_03812 VALUES (1, 1), (2, 2);


EXPLAIN PLAN
SELECT 1
FROM data_03812 AS t1
ALL INNER JOIN (
    SELECT number FROM system.numbers LIMIT 9223372036854775806
) AS t2 ON 1
FORMAT Null;

DROP TABLE data_03812;
