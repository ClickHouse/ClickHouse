-- Tags: no-random-merge-tree-settings, no-random-settings
-- EXPLAIN output may differ

DROP TABLE IF EXISTS test_sharded_agg_neg;
CREATE TABLE test_sharded_agg_neg
(
    a String,
    b UInt64
)
ENGINE = MergeTree
ORDER BY a;

INSERT INTO test_sharded_agg_neg
SELECT
    toString(rand() % 100000) AS a,
    number AS b
FROM numbers(300000);

INSERT INTO test_sharded_agg_neg
SELECT
    toString(rand() % 100000) AS a,
    number AS b
FROM numbers(300000);

SELECT 'Base case: sharded aggregation is used';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sharded_agg_neg GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1, optimize_aggregation_in_order = 0
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'Aggregation in order enabled, takes precedence over sharded aggregation';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sharded_agg_neg GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1, optimize_aggregation_in_order = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'No GROUP BY keys';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT sum(b) FROM test_sharded_agg_neg
    SETTINGS optimize_aggregation_by_sharding = 1
) WHERE explain LIKE '%ScatterByHashTransform%';

SELECT 'max_rows_to_group_by enabled';
SELECT count() = 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_sharded_agg_neg GROUP BY a
    SETTINGS optimize_aggregation_by_sharding = 1, max_rows_to_group_by = 10, group_by_overflow_mode = 'any'
) WHERE explain LIKE '%ScatterByHashTransform%';
