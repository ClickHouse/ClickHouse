-- Tags: long

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
SELECT
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS optimize_aggregation_by_sharding = 0, optimize_aggregation_in_order = 0))
    =
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS optimize_aggregation_by_sharding = 1, optimize_aggregation_in_order = 0));

SELECT 'Aggregation in order enabled, takes precedence over sharded aggregation';
SELECT
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS optimize_aggregation_by_sharding = 0, optimize_aggregation_in_order = 1))
    =
    (SELECT sum(s), count() FROM
        (SELECT a, sum(b) AS s FROM test_sharded_agg_neg GROUP BY a
         SETTINGS optimize_aggregation_by_sharding = 1, optimize_aggregation_in_order = 1));

SELECT 'No GROUP BY keys';
SELECT
    (SELECT sum(b) FROM test_sharded_agg_neg SETTINGS optimize_aggregation_by_sharding = 0)
    =
    (SELECT sum(b) FROM test_sharded_agg_neg SETTINGS optimize_aggregation_by_sharding = 1);
