-- Tags: no-random-merge-tree-settings, no-random-settings, no-parallel-replicas
-- EXPLAIN output may differ

SET max_rows_to_group_by = 0;
SET max_threads = 8;

DROP TABLE IF EXISTS test;
CREATE TABLE test
(
    a String,
    b UInt64,
    u8 UInt8,
    nullable_key Nullable(String),
    arr Array(UInt64),
    flag UInt8
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO test
SELECT
    toString(rand() % 100000) AS a,
    number AS b,
    toUInt8(number % 250) AS u8,
    if(number % 10 = 0, NULL, toString(number % 50000)) AS nullable_key,
    [number % 3, number % 7, number % 11] AS arr,
    toUInt8(number % 2) AS flag
FROM numbers(300000);

SELECT 'HAVING clause';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) AS s FROM test GROUP BY a HAVING s > 1000000
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'HAVING with count filter';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, count() AS cnt FROM test GROUP BY a HAVING cnt >= 3
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'HAVING with multiple aggregates';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) AS s, max(b) AS m FROM test GROUP BY a HAVING s > 500000 AND m > 100000
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'ORDER BY on aggregation result';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) AS s FROM test GROUP BY a ORDER BY s DESC LIMIT 10
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'ORDER BY key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) AS s FROM test GROUP BY a ORDER BY a LIMIT 10
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'ORDER BY with LIMIT';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT b % 1000 AS k, sum(b) AS s FROM test GROUP BY k ORDER BY k LIMIT 20
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'quantile';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, quantile(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'quantileExact';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, quantileExact(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'median';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, median(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'quantiles (multiple quantile levels)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, quantiles(0.25, 0.5, 0.75)(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Skewed distribution (one dominant key)';
DROP TABLE IF EXISTS test_skewed;
CREATE TABLE test_skewed (a String, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_skewed
SELECT
    if(number % 100 = 0, toString(number % 1000), 'dominant_key') AS a,
    number AS b
FROM numbers(300000);
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test_skewed GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'Skewed distribution with multiple aggregates';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b), count(), max(b) FROM test_skewed GROUP BY a
    SETTINGS enable_sharding_aggregator = 1
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'max_threads = 2 (minimum parallelism)';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, sum(b) FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, max_threads = 2
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'max_threads = 2 with count';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT a, count() FROM test GROUP BY a
    SETTINGS enable_sharding_aggregator = 1, max_threads = 2
) WHERE explain LIKE '%ShardByHashTransform%';

SELECT 'max_threads = 2 with Nullable key';
SELECT count() > 0 FROM (
    EXPLAIN PIPELINE SELECT nullable_key, sum(b) FROM test GROUP BY nullable_key
    SETTINGS enable_sharding_aggregator = 1, max_threads = 2
) WHERE explain LIKE '%ShardByHashTransform%';

DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_skewed;
