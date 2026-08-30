-- Tags: long

SET max_rows_to_group_by = 0;

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
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a HAVING s > 1000000 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a HAVING s > 1000000 SETTINGS enable_sharding_aggregator = 1));

SELECT 'HAVING with count filter';
SELECT
    (SELECT sum(cnt), count() FROM (SELECT a, count() AS cnt FROM test GROUP BY a HAVING cnt >= 3 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(cnt), count() FROM (SELECT a, count() AS cnt FROM test GROUP BY a HAVING cnt >= 3 SETTINGS enable_sharding_aggregator = 1));

SELECT 'HAVING with multiple aggregates';
SELECT
    (SELECT sum(s), sum(m), count() FROM
        (SELECT a, sum(b) AS s, max(b) AS m FROM test GROUP BY a HAVING s > 500000 AND m > 100000 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), sum(m), count() FROM
        (SELECT a, sum(b) AS s, max(b) AS m FROM test GROUP BY a HAVING s > 500000 AND m > 100000 SETTINGS enable_sharding_aggregator = 1));

SELECT 'ORDER BY on aggregation result';
SELECT
    (SELECT groupArray(s) FROM (SELECT a, sum(b) AS s FROM test GROUP BY a ORDER BY s DESC LIMIT 10 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT groupArray(s) FROM (SELECT a, sum(b) AS s FROM test GROUP BY a ORDER BY s DESC LIMIT 10 SETTINGS enable_sharding_aggregator = 1));

SELECT 'ORDER BY key';
SELECT
    (SELECT groupArray(a) FROM (SELECT a, sum(b) AS s FROM test GROUP BY a ORDER BY a LIMIT 10 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT groupArray(a) FROM (SELECT a, sum(b) AS s FROM test GROUP BY a ORDER BY a LIMIT 10 SETTINGS enable_sharding_aggregator = 1));

SELECT 'ORDER BY with LIMIT';
SELECT
    (SELECT groupArray(tuple(k, s)) FROM (SELECT b % 1000 AS k, sum(b) AS s FROM test GROUP BY k ORDER BY k LIMIT 20 SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT groupArray(tuple(k, s)) FROM (SELECT b % 1000 AS k, sum(b) AS s FROM test GROUP BY k ORDER BY k LIMIT 20 SETTINGS enable_sharding_aggregator = 1));

SELECT 'quantile';
SELECT abs(
    (SELECT sum(s) FROM (SELECT a, quantile(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 0))
    -
    (SELECT sum(s) FROM (SELECT a, quantile(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 1))
) < 0.001;

SELECT 'quantileExact';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, quantileExact(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, quantileExact(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 1));

SELECT 'median';
SELECT abs(
    (SELECT sum(s) FROM (SELECT a, median(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 0))
    -
    (SELECT sum(s) FROM (SELECT a, median(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 1))
) < 0.001;

SELECT 'quantiles (multiple quantile levels)';
SELECT
    (SELECT sum(length(s)), count() FROM (SELECT a, quantiles(0.25, 0.5, 0.75)(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(length(s)), count() FROM (SELECT a, quantiles(0.25, 0.5, 0.75)(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 1));

SELECT 'Skewed distribution (one dominant key)';
DROP TABLE IF EXISTS test_skewed;
CREATE TABLE test_skewed (a String, b UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO test_skewed
SELECT
    if(number % 100 = 0, toString(number % 1000), 'dominant_key') AS a,
    number AS b
FROM numbers(300000);
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_skewed GROUP BY a SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test_skewed GROUP BY a SETTINGS enable_sharding_aggregator = 1));

SELECT 'Skewed distribution with multiple aggregates';
SELECT
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sum(b) AS s1, count() AS s2, max(b) AS s3
         FROM test_skewed GROUP BY a SETTINGS enable_sharding_aggregator = 0))
    =
    (SELECT sum(s1), sum(s2), sum(s3), count() FROM
        (SELECT a, sum(b) AS s1, count() AS s2, max(b) AS s3
         FROM test_skewed GROUP BY a SETTINGS enable_sharding_aggregator = 1));

SELECT 'max_threads = 2 (minimum parallelism)';
SELECT
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 0, max_threads = 2))
    =
    (SELECT sum(s), count() FROM (SELECT a, sum(b) AS s FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 1, max_threads = 2));

SELECT 'max_threads = 2 with count';
SELECT
    (SELECT sum(cnt), count() FROM (SELECT a, count() AS cnt FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 0, max_threads = 2))
    =
    (SELECT sum(cnt), count() FROM (SELECT a, count() AS cnt FROM test GROUP BY a SETTINGS enable_sharding_aggregator = 1, max_threads = 2));

SELECT 'max_threads = 2 with Nullable key';
SELECT
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test GROUP BY nullable_key SETTINGS enable_sharding_aggregator = 0, max_threads = 2))
    =
    (SELECT sum(s), count() FROM (SELECT nullable_key, sum(b) AS s FROM test GROUP BY nullable_key SETTINGS enable_sharding_aggregator = 1, max_threads = 2));

DROP TABLE IF EXISTS test;
DROP TABLE IF EXISTS test_skewed;
