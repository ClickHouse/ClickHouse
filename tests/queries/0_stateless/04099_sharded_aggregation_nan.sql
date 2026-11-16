SET max_rows_to_group_by = 0;
SET enable_sharding_aggregator = 1;

DROP TABLE IF EXISTS test_sharded_nan;
CREATE TABLE test_sharded_nan (k UInt64, v Float64)
ENGINE = MergeTree ORDER BY tuple();

-- Two keys alternating, first value for each key is NaN.
INSERT INTO test_sharded_nan
SELECT
    if(number % 2 = 0, 1000, 2000) AS k,
    if(number <= 1, nan, toFloat64(number)) AS v
FROM numbers(1000);

SELECT 'min';
SELECT k, min(v) FROM test_sharded_nan GROUP BY k ORDER BY k;

SELECT 'max';
SELECT k, max(v) FROM test_sharded_nan GROUP BY k ORDER BY k;

SELECT 'argMin';
SELECT k, argMin(v, v) FROM test_sharded_nan GROUP BY k ORDER BY k;

SELECT 'argMax';
SELECT k, argMax(v, v) FROM test_sharded_nan GROUP BY k ORDER BY k;

SELECT 'min Float32';
SELECT k, min(toFloat32(v)) FROM test_sharded_nan GROUP BY k ORDER BY k;

SELECT 'max Float32';
SELECT k, max(toFloat32(v)) FROM test_sharded_nan GROUP BY k ORDER BY k;

-- Key 1000 has all NaN, key 2000 has all non-NaN.
DROP TABLE IF EXISTS test_sharded_all_nan;
CREATE TABLE test_sharded_all_nan (k UInt64, v Float64)
ENGINE = MergeTree ORDER BY tuple();

INSERT INTO test_sharded_all_nan
SELECT
    if(number % 2 = 0, 1000, 2000) AS k,
    if(number % 2 = 0, nan, toFloat64(number)) AS v
FROM numbers(100);

SELECT 'all NaN group min';
SELECT k, min(v) FROM test_sharded_all_nan GROUP BY k ORDER BY k;

SELECT 'all NaN group max';
SELECT k, max(v) FROM test_sharded_all_nan GROUP BY k ORDER BY k;

DROP TABLE IF EXISTS test_sharded_nan;
DROP TABLE IF EXISTS test_sharded_all_nan;
