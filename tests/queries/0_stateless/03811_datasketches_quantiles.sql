-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- Test serializedQuantiles with different data types
SELECT 'Test 1: serializedQuantiles with integers';
SELECT length(serializedQuantiles(number)) > 0 FROM numbers(1000);

SELECT 'Test 2: serializedQuantiles with Float64';
SELECT length(serializedQuantiles(toFloat64(number))) > 0 FROM numbers(1000);

SELECT 'Test 3: serializedQuantiles with UInt64';
SELECT length(serializedQuantiles(toUInt64(number))) > 0 FROM numbers(1000);

-- Test mergeSerializedQuantiles
SELECT 'Test 4: mergeSerializedQuantiles basic';
SELECT length(mergeSerializedQuantiles(sketch)) > 0 
FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(100));

SELECT 'Test 5: mergeSerializedQuantiles multiple sketches';
SELECT length(mergeSerializedQuantiles(sketch)) > 0 
FROM (
    SELECT serializedQuantiles(number) AS sketch FROM numbers(100)
    UNION ALL
    SELECT serializedQuantiles(number + 50) AS sketch FROM numbers(100)
);

-- Test mergeSerializedQuantiles with parameter
SELECT 'Test 6: mergeSerializedQuantiles with base64_encoded=0 (default, raw binary)';
SELECT length(mergeSerializedQuantiles(0)(sketch)) > 0 
FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(100));

SELECT 'Test 7: mergeSerializedQuantiles with base64_encoded=1 (check for base64)';
SELECT length(mergeSerializedQuantiles(1)(sketch)) > 0 
FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(100));

-- Test percentileFromQuantiles
SELECT 'Test 8: percentileFromQuantiles p50 (median)';
SELECT 
    percentileFromQuantiles(serializedQuantiles(number), 0.5) BETWEEN 450 AND 550
FROM numbers(1000);

SELECT 'Test 9: percentileFromQuantiles p95';
SELECT 
    percentileFromQuantiles(serializedQuantiles(number), 0.95) BETWEEN 900 AND 1000
FROM numbers(1000);

SELECT 'Test 10: percentileFromQuantiles p99';
SELECT 
    percentileFromQuantiles(serializedQuantiles(number), 0.99) BETWEEN 950 AND 1000
FROM numbers(1000);

-- Test percentiles from merged sketch
SELECT 'Test 11: Percentiles from merged sketch';
WITH 
    merged AS (
        SELECT mergeSerializedQuantiles(sketch) AS s
        FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(1000))
    )
SELECT 
    percentileFromQuantiles(s, 0.5) BETWEEN 450 AND 550 AS p50_ok,
    percentileFromQuantiles(s, 0.95) BETWEEN 900 AND 1000 AS p95_ok
FROM merged;

-- Test empty sketch
SELECT 'Test 12: percentileFromQuantiles empty sketch';
SELECT isNaN(percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.5))
FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(0));

-- Test boundary percentiles
SELECT 'Test 13: Boundary percentiles (p0, p1)';
WITH 
    sketch AS (SELECT serializedQuantiles(number) AS s FROM numbers(100))
SELECT 
    percentileFromQuantiles(s, 0.0) <= 10 AS p0_ok,
    percentileFromQuantiles(s, 1.0) >= 90 AS p100_ok
FROM sketch;

-- Test with GROUP BY
SELECT 'Test 14: GROUP BY with serializedQuantiles';
SELECT 
    key,
    length(serializedQuantiles(value)) > 0 AS has_sketch
FROM (
    SELECT number % 3 AS key, number AS value FROM numbers(300)
)
GROUP BY key
ORDER BY key;

-- Test merging across groups and extracting percentiles
SELECT 'Test 15: Merge sketches from groups and get percentiles';
WITH sketches AS (
    SELECT serializedQuantiles(number) AS sketch
    FROM numbers(1000)
    GROUP BY number % 10
)
SELECT 
    percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.5) BETWEEN 450 AND 550 AS median_ok
FROM sketches;

-- Test with NULL values (should be ignored)
SELECT 'Test 16: Handle NULL values';
WITH 
    sketch AS (
        SELECT serializedQuantiles(if(number % 10 = 0, NULL, number)) AS s
        FROM numbers(100)
    )
SELECT 
    percentileFromQuantiles(s, 0.5) BETWEEN 40 AND 60 AS median_ok
FROM sketch;

-- Test multiple percentiles from same sketch
SELECT 'Test 17: Multiple percentiles from same sketch';
WITH 
    sketch AS (SELECT mergeSerializedQuantiles(s) AS merged FROM (SELECT serializedQuantiles(number) AS s FROM numbers(1000)))
SELECT 
    percentileFromQuantiles(merged, 0.25) < percentileFromQuantiles(merged, 0.50) AS q1_lt_q2,
    percentileFromQuantiles(merged, 0.50) < percentileFromQuantiles(merged, 0.75) AS q2_lt_q3,
    percentileFromQuantiles(merged, 0.75) < percentileFromQuantiles(merged, 0.95) AS q3_lt_p95
FROM sketch;

-- Test with negative numbers
SELECT 'Test 18: Negative numbers';
WITH 
    sketch AS (SELECT serializedQuantiles(number - 500) AS s FROM numbers(1000))
SELECT 
    percentileFromQuantiles(s, 0.5) BETWEEN -50 AND 50 AS median_around_zero
FROM sketch;

-- Test with floating point numbers
SELECT 'Test 19: Floating point numbers';
WITH 
    sketch AS (SELECT serializedQuantiles(number / 10.0) AS s FROM numbers(1000))
SELECT 
    percentileFromQuantiles(s, 0.5) BETWEEN 40 AND 60 AS median_ok
FROM sketch;

-- Test accuracy of percentile estimation
SELECT 'Test 20: Percentile accuracy check';
WITH 
    sketch AS (SELECT mergeSerializedQuantiles(s) AS merged FROM (SELECT serializedQuantiles(number) AS s FROM numbers(10000)))
SELECT 
    abs(percentileFromQuantiles(merged, 0.5) - 5000) / 5000.0 < 0.05 AS p50_within_5_percent,
    abs(percentileFromQuantiles(merged, 0.95) - 9500) / 9500.0 < 0.05 AS p95_within_5_percent
FROM sketch;

-- Test merging sketches from different distributions
SELECT 'Test 21: Merge sketches from different distributions';
WITH 
    sketch1 AS (SELECT serializedQuantiles(number) AS s FROM numbers(1000)),
    sketch2 AS (SELECT serializedQuantiles(number * 10) AS s FROM numbers(1000)),
    merged AS (SELECT mergeSerializedQuantiles(s) AS m FROM (SELECT s FROM sketch1 UNION ALL SELECT s FROM sketch2))
SELECT 
    percentileFromQuantiles(m, 0.5) > 0 AS has_median,
    percentileFromQuantiles(m, 0.95) > percentileFromQuantiles(m, 0.5) AS p95_gt_p50
FROM merged;

-- Test sketch size with different cardinalities
SELECT 'Test 22: Sketch size is bounded';
WITH 
    size_small AS (SELECT length(serializedQuantiles(number)) AS s FROM numbers(10)),
    size_large AS (SELECT length(mergeSerializedQuantiles(sketch)) AS s FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(100000)))
SELECT 
    (SELECT s FROM size_small) AS small,
    (SELECT s FROM size_large) AS large,
    large < 10000 AS size_reasonable
FROM (SELECT 1);

-- Test with constant values
SELECT 'Test 23: Constant values';
WITH 
    sketch AS (SELECT serializedQuantiles(42) AS s FROM numbers(1000))
SELECT 
    abs(percentileFromQuantiles(s, 0.5) - 42) < 1 AS median_is_42
FROM sketch;

-- Test percentile parameter validation (0.0 to 1.0 range)
SELECT 'Test 24: Valid percentile range 0.01';
SELECT percentileFromQuantiles(serializedQuantiles(number), 0.01) >= 0
FROM numbers(1000);

SELECT 'Test 25: Valid percentile range 0.99';
SELECT percentileFromQuantiles(serializedQuantiles(number), 0.99) < 1000
FROM numbers(1000);
