-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- Test serializedHLL with different data types
SELECT 'Test 1: serializedHLL with integers';
SELECT length(serializedHLL(number)) > 0 FROM numbers(1000);

SELECT 'Test 2: serializedHLL with strings';
SELECT length(serializedHLL(toString(number))) > 0 FROM numbers(1000);

SELECT 'Test 3: serializedHLL with UInt64';
SELECT length(serializedHLL(toUInt64(number))) > 0 FROM numbers(1000);

-- Test mergeSerializedHLL
SELECT 'Test 4: mergeSerializedHLL basic';
SELECT length(mergeSerializedHLL(sketch)) > 0 
FROM (SELECT serializedHLL(number) AS sketch FROM numbers(100));

SELECT 'Test 5: mergeSerializedHLL multiple sketches';
SELECT length(mergeSerializedHLL(sketch)) > 0 
FROM (
    SELECT serializedHLL(number) AS sketch FROM numbers(100)
    UNION ALL
    SELECT serializedHLL(number + 50) AS sketch FROM numbers(100)
);

-- Test mergeSerializedHLL with parameter
SELECT 'Test 6: mergeSerializedHLL with assume_raw_binary=1';
SELECT length(mergeSerializedHLL(1)(sketch)) > 0 
FROM (SELECT serializedHLL(number) AS sketch FROM numbers(100));

SELECT 'Test 7: mergeSerializedHLL with assume_raw_binary=0';
SELECT length(mergeSerializedHLL(0)(sketch)) > 0 
FROM (SELECT serializedHLL(number) AS sketch FROM numbers(100));

-- Test cardinalityFromHLL
SELECT 'Test 8: cardinalityFromHLL basic';
SELECT cardinalityFromHLL(serializedHLL(number)) BETWEEN 90 AND 110
FROM numbers(100);

SELECT 'Test 9: cardinalityFromHLL with merged sketch';
SELECT cardinalityFromHLL(mergeSerializedHLL(sketch)) BETWEEN 900 AND 1100
FROM (SELECT serializedHLL(number) AS sketch FROM numbers(1000));

SELECT 'Test 10: cardinalityFromHLL empty sketch';
SELECT cardinalityFromHLL(mergeSerializedHLL(sketch)) AS cardinality
FROM (SELECT serializedHLL(number) AS sketch FROM numbers(0));

-- Test accuracy with known cardinalities
SELECT 'Test 11: Cardinality accuracy for 1000 distinct values';
WITH 
    sketch AS (SELECT mergeSerializedHLL(s) AS merged FROM (SELECT serializedHLL(number) AS s FROM numbers(1000)))
SELECT 
    cardinalityFromHLL(merged) AS estimated,
    abs(estimated - 1000) / 1000.0 < 0.05 AS within_5_percent
FROM sketch;

SELECT 'Test 12: Cardinality accuracy for 10000 distinct values';
WITH 
    sketch AS (SELECT mergeSerializedHLL(s) AS merged FROM (SELECT serializedHLL(number) AS s FROM numbers(10000)))
SELECT 
    cardinalityFromHLL(merged) AS estimated,
    abs(estimated - 10000) / 10000.0 < 0.05 AS within_5_percent
FROM sketch;

-- Test with GROUP BY
SELECT 'Test 13: GROUP BY with serializedHLL';
SELECT 
    key,
    length(serializedHLL(value)) > 0 AS has_sketch
FROM (
    SELECT number % 3 AS key, number AS value FROM numbers(300)
)
GROUP BY key
ORDER BY key;

-- Test merging across groups
SELECT 'Test 14: Merge sketches from groups';
WITH sketches AS (
    SELECT 
        number % 3 AS key,
        serializedHLL(number) AS sketch
    FROM numbers(300)
    GROUP BY key
)
SELECT cardinalityFromHLL(mergeSerializedHLL(sketch)) BETWEEN 270 AND 330
FROM sketches;

-- Test with NULL values (should be ignored)
SELECT 'Test 15: Handle NULL values';
SELECT cardinalityFromHLL(mergeSerializedHLL(sketch)) BETWEEN 90 AND 110
FROM (
    SELECT serializedHLL(if(number % 10 = 0, NULL, number)) AS sketch
    FROM numbers(100)
);

-- Test determinism (same input produces same sketch)
SELECT 'Test 16: Determinism check';
WITH 
    sketch1 AS (SELECT serializedHLL(number) AS s FROM numbers(100)),
    sketch2 AS (SELECT serializedHLL(number) AS s FROM numbers(100))
SELECT (SELECT s FROM sketch1) = (SELECT s FROM sketch2) AS sketches_equal;

-- Test union of disjoint sets
SELECT 'Test 17: Union of disjoint sets';
WITH 
    sketch1 AS (SELECT serializedHLL(number) AS s FROM numbers(100)),
    sketch2 AS (SELECT serializedHLL(number + 100) AS s FROM numbers(100)),
    merged AS (SELECT mergeSerializedHLL(s) AS m FROM (SELECT s FROM sketch1 UNION ALL SELECT s FROM sketch2))
SELECT cardinalityFromHLL(m) BETWEEN 180 AND 220 FROM merged;

-- Test union of overlapping sets
SELECT 'Test 18: Union of overlapping sets';
WITH 
    sketch1 AS (SELECT serializedHLL(number) AS s FROM numbers(100)),
    sketch2 AS (SELECT serializedHLL(number + 50) AS s FROM numbers(100)),
    merged AS (SELECT mergeSerializedHLL(s) AS m FROM (SELECT s FROM sketch1 UNION ALL SELECT s FROM sketch2))
SELECT cardinalityFromHLL(m) BETWEEN 130 AND 170 FROM merged;

-- Test with very large cardinality
SELECT 'Test 19: Large cardinality (100K distinct values)';
WITH 
    sketch AS (SELECT mergeSerializedHLL(s) AS merged FROM (SELECT serializedHLL(number) AS s FROM numbers(100000)))
SELECT 
    cardinalityFromHLL(merged) BETWEEN 95000 AND 105000 AS within_range
FROM sketch;

-- Test sketch size consistency
SELECT 'Test 20: Sketch size is bounded';
WITH 
    size_small AS (SELECT length(serializedHLL(number)) AS s FROM numbers(10)),
    size_large AS (SELECT length(mergeSerializedHLL(sketch)) AS s FROM (SELECT serializedHLL(number) AS sketch FROM numbers(100000)))
SELECT 
    (SELECT s FROM size_small) AS small,
    (SELECT s FROM size_large) AS large,
    abs(small - large) < 1000 AS sizes_similar
FROM (SELECT 1);
