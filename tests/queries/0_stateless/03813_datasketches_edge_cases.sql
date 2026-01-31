-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- Test edge cases and error conditions

SELECT 'Test 1: Single value HLL';
SELECT cardinalityFromHLL(serializedHLL(42)) = 1;

SELECT 'Test 2: Single value quantile';
SELECT abs(percentileFromQuantiles(serializedQuantiles(42), 0.5) - 42) < 1;

SELECT 'Test 3: Two identical values';
SELECT cardinalityFromHLL(serializedHLL(if(number < 1000, 42, 42))) = 1
FROM numbers(1000);

SELECT 'Test 4: Very small dataset (1 value)';
SELECT cardinalityFromHLL(serializedHLL(number)) = 1 FROM numbers(1);

SELECT 'Test 5: Very small dataset (2 values)';
SELECT cardinalityFromHLL(serializedHLL(number)) BETWEEN 1 AND 3 FROM numbers(2);

SELECT 'Test 6: Extreme percentiles';
WITH sketch AS (SELECT serializedQuantiles(number) AS s FROM numbers(1000))
SELECT 
    percentileFromQuantiles(s, 0.001) < percentileFromQuantiles(s, 0.999) AS extremes_ordered
FROM sketch;

SELECT 'Test 7: All percentiles between 0 and 1';
DROP TABLE IF EXISTS test_percentile_range;
CREATE TABLE test_percentile_range (sketch String) ENGINE = Memory;
INSERT INTO test_percentile_range SELECT serializedQuantiles(number) AS s FROM numbers(100);
SELECT 
    percentileFromQuantiles((SELECT sketch FROM test_percentile_range), number / 100.0) BETWEEN 0 AND 100 AS in_range
FROM numbers(101)
WHERE number <= 100
LIMIT 1;
DROP TABLE test_percentile_range;

SELECT 'Test 8: Large integers (UInt64)';
SELECT cardinalityFromHLL(serializedHLL(toUInt64(18446744073709551615 - number))) BETWEEN 90 AND 110
FROM numbers(100);

SELECT 'Test 9: Negative numbers in quantiles';
WITH sketch AS (SELECT serializedQuantiles(toInt64(number) - 5000) AS s FROM numbers(10000))
SELECT 
    percentileFromQuantiles(s, 0.5) BETWEEN -500 AND 500 AS median_around_zero
FROM sketch;

SELECT 'Test 10: Very small floating point numbers';
WITH sketch AS (SELECT serializedQuantiles(number / 1000000.0) AS s FROM numbers(1000))
SELECT 
    percentileFromQuantiles(s, 0.5) BETWEEN 0.0004 AND 0.0006 AS small_floats_ok
FROM sketch;

SELECT 'Test 11: Mixed positive and negative';
WITH sketch AS (SELECT serializedQuantiles(number - 500) AS s FROM numbers(1000))
SELECT 
    percentileFromQuantiles(s, 0.0) < 0 AS min_negative,
    percentileFromQuantiles(s, 1.0) > 0 AS max_positive
FROM sketch;

SELECT 'Test 12: Sparse data (many NULLs)';
SELECT cardinalityFromHLL(serializedHLL(if(number % 100 = 0, number, NULL))) BETWEEN 5 AND 15
FROM numbers(1000);

SELECT 'Test 13: All NULLs HLL';
SELECT cardinalityFromHLL(serializedHLL(NULL::Nullable(UInt64))) = 0
FROM numbers(100);

SELECT 'Test 14: All NULLs quantiles';
SELECT isNaN(percentileFromQuantiles(serializedQuantiles(NULL::Nullable(Float64)), 0.5))
FROM numbers(100);

SELECT 'Test 15: String with special characters';
SELECT cardinalityFromHLL(serializedHLL(concat('user_', toString(number), '_テスト'))) BETWEEN 90 AND 110
FROM numbers(100);

SELECT 'Test 16: Empty string';
SELECT cardinalityFromHLL(serializedHLL('')) = 1
FROM numbers(100);

SELECT 'Test 17: Very long strings';
SELECT cardinalityFromHLL(serializedHLL(repeat('a', toUInt64(number % 1000)))) BETWEEN 90 AND 110
FROM numbers(100);

SELECT 'Test 18: Identical sketches produce identical results';
WITH 
    s1 AS (SELECT serializedHLL(number) AS sketch FROM numbers(100)),
    s2 AS (SELECT serializedHLL(number) AS sketch FROM numbers(100))
SELECT 
    cardinalityFromHLL((SELECT sketch FROM s1)) = 
    cardinalityFromHLL((SELECT sketch FROM s2)) AS identical_results;

SELECT 'Test 19: Merging order independence';
WITH 
    s1 AS (SELECT serializedHLL(number) AS sketch FROM numbers(100)),
    s2 AS (SELECT serializedHLL(number + 50) AS sketch FROM numbers(100)),
    merge1 AS (SELECT mergeSerializedHLL(sketch) AS m FROM (SELECT sketch FROM s1 UNION ALL SELECT sketch FROM s2)),
    merge2 AS (SELECT mergeSerializedHLL(sketch) AS m FROM (SELECT sketch FROM s2 UNION ALL SELECT sketch FROM s1))
SELECT 
    cardinalityFromHLL((SELECT m FROM merge1)) = 
    cardinalityFromHLL((SELECT m FROM merge2)) AS order_independent;

SELECT 'Test 20: Quantile monotonicity';
WITH sketch AS (SELECT serializedQuantiles(number) AS s FROM numbers(1000))
SELECT 
    percentileFromQuantiles(s, 0.1) <= percentileFromQuantiles(s, 0.2) AS p10_le_p20,
    percentileFromQuantiles(s, 0.2) <= percentileFromQuantiles(s, 0.3) AS p20_le_p30,
    percentileFromQuantiles(s, 0.3) <= percentileFromQuantiles(s, 0.4) AS p30_le_p40,
    percentileFromQuantiles(s, 0.4) <= percentileFromQuantiles(s, 0.5) AS p40_le_p50,
    percentileFromQuantiles(s, 0.5) <= percentileFromQuantiles(s, 0.6) AS p50_le_p60,
    percentileFromQuantiles(s, 0.6) <= percentileFromQuantiles(s, 0.7) AS p60_le_p70,
    percentileFromQuantiles(s, 0.7) <= percentileFromQuantiles(s, 0.8) AS p70_le_p80,
    percentileFromQuantiles(s, 0.8) <= percentileFromQuantiles(s, 0.9) AS p80_le_p90
FROM sketch;

SELECT 'Test 21: base64_encoded parameter equivalence';
WITH 
    sketch AS (SELECT serializedHLL(number) AS s FROM numbers(1000)),
    merge_default AS (SELECT mergeSerializedHLL(s) AS m FROM sketch),
    merge_explicit_0 AS (SELECT mergeSerializedHLL(0)(s) AS m FROM sketch),
    merge_explicit_1 AS (SELECT mergeSerializedHLL(1)(s) AS m FROM sketch)
SELECT 
    abs(cardinalityFromHLL((SELECT m FROM merge_default)) - 
        cardinalityFromHLL((SELECT m FROM merge_explicit_0))) < 10 AS default_eq_0,
    abs(cardinalityFromHLL((SELECT m FROM merge_default)) - 
        cardinalityFromHLL((SELECT m FROM merge_explicit_1))) < 10 AS default_eq_1
FROM (SELECT 1);

SELECT 'Test 22: Quantiles base64_encoded parameter';
WITH 
    sketch AS (SELECT serializedQuantiles(number) AS s FROM numbers(1000)),
    merge_default AS (SELECT mergeSerializedQuantiles(s) AS m FROM sketch),
    merge_explicit_0 AS (SELECT mergeSerializedQuantiles(0)(s) AS m FROM sketch)
SELECT 
    abs(percentileFromQuantiles((SELECT m FROM merge_default), 0.5) - 
        percentileFromQuantiles((SELECT m FROM merge_explicit_0), 0.5)) < 10 AS parameters_equivalent
FROM (SELECT 1);

SELECT 'Test 23: Very high cardinality';
SELECT cardinalityFromHLL(mergeSerializedHLL(sketch)) > 950000 AS high_cardinality_ok
FROM (SELECT serializedHLL(number) AS sketch FROM numbers(1000000));

SELECT 'Test 24: Percentile with uniform distribution';
WITH sketch AS (SELECT serializedQuantiles(rand() % 1000) AS s FROM numbers(100000))
SELECT 
    abs(percentileFromQuantiles(s, 0.5) - 500) / 500.0 < 0.1 AS uniform_median_ok
FROM sketch;

SELECT 'Test 25: Multiple aggregations in same query';
SELECT 
    cardinalityFromHLL(serializedHLL(number)) BETWEEN 90 AND 110 AS card1,
    cardinalityFromHLL(serializedHLL(number + 1000)) BETWEEN 90 AND 110 AS card2,
    percentileFromQuantiles(serializedQuantiles(number), 0.5) BETWEEN 40 AND 60 AS p50
FROM numbers(100);

-- Test edge cases for new optional parameters

SELECT 'Test 26: lg_k at minimum bound (4)';
SELECT cardinalityFromHLL(serializedHLL(4)(number)) BETWEEN 80 AND 120
FROM numbers(100);

SELECT 'Test 27: lg_k at maximum bound (21)';
SELECT cardinalityFromHLL(serializedHLL(21)(number)) BETWEEN 95 AND 105
FROM numbers(100);

SELECT 'Test 28: All HLL types with small dataset';
WITH 
    hll4 AS (SELECT serializedHLL(10, 'HLL_4')(number) AS s FROM numbers(10)),
    hll6 AS (SELECT serializedHLL(10, 'HLL_6')(number) AS s FROM numbers(10)),
    hll8 AS (SELECT serializedHLL(10, 'HLL_8')(number) AS s FROM numbers(10))
SELECT 
    cardinalityFromHLL((SELECT s FROM hll4)) = 10 AS hll4_exact,
    cardinalityFromHLL((SELECT s FROM hll6)) = 10 AS hll6_exact,
    cardinalityFromHLL((SELECT s FROM hll8)) = 10 AS hll8_exact
FROM (SELECT 1);

SELECT 'Test 29: Merge sketches with matching parameters';
WITH sketches AS (
    SELECT serializedHLL(12, 'HLL_4')(number) AS sketch FROM numbers(50)
    UNION ALL
    SELECT serializedHLL(12, 'HLL_4')(number + 25) AS sketch FROM numbers(50)
)
SELECT cardinalityFromHLL(mergeSerializedHLL(0, 12, 'HLL_4')(sketch)) BETWEEN 65 AND 85
FROM sketches;

SELECT 'Test 30: Parameter combinations';
WITH 
    default_sketch AS (SELECT serializedHLL(number) AS s FROM numbers(100)),
    custom_sketch AS (SELECT serializedHLL(12, 'HLL_8')(number) AS s FROM numbers(100))
SELECT 
    abs(cardinalityFromHLL((SELECT s FROM default_sketch)) - 
        cardinalityFromHLL((SELECT s FROM custom_sketch))) < 10 AS estimates_similar
FROM (SELECT 1);

SELECT 'Test 31: Higher lg_k with small dataset';
SELECT cardinalityFromHLL(serializedHLL(16)(number)) BETWEEN 8 AND 12
FROM numbers(10);

SELECT 'Test 32: Different types same cardinality estimate';
WITH data AS (SELECT number FROM numbers(500))
SELECT 
    abs(cardinalityFromHLL(serializedHLL(10, 'HLL_4')(number)) - 
        cardinalityFromHLL(serializedHLL(10, 'HLL_6')(number))) < 3 AS hll4_eq_hll6,
    abs(cardinalityFromHLL(serializedHLL(10, 'HLL_6')(number)) - 
        cardinalityFromHLL(serializedHLL(10, 'HLL_8')(number))) < 3 AS hll6_eq_hll8
FROM data;

SELECT 'Test 33: Merge with default vs explicit parameters';
WITH 
    sketches AS (
        SELECT serializedHLL(number) AS sketch FROM numbers(50)
        UNION ALL
        SELECT serializedHLL(number + 25) AS sketch FROM numbers(50)
    ),
    merge_default AS (SELECT mergeSerializedHLL(sketch) AS m FROM sketches),
    merge_explicit AS (SELECT mergeSerializedHLL(0, 10, 'HLL_4')(sketch) AS m FROM sketches)
SELECT 
    abs(cardinalityFromHLL((SELECT m FROM merge_default)) - 
        cardinalityFromHLL((SELECT m FROM merge_explicit))) < 5 AS results_equivalent
FROM (SELECT 1);
