-- Tags: no-fasttest
-- no-fasttest: requires datasketches library

-- Integration test: Complete HLL workflow
SELECT 'Test 1: Complete HLL workflow - daily to weekly aggregation';

DROP TABLE IF EXISTS daily_user_sketches;
CREATE TABLE daily_user_sketches (
    date Date,
    sketch String
) ENGINE = Memory;

-- Simulate 7 days of user activity
INSERT INTO daily_user_sketches
SELECT 
    toDate('2024-01-01') + outer.number AS date,
    serializedHLL(outer.number * 1000 + inner.number) AS sketch
FROM numbers(7) AS outer, numbers(1000) AS inner
GROUP BY date;

-- Get weekly cardinality
SELECT 
    cardinalityFromHLL(mergeSerializedHLL(sketch)) BETWEEN 6500 AND 7500 AS weekly_cardinality_ok
FROM daily_user_sketches;

DROP TABLE daily_user_sketches;

-- Integration test: Complete Quantiles workflow
SELECT 'Test 2: Complete Quantiles workflow - hourly to daily percentiles';

DROP TABLE IF EXISTS hourly_latency_sketches;
CREATE TABLE hourly_latency_sketches (
    hour DateTime,
    service String,
    sketch String
) ENGINE = Memory;

-- Simulate 24 hours of latency data for 3 services
INSERT INTO hourly_latency_sketches
SELECT 
    toDateTime('2024-01-01 00:00:00') + toIntervalHour(h.number) AS hour,
    s.service AS service,
    serializedQuantiles(rand() % 1000) AS sketch
FROM numbers(24) AS h, (SELECT arrayJoin(['api', 'db', 'cache']) AS service) AS s
GROUP BY hour, service;

-- Get daily percentiles per service
WITH daily_sketches AS (
    SELECT 
        service,
        mergeSerializedQuantiles(sketch) AS merged
    FROM hourly_latency_sketches
    GROUP BY service
)
SELECT 
    service,
    percentileFromQuantiles(merged, 0.5) BETWEEN 0 AND 1000 AS p50_ok,
    percentileFromQuantiles(merged, 0.95) BETWEEN 0 AND 1000 AS p95_ok,
    percentileFromQuantiles(merged, 0.99) BETWEEN 0 AND 1000 AS p99_ok
FROM daily_sketches
ORDER BY service;

DROP TABLE hourly_latency_sketches;

-- Test: Incremental sketch updates
SELECT 'Test 3: Incremental sketch updates';

DROP TABLE IF EXISTS incremental_sketches;
CREATE TABLE incremental_sketches (
    id UInt64,
    sketch String
) ENGINE = Memory;

-- Initial sketch
INSERT INTO incremental_sketches
SELECT 1 AS id, serializedHLL(number) AS sketch FROM numbers(1000);

-- Merge with new data
INSERT INTO incremental_sketches
SELECT 1 AS id, serializedHLL(number + 500) AS sketch FROM numbers(1000);

-- Get final cardinality (should be around 1500 due to overlap)
SELECT 
    cardinalityFromHLL(mergeSerializedHLL(sketch)) BETWEEN 1400 AND 1600 AS incremental_ok
FROM incremental_sketches;

DROP TABLE incremental_sketches;

-- Test: Multi-dimensional aggregation
SELECT 'Test 4: Multi-dimensional aggregation';

DROP TABLE IF EXISTS multi_dim_sketches;
CREATE TABLE multi_dim_sketches (
    region String,
    product String,
    sketch String
) ENGINE = Memory;

INSERT INTO multi_dim_sketches
SELECT 
    arrayJoin(['US', 'EU', 'ASIA']) AS region,
    arrayJoin(['A', 'B', 'C']) AS product,
    serializedHLL(number) AS sketch
FROM numbers(100)
GROUP BY region, product;

-- Aggregate by region only
SELECT 
    region,
    cardinalityFromHLL(mergeSerializedHLL(sketch)) BETWEEN 90 AND 110 AS cardinality_ok
FROM multi_dim_sketches
GROUP BY region
ORDER BY region;

-- Aggregate by product only
SELECT 
    product,
    cardinalityFromHLL(mergeSerializedHLL(sketch)) BETWEEN 90 AND 110 AS cardinality_ok
FROM multi_dim_sketches
GROUP BY product
ORDER BY product;

-- Global aggregate
SELECT 
    cardinalityFromHLL(mergeSerializedHLL(sketch)) BETWEEN 90 AND 110 AS global_cardinality_ok
FROM multi_dim_sketches;

DROP TABLE multi_dim_sketches;

-- Test: Time series rollup
SELECT 'Test 5: Time series rollup (minute -> hour -> day)';

DROP TABLE IF EXISTS minute_sketches;
CREATE TABLE minute_sketches (
    timestamp DateTime,
    sketch String
) ENGINE = Memory;

-- Simulate 1 day of minute-level data
INSERT INTO minute_sketches
SELECT 
    toDateTime('2024-01-01 00:00:00') + toIntervalMinute(outer.number) AS timestamp,
    serializedHLL(outer.number % 1000 + inner.number) AS sketch
FROM numbers(1440) AS outer, numbers(10) AS inner
GROUP BY timestamp;

-- Hourly rollup
WITH hourly AS (
    SELECT 
        toStartOfHour(timestamp) AS hour,
        mergeSerializedHLL(sketch) AS hourly_sketch
    FROM minute_sketches
    GROUP BY hour
)
SELECT 
    count() = 24 AS has_24_hours,
    cardinalityFromHLL(mergeSerializedHLL(hourly_sketch)) BETWEEN 900 AND 1100 AS daily_cardinality_ok
FROM hourly;

DROP TABLE minute_sketches;

-- Test: Sketch persistence and retrieval
SELECT 'Test 6: Sketch persistence and retrieval';

DROP TABLE IF EXISTS persistent_sketches;
CREATE TABLE persistent_sketches (
    id UInt64,
    sketch_hll String,
    sketch_quantiles String
) ENGINE = Memory;

INSERT INTO persistent_sketches
SELECT 
    1 AS id,
    serializedHLL(number) AS sketch_hll,
    serializedQuantiles(number) AS sketch_quantiles
FROM numbers(10000);

-- Retrieve and use stored sketches
SELECT 
    cardinalityFromHLL(sketch_hll) BETWEEN 9500 AND 10500 AS hll_ok,
    percentileFromQuantiles(sketch_quantiles, 0.5) BETWEEN 4500 AND 5500 AS quantile_ok
FROM persistent_sketches
WHERE id = 1;

DROP TABLE persistent_sketches;

-- Test: Combining sketches from different time periods
SELECT 'Test 7: Union of sketches from different time periods';

WITH 
    week1 AS (SELECT serializedHLL(number) AS sketch FROM numbers(1000)),
    week2 AS (SELECT serializedHLL(number + 500) AS sketch FROM numbers(1000)),
    week3 AS (SELECT serializedHLL(number + 1000) AS sketch FROM numbers(1000)),
    all_weeks AS (
        SELECT sketch FROM week1
        UNION ALL SELECT sketch FROM week2
        UNION ALL SELECT sketch FROM week3
    )
SELECT 
    cardinalityFromHLL(mergeSerializedHLL(sketch)) BETWEEN 1800 AND 2200 AS union_cardinality_ok
FROM all_weeks;

-- Test: Sketch size efficiency
SELECT 'Test 8: Sketch size efficiency';

WITH 
    small_sketch AS (SELECT length(serializedHLL(number)) AS s FROM numbers(100)),
    medium_sketch AS (SELECT length(serializedHLL(number)) AS s FROM numbers(10000)),
    large_sketch AS (SELECT length(mergeSerializedHLL(sketch)) AS s FROM (SELECT serializedHLL(number) AS sketch FROM numbers(1000000)))
SELECT 
    (SELECT s FROM small_sketch) AS size_small,
    (SELECT s FROM medium_sketch) AS size_medium,
    (SELECT s FROM large_sketch) AS size_large,
    size_large < 2000 AS size_bounded
FROM (SELECT 1);

-- Test: Quantiles with different distributions
SELECT 'Test 9: Quantiles with different distributions';

DROP TABLE IF EXISTS test_distributions;
CREATE TABLE test_distributions (
    dist_type String,
    sketch String
) ENGINE = Memory;

INSERT INTO test_distributions
SELECT 'uniform' AS dist_type, mergeSerializedQuantiles(sketch) AS sketch
FROM (SELECT serializedQuantiles(number) AS sketch FROM numbers(1000));

INSERT INTO test_distributions
SELECT 'skewed' AS dist_type, mergeSerializedQuantiles(sketch) AS sketch
FROM (SELECT serializedQuantiles(pow(number, 2)) AS sketch FROM numbers(100));

SELECT 
    percentileFromQuantiles((SELECT sketch FROM test_distributions WHERE dist_type = 'uniform'), 0.5) BETWEEN 400 AND 600 AS uniform_median_ok,
    percentileFromQuantiles((SELECT sketch FROM test_distributions WHERE dist_type = 'skewed'), 0.95) > 
    percentileFromQuantiles((SELECT sketch FROM test_distributions WHERE dist_type = 'skewed'), 0.5) AS skewed_increasing
FROM (SELECT 1);

DROP TABLE test_distributions;

-- Test: Error handling with empty results
SELECT 'Test 10: Empty result handling';

SELECT 
    cardinalityFromHLL(mergeSerializedHLL(sketch)) = 0 AS hll_empty_ok,
    isNaN(percentileFromQuantiles(mergeSerializedQuantiles(q_sketch), 0.5)) AS quantile_empty_ok
FROM (
    SELECT 
        serializedHLL(number) AS sketch,
        serializedQuantiles(number) AS q_sketch
    FROM numbers(0)
);

-- Test: High-precision business metrics workflow with custom parameters
SELECT 'Test 11: High-precision business metrics with custom parameters';

DROP TABLE IF EXISTS business_metrics;
CREATE TABLE business_metrics (
    date Date,
    region String,
    customer_sketch String,
    transaction_sketch String
) ENGINE = Memory;

-- Simulate business data for 3 regions over 5 days using higher precision
INSERT INTO business_metrics
SELECT 
    toDate('2024-01-01') + d.number AS date,
    r.region AS region,
    serializedHLL(14, 'HLL_4')(d.number * 10000 + r.region_id * 1000 + c.number) AS customer_sketch,
    serializedHLL(12, 'HLL_8')(d.number * 50000 + r.region_id * 5000 + t.number) AS transaction_sketch
FROM 
    numbers(5) AS d,
    (
        /* Important: keep region and region_id paired (no cartesian product). */
        SELECT
            tupleElement(x, 1) AS region,
            tupleElement(x, 2) AS region_id
        FROM
        (
            SELECT arrayJoin(arrayZip(['US', 'EU', 'APAC'], [1, 2, 3])) AS x
        )
    ) AS r,
    numbers(500) AS c,
    numbers(1000) AS t
GROUP BY date, region;

-- Aggregate by region with matching merge parameters
WITH regional_totals AS (
    SELECT 
        region,
        mergeSerializedHLL(14, 'HLL_4')(customer_sketch) AS merged_customers,
        mergeSerializedHLL(12, 'HLL_8')(transaction_sketch) AS merged_transactions
    FROM business_metrics
    GROUP BY region
)
SELECT 
    region,
    cardinalityFromHLL(merged_customers) BETWEEN 2400 AND 2600 AS customers_ok,
    cardinalityFromHLL(merged_transactions) BETWEEN 4800 AND 5200 AS transactions_ok
FROM regional_totals
ORDER BY region;

-- Get global totals
SELECT 
    cardinalityFromHLL(mergeSerializedHLL(14, 'HLL_4')(customer_sketch)) BETWEEN 7000 AND 8000 AS total_customers_ok,
    cardinalityFromHLL(mergeSerializedHLL(12, 'HLL_8')(transaction_sketch)) BETWEEN 14000 AND 16000 AS total_transactions_ok
FROM business_metrics;

DROP TABLE business_metrics;

-- Test: Compare default vs high-precision parameters
SELECT 'Test 12: Accuracy comparison - default vs high-precision';

WITH 
    default_precision AS (
        SELECT cardinalityFromHLL(serializedHLL(number)) AS estimate FROM numbers(10000)
    ),
    high_precision AS (
        SELECT cardinalityFromHLL(serializedHLL(14)(number)) AS estimate FROM numbers(10000)
    )
SELECT 
    abs((SELECT estimate FROM default_precision) - 10000) > 
    abs((SELECT estimate FROM high_precision) - 10000) AS high_precision_more_accurate
FROM (SELECT 1);
