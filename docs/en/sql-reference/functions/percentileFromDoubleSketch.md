---
description: 'Extracts a specific percentile value from a serialized Quantiles DoubleSketch'
slug: /sql-reference/functions/percentileFromDoubleSketch
title: 'percentileFromDoubleSketch'
doc_type: 'reference'
---

# percentileFromDoubleSketch

Extracts the approximate value at a specified percentile from a serialized Quantiles DoubleSketch created by [serializedDoubleSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeddoublesketch) or [mergeSerializedDoubleSketch](/docs/en/sql-reference/aggregate-functions/reference/mergeserializeddoublesketch).

## Syntax

```sql
percentileFromDoubleSketch(sketch, percentile)
```

## Arguments

- `sketch` — Serialized Quantiles DoubleSketch. Type: [String](/docs/en/sql-reference/data-types/string.md).
- `percentile` — Percentile value between 0.0 and 1.0. Type: [Float64](/docs/en/sql-reference/data-types/float.md).
  - 0.0 = minimum value
  - 0.5 = median
  - 0.95 = 95th percentile
  - 0.99 = 99th percentile
  - 1.0 = maximum value

## Returned Value

- Approximate value at the specified percentile. Type: [Float64](/docs/en/sql-reference/data-types/float.md).
- Returns NaN if the sketch is empty or invalid.

## Implementation Details

- Uses the Apache DataSketches KLL algorithm for percentile estimation
- Provides rank-based guarantees on accuracy
- Very fast: typically microseconds regardless of dataset size
- Monotonic: percentileFromDoubleSketch(s, p1) ≤ percentileFromDoubleSketch(s, p2) when p1 ≤ p2

## Usage

### Basic Percentile Extraction

```sql
SELECT percentileFromDoubleSketch(latency_sketch, 0.95) AS p95_latency
FROM service_metrics
WHERE service = 'api';
```

### Multiple Percentiles

```sql
SELECT 
    service,
    percentileFromDoubleSketch(latency_sketch, 0.50) AS p50,
    percentileFromDoubleSketch(latency_sketch, 0.95) AS p95,
    percentileFromDoubleSketch(latency_sketch, 0.99) AS p99
FROM hourly_metrics
WHERE hour = toStartOfHour(now());
```

## Examples

### Example 1: Service Latency Percentiles

```sql
WITH sketch AS (
    SELECT serializedDoubleSketch(response_time_ms) AS s
    FROM requests
    WHERE service = 'api' AND date = today()
)
SELECT 
    percentileFromDoubleSketch(s, 0.50) AS median_ms,
    percentileFromDoubleSketch(s, 0.95) AS p95_ms,
    percentileFromDoubleSketch(s, 0.99) AS p99_ms,
    percentileFromDoubleSketch(s, 0.999) AS p999_ms
FROM sketch;
```

```response
┌─median_ms─┬──p95_ms─┬──p99_ms─┬─p999_ms─┐
│     45.2  │   156.8 │   287.3 │   521.7 │
└───────────┴─────────┴─────────┴─────────┘
```

### Example 2: Time-Series Percentile Analysis

```sql
SELECT 
    toStartOfHour(timestamp) AS hour,
    percentileFromDoubleSketch(
        serializedDoubleSketch(query_duration_ms), 
        0.95
    ) AS p95_query_duration
FROM query_log
WHERE timestamp >= now() - INTERVAL 24 HOUR
GROUP BY hour
ORDER BY hour;
```

### Example 3: Comparing Distributions

```sql
WITH 
    before AS (
        SELECT serializedDoubleSketch(latency_ms) AS sketch
        FROM requests
        WHERE date = '2024-01-01'
    ),
    after AS (
        SELECT serializedDoubleSketch(latency_ms) AS sketch
        FROM requests
        WHERE date = '2024-01-02'
    )
SELECT 
    percentileFromDoubleSketch((SELECT sketch FROM before), 0.50) AS before_p50,
    percentileFromDoubleSketch((SELECT sketch FROM before), 0.95) AS before_p95,
    percentileFromDoubleSketch((SELECT sketch FROM after), 0.50) AS after_p50,
    percentileFromDoubleSketch((SELECT sketch FROM after), 0.95) AS after_p95;
```

### Example 4: Merged Sketches Across Regions

```sql
WITH regional_sketches AS (
    SELECT 
        region,
        serializedDoubleSketch(response_time_ms) AS sketch
    FROM requests
    WHERE date = today()
    GROUP BY region
),
global_sketch AS (
    SELECT mergeSerializedDoubleSketch(sketch) AS merged
    FROM regional_sketches
)
SELECT 
    percentileFromDoubleSketch(merged, 0.50) AS global_median,
    percentileFromDoubleSketch(merged, 0.95) AS global_p95,
    percentileFromDoubleSketch(merged, 0.99) AS global_p99
FROM global_sketch;
```

### Example 5: Percentile Range

```sql
-- Get percentiles from 0% to 100% in 10% increments
SELECT 
    number / 10.0 AS percentile,
    percentileFromDoubleSketch(
        (SELECT serializedDoubleSketch(latency_ms) FROM requests), 
        number / 10.0
    ) AS value
FROM numbers(11);
```

## Performance Notes

- Constant time execution regardless of dataset size
- Can efficiently query many percentiles from the same sketch
- No memory overhead beyond the sketch itself

## Error Handling

- Invalid percentile values (< 0 or > 1) throw an exception
- Empty or corrupted sketches return NaN
- NULL sketches return NaN

## See Also

- [serializedDoubleSketch](/docs/en/sql-reference/aggregate-functions/reference/serializeddoublesketch) — Create quantiles sketches
- [mergeSerializedDoubleSketch](/docs/en/sql-reference/aggregate-functions/reference/mergeserializeddoublesketch) — Merge quantiles sketches
- [quantile](/docs/en/sql-reference/aggregate-functions/reference/quantile) — Direct percentile computation
