---
description: 'Extracts a percentile value from a serialized TDigest sketch'
slug: /sql-reference/functions/percentilefromtdigest
title: 'percentileFromTDigest'
---

# percentileFromTDigest

Extracts a percentile value from a serialized Apache DataSketches TDigest sketch created by `serializedTDigest` or `mergeSerializedTDigest`. TDigest provides high accuracy for extreme percentiles.

## Syntax

```sql
percentileFromTDigest(sketch, percentile)
```

## Arguments

- `sketch` — Serialized TDigest sketch. Type: [String](../../sql-reference/data-types/string).
- `percentile` — Percentile value between 0.0 and 1.0. Type: [Float64](../../sql-reference/data-types/float).

## Returned Value

- Estimated percentile value. Type: [Float64](../../sql-reference/data-types/float).
- Returns `NaN` if the sketch is empty.

## Examples

### Example 1: Extreme Percentiles

```sql
WITH sketch AS (
    SELECT serializedTDigest(response_time_ms) AS tdigest
    FROM requests
    WHERE service = 'api'
)
SELECT 
    percentileFromTDigest(tdigest, 0.50) AS p50,
    percentileFromTDigest(tdigest, 0.99) AS p99,
    percentileFromTDigest(tdigest, 0.999) AS p999,
    percentileFromTDigest(tdigest, 0.9999) AS p9999
FROM sketch;
```

```response
┌───p50─┬───p99─┬──p999─┬─p9999─┐
│ 45.2  │ 234.5 │ 456.7 │ 789.1 │
└───────┴───────┴───────┴───────┘
```

### Example 2: SLA Monitoring

```sql
WITH daily_sketch AS (
    SELECT 
        date,
        service,
        mergeSerializedTDigest(hourly_tdigest) AS daily_tdigest
    FROM hourly_metrics
    WHERE date >= today() - 7
    GROUP BY date, service
)
SELECT 
    service,
    avg(percentileFromTDigest(daily_tdigest, 0.95)) AS avg_p95,
    max(percentileFromTDigest(daily_tdigest, 0.95)) AS max_p95,
    avg(percentileFromTDigest(daily_tdigest, 0.99)) AS avg_p99,
    max(percentileFromTDigest(daily_tdigest, 0.99)) AS max_p99
FROM daily_sketch
GROUP BY service;
```

### Example 3: Cross-Region Comparison

```sql
WITH regional_sketches AS (
    SELECT 
        region,
        serializedTDigest(latency_ms) AS sketch
    FROM requests
    WHERE timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY region
)
SELECT 
    region,
    percentileFromTDigest(sketch, 0.50) AS median,
    percentileFromTDigest(sketch, 0.95) AS p95,
    percentileFromTDigest(sketch, 0.99) AS p99,
    percentileFromTDigest(sketch, 0.999) AS p999
FROM regional_sketches
ORDER BY p99 DESC;
```

## Implementation Details

TDigest is optimized for:
- High accuracy at extreme percentiles (p99, p99.9, p99.99+)
- Bounded memory usage regardless of data size
- Fast percentile queries

Use TDigest when:
- You need accurate tail latencies (p99+)
- SLA monitoring requires precise percentile values
- Analyzing outliers and extreme values

Use Quantiles sketches instead when:
- General percentile estimation (p50, p75, p90)
- Memory efficiency is critical
- Standard latency monitoring

## See Also

- [serializedTDigest](../../sql-reference/aggregate-functions/reference/serializedtdigest) — Create TDigest sketch
- [mergeSerializedTDigest](../../sql-reference/aggregate-functions/reference/mergeserializedtdigest) — Merge TDigest sketches
- [centroidsFromTDigest](../../sql-reference/functions/centroidsfromtdigest) — Extract centroids from TDigest
- [percentileFromQuantiles](../../sql-reference/functions/percentilefromquantiles) — Alternative using Quantiles sketches
