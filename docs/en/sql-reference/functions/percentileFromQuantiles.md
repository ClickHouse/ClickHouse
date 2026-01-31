---
description: 'Extracts a percentile value from a serialized Quantiles sketch'
slug: /sql-reference/functions/percentilefromquantiles
title: 'percentileFromQuantiles'
---

# percentileFromQuantiles

Extracts a percentile value from a serialized Apache DataSketches Quantiles sketch created by `serializedQuantiles` or `mergeSerializedQuantiles`.

## Syntax

```sql
percentileFromQuantiles(sketch, percentile)
```

## Arguments

- `sketch` — Serialized Quantiles sketch. Type: [String](../../sql-reference/data-types/string).
- `percentile` — Percentile value between 0.0 and 1.0. Type: [Float64](../../sql-reference/data-types/float).

## Returned Value

- Estimated percentile value. Type: [Float64](../../sql-reference/data-types/float).
- Returns `NaN` if the sketch is empty.

## Examples

### Example 1: Basic Percentile Extraction

```sql
WITH sketch AS (
    SELECT serializedQuantiles(number) AS s
    FROM numbers(1000)
)
SELECT 
    percentileFromQuantiles(s, 0.50) AS median,
    percentileFromQuantiles(s, 0.95) AS p95,
    percentileFromQuantiles(s, 0.99) AS p99
FROM sketch;
```

```response
┌─median─┬───p95─┬───p99─┐
│  499.5 │ 950.5 │ 990.5 │
└────────┴───────┴───────┘
```

### Example 2: Service Latency Analysis

```sql
WITH sketches AS (
    SELECT 
        service,
        serializedQuantiles(latency_ms) AS sketch
    FROM requests
    WHERE timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY service
)
SELECT 
    service,
    percentileFromQuantiles(sketch, 0.50) AS p50_ms,
    percentileFromQuantiles(sketch, 0.95) AS p95_ms,
    percentileFromQuantiles(sketch, 0.99) AS p99_ms
FROM sketches
ORDER BY p99_ms DESC;
```

### Example 3: Multiple Percentiles from Merged Sketch

```sql
WITH merged AS (
    SELECT mergeSerializedQuantiles(daily_sketch) AS weekly
    FROM daily_latency_table
    WHERE date >= toStartOfWeek(now())
)
SELECT 
    percentileFromQuantiles(weekly, 0.25) AS p25,
    percentileFromQuantiles(weekly, 0.50) AS p50,
    percentileFromQuantiles(weekly, 0.75) AS p75,
    percentileFromQuantiles(weekly, 0.90) AS p90,
    percentileFromQuantiles(weekly, 0.95) AS p95,
    percentileFromQuantiles(weekly, 0.99) AS p99
FROM merged;
```

### Example 4: Compare Before/After

```sql
WITH 
    before AS (
        SELECT serializedQuantiles(latency_ms) AS sketch
        FROM requests
        WHERE date = today() - 1
    ),
    after AS (
        SELECT serializedQuantiles(latency_ms) AS sketch
        FROM requests
        WHERE date = today()
    )
SELECT 
    percentileFromQuantiles((SELECT sketch FROM before), 0.95) AS yesterday_p95,
    percentileFromQuantiles((SELECT sketch FROM after), 0.95) AS today_p95,
    (today_p95 - yesterday_p95) / yesterday_p95 * 100 AS change_percent;
```

## See Also

- [serializedQuantiles](../../sql-reference/aggregate-functions/reference/serializedquantiles) — Create Quantiles sketch
- [mergeSerializedQuantiles](../../sql-reference/aggregate-functions/reference/mergeserializedquantiles) — Merge Quantiles sketches
- [percentileFromTDigest](../../sql-reference/functions/percentilefromtdigest) — Alternative for TDigest sketches
- [quantile](../../sql-reference/aggregate-functions/reference/quantile) — Native ClickHouse percentile function
