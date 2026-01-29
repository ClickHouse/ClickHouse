---
description: 'Creates a serialized Quantiles sketch for approximate percentile estimation'
slug: /sql-reference/aggregate-functions/reference/serializedQuantiles
title: 'serializedQuantiles'
doc_type: 'reference'
---

# serializedQuantiles

Creates a serialized Apache DataSketches Quantiles sketch from numeric values for approximate percentile estimation. The sketch can be stored, transmitted, or merged with other sketches for distributed percentile computation.

## Syntax {#syntax}

```sql
serializedQuantiles(expression)
```

## Arguments {#arguments}

- `expression` — Column expression with numeric values. Supported types: [Int](../../../sql-reference/data-types/int-uint), [UInt](../../../sql-reference/data-types/int-uint), [Float](../../../sql-reference/data-types/float).

## Returned Value {#returned-value}

- Serialized binary Quantiles sketch. Type: [String](../../../sql-reference/data-types/string).

## Implementation Details {#implementation-details}

This function uses the Apache DataSketches Quantiles algorithm (KLL sketch), providing:
- Compact representation of value distribution
- Approximate percentile queries with provable error bounds
- Mergeable sketches for distributed computation
- Binary compatibility with other DataSketches implementations (Java, Python)

The sketch automatically adapts its internal structure based on the number of values, maintaining accuracy while minimizing memory usage.

## Usage {#usage}

### Basic Usage {#basic-usage}

```sql
-- Create quantiles sketch for latency data
SELECT serializedQuantiles(response_time_ms) AS latency_sketch
FROM requests;
```

### Materialized View Pattern {#materialized-view-pattern}

```sql
-- Store sketches for time-series percentile analysis
CREATE MATERIALIZED VIEW hourly_latency_sketches
ENGINE = AggregatingMergeTree()
ORDER BY (service, hour)
AS SELECT
    service,
    toStartOfHour(timestamp) AS hour,
    serializedQuantiles(latency_ms) AS latency_sketch
FROM requests
GROUP BY service, hour;
```

## Examples {#examples}

### Example 1: Basic Percentile Estimation {#example-1-basic-percentile-estimation}

```sql
SELECT 
    percentileFromQuantiles(serializedQuantiles(number), 0.5) AS median,
    percentileFromQuantiles(serializedQuantiles(number), 0.95) AS p95,
    percentileFromQuantiles(serializedQuantiles(number), 0.99) AS p99
FROM numbers(1000);
```

```response
┌─median─┬───p95─┬───p99─┐
│    501 │ 950.5 │ 990.5 │
└────────┴───────┴───────┘
```

### Example 2: Service Latency Monitoring {#example-2-service-latency-monitoring}

```sql
WITH sketches AS (
    SELECT 
        service,
        serializedQuantiles(latency_ms) AS latency_sketch
    FROM requests
    WHERE timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY service
)
SELECT 
    service,
    percentileFromQuantiles(latency_sketch, 0.50) AS p50_latency_ms,
    percentileFromQuantiles(latency_sketch, 0.95) AS p95_latency_ms,
    percentileFromQuantiles(latency_sketch, 0.99) AS p99_latency_ms
FROM sketches
ORDER BY service;
```

### Example 3: Comparing Distributions {#example-3-comparing-distributions}

```sql
WITH 
    before AS (
        SELECT serializedQuantiles(latency_ms) AS sketch
        FROM requests
        WHERE date = '2024-01-01'
    ),
    after AS (
        SELECT serializedQuantiles(latency_ms) AS sketch
        FROM requests
        WHERE date = '2024-01-02'
    )
SELECT 
    percentileFromQuantiles((SELECT sketch FROM before), 0.95) AS before_p95,
    percentileFromQuantiles((SELECT sketch FROM after), 0.95) AS after_p95,
    (after_p95 - before_p95) / before_p95 * 100 AS percent_change;
```

## See Also {#see-also}

- [mergeSerializedQuantiles](../../../sql-reference/aggregate-functions/reference/mergeserializedquantiles) — Merge multiple quantiles sketches
- [percentileFromQuantiles](../../../sql-reference/functions/percentilefromquantiles) — Extract percentile from sketch
- [quantile](../../../sql-reference/aggregate-functions/reference/quantile) — Native ClickHouse percentile function
- [quantileTDigest](../../../sql-reference/aggregate-functions/reference/quantiletdigest) — Alternative percentile algorithm
