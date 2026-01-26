---
description: 'Creates a serialized Quantiles DoubleSketch for approximate percentile estimation'
slug: /sql-reference/aggregate-functions/reference/serializedDoubleSketch
title: 'serializedDoubleSketch'
doc_type: 'reference'
---

# serializedDoubleSketch

Creates a serialized Apache DataSketches Quantiles DoubleSketch from numeric values for approximate percentile estimation. The sketch can be stored, transmitted, or merged with other sketches for distributed percentile computation.

## Syntax

```sql
serializedDoubleSketch(expression)
```

## Arguments

- `expression` — Column expression with numeric values. Supported types: [Int](/docs/en/sql-reference/data-types/int-uint.md), [UInt](/docs/en/sql-reference/data-types/int-uint.md), [Float](/docs/en/sql-reference/data-types/float.md).

## Returned Value

- Serialized binary Quantiles DoubleSketch. Type: [String](/docs/en/sql-reference/data-types/string.md).

## Implementation Details

This function uses the Apache DataSketches Quantiles algorithm (KLL sketch), providing:
- Compact representation of value distribution
- Approximate percentile queries with provable error bounds
- Mergeable sketches for distributed computation
- Binary compatibility with other DataSketches implementations (Java, Python)

The sketch automatically adapts its internal structure based on the number of values, maintaining accuracy while minimizing memory usage.

## Usage

### Basic Usage

```sql
-- Create quantiles sketch for latency data
SELECT serializedDoubleSketch(response_time_ms) AS latency_sketch
FROM requests;
```

### Materialized View Pattern

```sql
-- Store sketches for time-series percentile analysis
CREATE MATERIALIZED VIEW hourly_latency_sketches
ENGINE = AggregatingMergeTree()
ORDER BY (service, hour)
AS SELECT
    service,
    toStartOfHour(timestamp) AS hour,
    serializedDoubleSketch(latency_ms) AS latency_sketch
FROM requests
GROUP BY service, hour;
```

## Examples

### Example 1: Basic Percentile Estimation

```sql
SELECT 
    percentileFromDoubleSketch(serializedDoubleSketch(number), 0.5) AS median,
    percentileFromDoubleSketch(serializedDoubleSketch(number), 0.95) AS p95,
    percentileFromDoubleSketch(serializedDoubleSketch(number), 0.99) AS p99
FROM numbers(1000);
```

```response
┌─median─┬───p95─┬───p99─┐
│    501 │ 950.5 │ 990.5 │
└────────┴───────┴───────┘
```

### Example 2: Service Latency Monitoring

```sql
WITH sketches AS (
    SELECT 
        service,
        serializedDoubleSketch(latency_ms) AS latency_sketch
    FROM requests
    WHERE timestamp >= now() - INTERVAL 1 HOUR
    GROUP BY service
)
SELECT 
    service,
    percentileFromDoubleSketch(latency_sketch, 0.50) AS p50_latency_ms,
    percentileFromDoubleSketch(latency_sketch, 0.95) AS p95_latency_ms,
    percentileFromDoubleSketch(latency_sketch, 0.99) AS p99_latency_ms
FROM sketches
ORDER BY service;
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
    percentileFromDoubleSketch((SELECT sketch FROM before), 0.95) AS before_p95,
    percentileFromDoubleSketch((SELECT sketch FROM after), 0.95) AS after_p95,
    (after_p95 - before_p95) / before_p95 * 100 AS percent_change;
```

## See Also

- [mergeSerializedDoubleSketch](/docs/en/sql-reference/aggregate-functions/reference/mergeserializeddoublesketch) — Merge multiple quantiles sketches
- [percentileFromDoubleSketch](/docs/en/sql-reference/functions/percentilefromdoublesketch) — Extract percentile from sketch
- [quantile](/docs/en/sql-reference/aggregate-functions/reference/quantile) — Native ClickHouse percentile function
- [quantileTDigest](/docs/en/sql-reference/aggregate-functions/reference/quantiletdigest) — Alternative percentile algorithm
