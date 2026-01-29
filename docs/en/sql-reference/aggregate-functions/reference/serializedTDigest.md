---
description: 'Creates a serialized TDigest sketch for accurate percentile estimation, especially at extremes'
slug: /sql-reference/aggregate-functions/reference/serializedTDigest
title: 'serializedTDigest'
doc_type: 'reference'
---

# serializedTDigest

Creates a serialized Apache DataSketches TDigest sketch from numeric values for accurate percentile estimation, particularly at the extremes (p99, p99.9, etc.). The sketch can be stored, transmitted, or merged with other sketches.

## Syntax {#syntax}

```sql
serializedTDigest(expression)
```

## Arguments {#arguments}

- `expression` — Numeric expression. Supported types: [Int](../../../sql-reference/data-types/int-uint), [UInt](../../../sql-reference/data-types/int-uint), [Float](../../../sql-reference/data-types/float).

## Returned Value {#returned-value}

- Serialized binary TDigest sketch. Type: [String](../../../sql-reference/data-types/string).

## Implementation Details {#implementation-details}

TDigest provides:
- High accuracy for extreme percentiles (p99, p99.9, p99.99)
- Compact sketch representation
- Mergeable for distributed computation
- Binary compatibility with Apache DataSketches implementations

## Examples {#examples}

### Example 1: Create and Store TDigest Sketch {#example-1-create-and-store-tdigest-sketch}

```sql
SELECT serializedTDigest(response_time_ms) AS tdigest_sketch
FROM requests
WHERE service = 'api';
```

### Example 2: Extract Percentiles {#example-2-extract-percentiles}

```sql
WITH sketch AS (
    SELECT serializedTDigest(latency_ms) AS tdigest
    FROM requests
)
SELECT 
    percentileFromTDigest(tdigest, 0.50) AS p50,
    percentileFromTDigest(tdigest, 0.99) AS p99,
    percentileFromTDigest(tdigest, 0.999) AS p999
FROM sketch;
```

### Example 3: Merge Across Time Periods {#example-3-merge-across-time-periods}

```sql
WITH merged AS (
    SELECT mergeSerializedTDigest(daily_sketch) AS weekly_sketch
    FROM daily_tdigest_table
    WHERE date >= toStartOfWeek(now())
)
SELECT 
    percentileFromTDigest(weekly_sketch, 0.99) AS weekly_p99
FROM merged;
```

## See Also {#see-also}

- [mergeSerializedTDigest](../../../sql-reference/aggregate-functions/reference/mergeserializedtdigest) — Merge TDigest sketches
- [percentileFromTDigest](../../../sql-reference/functions/percentilefromtdigest) — Extract percentile from TDigest
- [centroidsFromTDigest](../../../sql-reference/functions/centroidsfromtdigest) — Extract centroids from TDigest
- [quantileTDigest](../../../sql-reference/aggregate-functions/reference/quantiletdigest) — Native ClickHouse TDigest percentile
- [serializedQuantiles](../../../sql-reference/aggregate-functions/reference/serializedquantiles) — Alternative quantiles algorithm
