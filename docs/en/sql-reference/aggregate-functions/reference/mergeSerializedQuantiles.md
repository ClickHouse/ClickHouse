---
description: 'Merges multiple serialized Quantiles sketches for distributed percentile computation'
slug: /sql-reference/aggregate-functions/reference/mergeSerializedQuantiles
title: 'mergeSerializedQuantiles'
doc_type: 'reference'
---

# mergeSerializedQuantiles

Merges multiple Apache DataSketches Quantiles sketches into a single sketch. This enables distributed percentile computation across shards, time periods, or dimensions.

## Syntax {#syntax}

```sql
mergeSerializedQuantiles([base64_encoded])(sketch)
```

## Arguments {#arguments}

- `base64_encoded` (optional) — Boolean flag (0 or 1) to control base64 decoding. Default: 0 (raw binary).
  - `0` (default): Input is raw binary sketch data (most common)
  - `1`: Input is base64-encoded and will be decoded before merging
- `sketch` — Column containing serialized Quantiles sketches. Type: [String](../../../sql-reference/data-types/string).

## Returned Value {#returned-value}

- Merged serialized Quantiles sketch. Type: [String](../../../sql-reference/data-types/string).

## Implementation Details {#implementation-details}

The merge operation is:
- **Commutative**: Order doesn't matter
- **Associative**: Can merge in any grouping
- **Idempotent**: Merging same sketch multiple times is safe

This makes it ideal for distributed aggregation in ClickHouse.

## Examples {#examples}

### Example 1: Merge Daily Sketches into Weekly {#example-1-merge-daily-sketches-into-weekly}

```sql
WITH daily_sketches AS (
    SELECT 
        toDate(timestamp) AS date,
        serializedQuantiles(latency_ms) AS sketch
    FROM requests
    WHERE timestamp >= toStartOfWeek(now())
    GROUP BY date
)
SELECT 
    percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.95) AS weekly_p95
FROM daily_sketches;
```

### Example 2: Cross-Shard Aggregation {#example-2-cross-shard-aggregation}

```sql
-- Aggregate from multiple distributed shards
SELECT 
    service,
    percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.50) AS p50,
    percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.95) AS p95,
    percentileFromQuantiles(mergeSerializedQuantiles(sketch), 0.99) AS p99
FROM distributed_latency_table
GROUP BY service;
```

### Example 3: Time Series Rollup {#example-3-time-series-rollup}

```sql
-- Rollup hourly -> daily -> weekly
CREATE MATERIALIZED VIEW daily_latency_rollup
ENGINE = AggregatingMergeTree()
ORDER BY (service, date)
AS SELECT
    service,
    toDate(hour) AS date,
    mergeSerializedQuantiles(hourly_sketch) AS daily_sketch
FROM hourly_latency_sketches
GROUP BY service, date;
```

### Example 4: Base64-Encoded Input {#example-4-base64-encoded-input}

```sql
-- Merge sketches stored as base64 strings
SELECT 
    percentileFromQuantiles(
        mergeSerializedQuantiles(1)(base64_sketch), 
        0.95
    ) AS p95
FROM external_sketches
WHERE source = 'partner_api';
```

## See Also {#see-also}

- [serializedQuantiles](../../../sql-reference/aggregate-functions/reference/serializedquantiles) — Create Quantiles sketch
- [percentileFromQuantiles](../../../sql-reference/functions/percentilefromquantiles) — Extract percentile from sketch
- [mergeSerializedHLL](../../../sql-reference/aggregate-functions/reference/mergeserializedhll) — Similar pattern for cardinality sketches
