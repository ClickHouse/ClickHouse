---
description: 'Merges multiple serialized TDigest sketches for distributed percentile computation'
slug: /sql-reference/aggregate-functions/reference/mergeSerializedTDigest
title: 'mergeSerializedTDigest'
doc_type: 'reference'
---

# mergeSerializedTDigest

Merges multiple Apache DataSketches TDigest sketches into a single sketch. This enables distributed percentile computation with high accuracy at extreme percentiles.

## Syntax {#syntax}

```sql
mergeSerializedTDigest([base64_encoded])(sketch)
```

## Arguments {#arguments}

- `base64_encoded` (optional) — Boolean flag (0 or 1) to control base64 decoding. Default: 0 (raw binary).
  - `0` (default): Input is raw binary sketch data (most common)
  - `1`: Input is base64-encoded and will be decoded before merging
- `sketch` — Column containing serialized TDigest sketches. Type: [String](../../../sql-reference/data-types/string).

## Returned Value {#returned-value}

- Merged serialized TDigest sketch. Type: [String](../../../sql-reference/data-types/string).

## Implementation Details {#implementation-details}

The merge operation is:
- **Commutative**: Order doesn't matter
- **Associative**: Can merge in any grouping
- Preserves high accuracy at extreme percentiles

## Examples {#examples}

### Example 1: Merge Hourly into Daily {#example-1-merge-hourly-into-daily}

```sql
WITH hourly_sketches AS (
    SELECT 
        toStartOfHour(timestamp) AS hour,
        serializedTDigest(latency_ms) AS sketch
    FROM requests
    WHERE date = today()
    GROUP BY hour
)
SELECT 
    percentileFromTDigest(mergeSerializedTDigest(sketch), 0.99) AS daily_p99,
    percentileFromTDigest(mergeSerializedTDigest(sketch), 0.999) AS daily_p999
FROM hourly_sketches;
```

### Example 2: Cross-Region Aggregation {#example-2-cross-region-aggregation}

```sql
SELECT 
    service,
    percentileFromTDigest(mergeSerializedTDigest(sketch), 0.50) AS global_p50,
    percentileFromTDigest(mergeSerializedTDigest(sketch), 0.99) AS global_p99,
    percentileFromTDigest(mergeSerializedTDigest(sketch), 0.999) AS global_p999
FROM distributed_tdigest_table
GROUP BY service;
```

### Example 3: Base64-Encoded Input {#example-3-base64-encoded-input}

```sql
-- Merge sketches from external system stored as base64
SELECT 
    percentileFromTDigest(
        mergeSerializedTDigest(1)(base64_sketch), 
        0.999
    ) AS p999
FROM external_tdigest_data;
```

## See Also {#see-also}

- [serializedTDigest](../../../sql-reference/aggregate-functions/reference/serializedtdigest) — Create TDigest sketch
- [percentileFromTDigest](../../../sql-reference/functions/percentilefromtdigest) — Extract percentile from TDigest
- [mergeSerializedQuantiles](../../../sql-reference/aggregate-functions/reference/mergeserializedquantiles) — Similar pattern for Quantiles sketches
