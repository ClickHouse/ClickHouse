---
description: 'Creates a serialized HyperLogLog (HLL) sketch for approximate cardinality estimation with configurable accuracy'
slug: /sql-reference/aggregate-functions/reference/serializedHLL
title: 'serializedHLL'
doc_type: 'reference'
---

# serializedHLL

Creates a serialized Apache DataSketches HyperLogLog (HLL) sketch from column values for approximate distinct count estimation. The sketch can be stored, transmitted, or merged with other sketches for distributed cardinality estimation.

## Syntax {#syntax}

```sql
serializedHLL([lg_k, type])(expression)
```

## Arguments {#arguments}

- `expression` — Column expression. Supported types: [Int](../../data-types/int-uint.md), [UInt](../../data-types/int-uint.md), [Float](../../data-types/float.md), [String](../../data-types/string.md), [FixedString](../../data-types/fixedstring.md).

## Parameters (optional) {#parameters}

- `lg_k` — Log-base-2 of the number of buckets (precision parameter). Type: [UInt8](../../data-types/int-uint.md). Valid range: 4-21. Default: 10.
  - Higher values provide better accuracy but use more memory
  - `lg_k=10` (1,024 buckets): ~3.2% error, ~512 bytes (HLL_4)
  - `lg_k=12` (4,096 buckets): ~1.6% error, ~2 KB (HLL_4)
  - `lg_k=14` (16,384 buckets): ~0.8% error, ~8 KB (HLL_4)

- `type` — Storage format for the HLL sketch. Type: [String](../../data-types/string.md). Valid values: 'HLL_4', 'HLL_6', 'HLL_8'. Default: 'HLL_4'.
  - `'HLL_4'`: 4 bits per bucket, most compact (~K/2 bytes), slowest updates
  - `'HLL_6'`: 6 bits per bucket, balanced (~3K/4 bytes), medium speed
  - `'HLL_8'`: 8 bits per bucket, largest (~K bytes), fastest updates
  - All types produce identical accuracy for the same `lg_k`

## Returned Value {#returned-value}

- Serialized binary HLL sketch. Type: [String](../../data-types/string.md).

## Implementation Details {#implementation-details}

This function uses the Apache DataSketches HyperLogLog algorithm, providing:
- Fixed memory usage regardless of cardinality
- Mergeable sketches for distributed computation
- Binary compatibility with other DataSketches implementations (Java, Python)
- Mathematically provable error bounds

The relative error is approximately `1.04 / √K` where `K = 2^lg_k`.

## Usage {#usage}

### Basic Usage (Default Parameters) {#basic-usage}

```sql
-- Create HLL sketch with default parameters (lg_k=10, type='HLL_4')
SELECT serializedHLL(user_id) AS user_sketch
FROM events;
```

### Custom Precision {#custom-precision}

```sql
-- Higher precision for critical metrics (lg_k=14)
SELECT serializedHLL(14)(customer_id) AS customer_sketch
FROM transactions
GROUP BY date;
```

### Fast Updates {#fast-updates}

```sql
-- Use HLL_8 for fastest update performance
SELECT serializedHLL(10, 'HLL_8')(session_id) AS session_sketch
FROM realtime_events
GROUP BY minute;
```

### Materialized View Pattern {#materialized-view-pattern}

```sql
-- Store sketches for incremental aggregation
CREATE MATERIALIZED VIEW daily_user_sketches
ENGINE = AggregatingMergeTree()
ORDER BY date
AS SELECT
    date,
    serializedHLL(12)(user_id) AS user_sketch
FROM events
GROUP BY date;
```

## Examples {#examples}

### Example 1: Basic Cardinality Estimation {#example-1-basic-cardinality-estimation}

```sql
SELECT cardinalityFromHLL(serializedHLL(number)) AS estimated_count
FROM numbers(1000);
```

```response
┌─estimated_count─┐
│            1031 │
└─────────────────┘
```

### Example 2: Accuracy Comparison {#example-2-accuracy-comparison}

```sql
WITH 
    lg8 AS (SELECT cardinalityFromHLL(serializedHLL(8)(number)) AS c FROM numbers(10000)),
    lg12 AS (SELECT cardinalityFromHLL(serializedHLL(12)(number)) AS c FROM numbers(10000))
SELECT 
    (SELECT c FROM lg8) AS lg_k_8_estimate,
    (SELECT c FROM lg12) AS lg_k_12_estimate,
    abs((SELECT c FROM lg12) - 10000) < abs((SELECT c FROM lg8) - 10000) AS lg12_more_accurate;
```

```response
┌─lg_k_8_estimate─┬─lg_k_12_estimate─┬─lg12_more_accurate─┐
│           10234 │            10089 │                  1 │
└─────────────────┴──────────────────┴────────────────────┘
```

### Example 3: Different Storage Types {#example-3-different-storage-types}

```sql
SELECT 
    length(serializedHLL(10, 'HLL_4')(number)) AS hll4_size,
    length(serializedHLL(10, 'HLL_6')(number)) AS hll6_size,
    length(serializedHLL(10, 'HLL_8')(number)) AS hll8_size
FROM numbers(10000);
```

```response
┌─hll4_size─┬─hll6_size─┬─hll8_size─┐
│       552 │       824 │      1096 │
└───────────┴───────────┴───────────┘
```

## See Also {#see-also}

- [mergeSerializedHLL](./mergeSerializedHLL.md) — Merge multiple HLL sketches
- [cardinalityFromHLL](../../functions/cardinalityFromHLL.md) — Extract cardinality estimate from sketch
- [uniq](./uniq.md) — Native ClickHouse approximate distinct count
- [uniqHLL12](./uniqHLL12.md) — Alternative HLL implementation
