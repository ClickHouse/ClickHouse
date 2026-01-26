---
description: 'Merges multiple serialized HLL sketches into a single sketch for distributed cardinality estimation'
slug: /sql-reference/aggregate-functions/reference/mergeSerializedHLL
title: 'mergeSerializedHLL'
doc_type: 'reference'
---

# mergeSerializedHLL

Merges multiple serialized Apache DataSketches HyperLogLog (HLL) sketches that were created by [serializedHLL](../../../sql-reference/aggregate-functions/reference/serializedhll) into a single unified sketch. This enables distributed cardinality estimation where sketches are computed on different nodes or time periods and then merged together.

## Syntax

```sql
mergeSerializedHLL([base64_encoded, lg_k, type])(sketch_column)
```

## Arguments

- `sketch_column` — Column containing serialized HLL sketches. Type: [String](../../../sql-reference/data-types/string).

## Parameters (optional)

- `base64_encoded` — Boolean flag to control base64 decoding. Type: [Bool](../../../sql-reference/data-types/bool). Default: 0 (false).
  - `0` (false, default): Input is raw binary data, no decoding needed (recommended for ClickHouse-generated sketches, ~95% faster)
  - `1` (true): Input is base64-encoded and will be decoded before merging (for external data sources)

- `lg_k` — Log-base-2 of buckets (should match the value used in [serializedHLL](../../../sql-reference/aggregate-functions/reference/serializedhll)). Type: [UInt8](../../../sql-reference/data-types/int-uint). Valid range: 4-21. Default: 10.

- `type` — Storage format (should match the value used in [serializedHLL](../../../sql-reference/aggregate-functions/reference/serializedhll)). Type: [String](../../../sql-reference/data-types/string). Valid values: 'HLL_4', 'HLL_6', 'HLL_8'. Default: 'HLL_4'.

## Returned Value

- Merged serialized HLL sketch. Type: [String](../../../sql-reference/data-types/string).

## Implementation Details

- Merging is commutative and associative (order doesn't matter)
- The merged sketch has the same accuracy as individual sketches
- When merging sketches with different `lg_k` values, the result uses the smaller `lg_k`
- Very efficient: logarithmic time complexity in sketch size

## Usage

### Basic Merge (Default Parameters)

```sql
-- Merge sketches from different partitions
SELECT cardinalityFromHLL(mergeSerializedHLL(sketch)) AS total_users
FROM daily_user_sketches;
```

### Explicit Performance Flag

```sql
-- Explicitly set base64_encoded=0 (same as default, raw binary)
SELECT mergeSerializedHLL(0)(sketch) AS merged_sketch
FROM sketches;
```

### Merge with Custom Parameters

```sql
-- Merge high-precision sketches (lg_k=14)
SELECT mergeSerializedHLL(0, 14, 'HLL_4')(sketch) AS merged_sketch
FROM high_precision_sketches;
```

### External Data with Base64

```sql
-- For external data that is base64-encoded
SELECT mergeSerializedHLL(1)(sketch) AS merged_sketch
FROM imported_sketches;
```

## Examples

### Example 1: Daily to Monthly Aggregation

```sql
-- Create daily sketches
CREATE TABLE daily_metrics (
    date Date,
    user_sketch String
) ENGINE = MergeTree()
ORDER BY date;

INSERT INTO daily_metrics
SELECT 
    toDate('2024-01-01') + number AS date,
    serializedHLL(rand() % 100000) AS user_sketch
FROM numbers(30)
GROUP BY date;

-- Get monthly cardinality
SELECT 
    toStartOfMonth(date) AS month,
    cardinalityFromHLL(mergeSerializedHLL(user_sketch)) AS monthly_unique_users
FROM daily_metrics
GROUP BY month;
```

### Example 2: Multi-Region Aggregation

```sql
WITH regional_sketches AS (
    SELECT 
        region,
        serializedHLL(12)(user_id) AS user_sketch
    FROM events
    GROUP BY region
)
SELECT cardinalityFromHLL(mergeSerializedHLL(0, 12, 'HLL_4')(user_sketch)) AS global_unique_users
FROM regional_sketches;
```

### Example 3: Time-Series Rollup

```sql
-- Minute-level sketches
CREATE MATERIALIZED VIEW minute_metrics AS
SELECT 
    toStartOfMinute(timestamp) AS minute,
    serializedHLL(10, 'HLL_8')(session_id) AS session_sketch
FROM events
GROUP BY minute;

-- Hourly rollup
CREATE MATERIALIZED VIEW hourly_metrics AS
SELECT 
    toStartOfHour(minute) AS hour,
    mergeSerializedHLL(0, 10, 'HLL_8')(session_sketch) AS session_sketch
FROM minute_metrics
GROUP BY hour;

-- Query hourly unique sessions
SELECT 
    hour,
    cardinalityFromHLL(session_sketch) AS unique_sessions
FROM hourly_metrics
WHERE hour >= now() - INTERVAL 24 HOUR;
```

### Example 4: Union of Disjoint Sets

```sql
WITH 
    set1 AS (SELECT serializedHLL(number) AS s FROM numbers(1000)),
    set2 AS (SELECT serializedHLL(number + 1000) AS s FROM numbers(1000)),
    merged AS (SELECT mergeSerializedHLL(s) AS m FROM (SELECT s FROM set1 UNION ALL SELECT s FROM set2))
SELECT cardinalityFromHLL(m) AS total_cardinality FROM merged;
```

```response
┌─total_cardinality─┐
│              2065 │
└───────────────────┘
```

## Performance Notes

- Using `base64_encoded=0` (default) provides ~95% speedup for ClickHouse-generated sketches (no base64 decoding overhead)
- For best performance, match `lg_k` and `type` parameters to those used in `serializedHLL`
- Merging is very fast: typically microseconds per sketch regardless of cardinality

## See Also

- [serializedHLL](../../../sql-reference/aggregate-functions/reference/serializedhll) — Create HLL sketches
- [cardinalityFromHLL](../../../sql-reference/functions/cardinalityfromhll) — Extract cardinality from merged sketch
- [uniq](../../../sql-reference/aggregate-functions/reference/uniq) — Native approximate distinct count
