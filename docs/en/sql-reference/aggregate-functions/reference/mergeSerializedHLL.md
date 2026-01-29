---
description: 'Merges multiple serialized HLL sketches into a single sketch for distributed cardinality estimation'
slug: /sql-reference/aggregate-functions/reference/mergeSerializedHLL
title: 'mergeSerializedHLL'
doc_type: 'reference'
---

# mergeSerializedHLL

Merges multiple serialized Apache DataSketches HyperLogLog (HLL) sketches that were created by [serializedHLL](../../../sql-reference/aggregate-functions/reference/serializedhll) into a single unified sketch. This enables distributed cardinality estimation where sketches are computed on different nodes or time periods and then merged together.

## Syntax {#syntax}

```sql
mergeSerializedHLL([base64_encoded, lg_k, type])(sketch_column)
```

## Arguments {#arguments}

- `sketch_column` — Column containing serialized HLL sketches. Type: [String](../../../sql-reference/data-types/string).

## Parameters (optional) {#parameters}

- The function accepts **multiple equivalent parameter orderings** for convenience:
  - `mergeSerializedHLL()` — defaults: `base64_encoded=0`, `lg_k=10`, `type='HLL_4'`
  - `mergeSerializedHLL(base64_encoded)` — `base64_encoded` is `0` or `1`
  - `mergeSerializedHLL(lg_k)` — `lg_k` is in `[4..21]`
  - `mergeSerializedHLL(base64_encoded, lg_k)`
  - `mergeSerializedHLL(lg_k, type)`
  - `mergeSerializedHLL(base64_encoded, lg_k, type)`
  - `mergeSerializedHLL(lg_k, type, base64_encoded)`

- When parameter types could be ambiguous, ClickHouse applies the following disambiguation rules:
  - If a parameter position is a **String**, it is interpreted as `type`
  - Otherwise, numeric parameters are interpreted as:
    - `base64_encoded` when the value is `0` or `1`
    - `lg_k` when the value is in `[4..21]`
  - If the 3-parameter form is still ambiguous (e.g. both the 2nd and 3rd parameters are numeric), the function throws an error.

- `base64_encoded` — Boolean flag to control base64 decoding. Type: [Bool](../../../sql-reference/data-types/bool). Default: 0 (false).
  - `0` (false, default): Input is raw binary data, no decoding needed (recommended for ClickHouse-generated sketches)
  - `1` (true): Input is base64-encoded and will be decoded before merging (for external data sources)

- `lg_k` — Log-base-2 of buckets (should match the value used in [serializedHLL](../../../sql-reference/aggregate-functions/reference/serializedhll)). Type: [UInt8](../../../sql-reference/data-types/int-uint). Valid range: 4-21. Default: 10.

- `type` — Storage format (should match the value used in [serializedHLL](../../../sql-reference/aggregate-functions/reference/serializedhll)). Type: [String](../../../sql-reference/data-types/string). Valid values: 'HLL_4', 'HLL_6', 'HLL_8'. Default: 'HLL_4'.

## Returned Value {#returned-value}

- Merged serialized HLL sketch. Type: [String](../../../sql-reference/data-types/string).

## Implementation Details {#implementation-details}

- Merging is commutative and associative (order doesn't matter)
- The merged sketch has the same accuracy as individual sketches
- When merging sketches with different `lg_k` values, the result uses the smaller `lg_k`
- Very efficient: logarithmic time complexity in sketch size

## Usage {#usage}

### Basic Merge (Default Parameters) {#basic-merge}

```sql
-- Merge sketches from different partitions
SELECT cardinalityFromHLL(mergeSerializedHLL(sketch)) AS total_users
FROM daily_user_sketches;
```

### Explicit Performance Flag {#explicit-performance-flag}

```sql
-- Explicitly set base64_encoded=0 (same as default, raw binary)
SELECT mergeSerializedHLL(0)(sketch) AS merged_sketch
FROM sketches;
```

### Merge with Custom Parameters {#merge-with-custom-parameters}

```sql
-- Merge high-precision sketches (lg_k=14)
SELECT mergeSerializedHLL(0, 14, 'HLL_4')(sketch) AS merged_sketch
FROM high_precision_sketches;
```

### External Data with Base64 {#external-data-with-base64}

```sql
-- For external data that is base64-encoded
SELECT mergeSerializedHLL(1)(sketch) AS merged_sketch
FROM imported_sketches;
```

## Examples {#examples}

### Example 1: Daily to Monthly Aggregation {#example-1-daily-to-monthly-aggregation}

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

### Example 2: Multi-Region Aggregation {#example-2-multi-region-aggregation}

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

### Example 3: Time-Series Rollup {#example-3-time-series-rollup}

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

### Example 4: Union of Disjoint Sets {#example-4-union-of-disjoint-sets}

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

## Performance Notes {#performance-notes}

- Using `base64_encoded=0` (default) avoids base64 decoding overhead for ClickHouse-generated sketches
- For best performance, match `lg_k` and `type` parameters to those used in `serializedHLL`
- Merging is very fast: typically microseconds per sketch regardless of cardinality

## See Also {#see-also}

- [serializedHLL](../../../sql-reference/aggregate-functions/reference/serializedhll) — Create HLL sketches
- [cardinalityFromHLL](../../../sql-reference/functions/cardinalityfromhll) — Extract cardinality from merged sketch
- [uniq](../../../sql-reference/aggregate-functions/reference/uniq) — Native approximate distinct count
