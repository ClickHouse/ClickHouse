---
description: 'Extracts the cardinality estimate from a serialized HLL sketch'
slug: /sql-reference/functions/cardinalityFromHLL
title: 'cardinalityFromHLL'
doc_type: 'reference'
---

# cardinalityFromHLL

Extracts the approximate cardinality (number of distinct elements) from a serialized HyperLogLog (HLL) sketch created by [serializedHLL](../../sql-reference/aggregate-functions/reference/serializedhll) or [mergeSerializedHLL](../../sql-reference/aggregate-functions/reference/mergeserializedhll).

## Syntax

```sql
cardinalityFromHLL(sketch)
```

## Arguments

- `sketch` — Serialized HLL sketch. Type: [String](../../sql-reference/data-types/string).

## Returned Value

- Approximate number of distinct elements. Type: [UInt64](../../sql-reference/data-types/int-uint).
- Returns 0 if the sketch is empty or invalid.

## Implementation Details

- Deserializes the HLL sketch and computes the cardinality estimate using the HyperLogLog algorithm
- The estimate has mathematically provable error bounds based on the `lg_k` parameter used when creating the sketch
- Very fast: typically microseconds regardless of the actual cardinality
- Handles both ClickHouse-generated sketches and external Apache DataSketches HLL sketches

## Usage

### Extract Cardinality from Sketch

```sql
SELECT cardinalityFromHLL(user_sketch) AS unique_users
FROM daily_metrics
WHERE date = today();
```

### Combine with Aggregation

```sql
SELECT 
    service,
    cardinalityFromHLL(mergeSerializedHLL(user_sketch)) AS total_unique_users
FROM hourly_metrics
WHERE hour >= now() - INTERVAL 24 HOUR
GROUP BY service;
```

## Examples

### Example 1: Basic Cardinality Estimation

```sql
SELECT cardinalityFromHLL(serializedHLL(number)) AS estimated_count
FROM numbers(1000);
```

```response
┌─estimated_count─┐
│            1031 │
└─────────────────┘
```

### Example 2: Daily Active Users

```sql
WITH daily_sketches AS (
    SELECT 
        date,
        serializedHLL(user_id) AS user_sketch
    FROM events
    WHERE date >= today() - 30
    GROUP BY date
)
SELECT 
    date,
    cardinalityFromHLL(user_sketch) AS daily_active_users
FROM daily_sketches
ORDER BY date;
```

### Example 3: Multi-Dimensional Analysis

```sql
SELECT 
    region,
    platform,
    cardinalityFromHLL(serializedHLL(12)(user_id)) AS unique_users
FROM events
WHERE date = today()
GROUP BY region, platform
ORDER BY unique_users DESC;
```

### Example 4: Union of Sets

```sql
WITH 
    mobile_users AS (SELECT serializedHLL(user_id) AS s FROM events WHERE platform = 'mobile'),
    web_users AS (SELECT serializedHLL(user_id) AS s FROM events WHERE platform = 'web'),
    all_users AS (SELECT mergeSerializedHLL(s) AS m FROM (SELECT s FROM mobile_users UNION ALL SELECT s FROM web_users))
SELECT cardinalityFromHLL(m) AS total_unique_users FROM all_users;
```

### Example 5: Accuracy Verification

```sql
WITH data AS (
    SELECT number FROM numbers(10000)
)
SELECT 
    count(DISTINCT number) AS exact_count,
    cardinalityFromHLL(serializedHLL(number)) AS hll_estimate,
    abs(hll_estimate - exact_count) / exact_count * 100 AS error_percent
FROM data;
```

```response
┌─exact_count─┬─hll_estimate─┬─error_percent─┐
│       10000 │        10486 │          4.86 │
└─────────────┴──────────────┴───────────────┘
```

## Performance Notes

- Execution time is constant regardless of cardinality (typically < 1ms)
- No memory overhead beyond the sketch itself
- Can be used in WHERE clauses and ORDER BY without performance penalty

## Error Handling

- Invalid or corrupted sketches return 0
- Empty sketches return 0
- NULL inputs return 0

## See Also

- [serializedHLL](../../sql-reference/aggregate-functions/reference/serializedhll) — Create HLL sketches
- [mergeSerializedHLL](../../sql-reference/aggregate-functions/reference/mergeserializedhll) — Merge HLL sketches
- [uniq](../../sql-reference/aggregate-functions/reference/uniq) — Direct distinct count estimation
