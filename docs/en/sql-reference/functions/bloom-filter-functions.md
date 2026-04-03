---
description: 'Documentation for Bloom Filter Functions'
sidebar_label: 'Bloom filter'
slug: /sql-reference/functions/bloom-filter-functions
title: 'Bloom Filter Functions'
doc_type: 'reference'
---

# Bloom Filter Functions

Functions for working with Bloom filter states produced by the [`groupBloomFilter`](/sql-reference/aggregate-functions/reference/groupbloomfilter) aggregate function.

## bloomFilterContains

Checks whether a value is probably present in a Bloom filter built by [`groupBloomFilter`](/sql-reference/aggregate-functions/reference/groupbloomfilter).

Returns `1` if the value is probably in the filter (may have false positives), or `0` if the value is definitely not in the filter (no false negatives).

The false positive rate is controlled by the `false_positive_rate` parameter of `groupBloomFilter`.

**Syntax**

```sql
bloomFilterContains(bloom_filter, value)
```

**Arguments**

| Argument | Description | Type |
|----------|-------------|------|
| `bloom_filter` | Bloom filter state produced by `groupBloomFilterState`. | [`AggregateFunction(groupBloomFilter, T)`](/sql-reference/data-types/aggregatefunction) |
| `value` | Value to check for membership. Must be the same type `T` as was used to build the filter. | `T` |

**Returned value**

- `1` — the value is probably present in the filter.
- `0` — the value is definitely absent from the filter.

Type: [UInt8](/sql-reference/data-types/int-uint).

**Examples**

**Basic usage**

Check whether a number is present in a Bloom filter built from `numbers(100)`:

```sql
SELECT bloomFilterContains(groupBloomFilterState(1000)(number), toUInt64(42)) AS result
FROM numbers(100)
```

```text
┌─result─┐
│      1 │
└────────┘
```

**Check a value absent from the filter**

A value outside the range used to build the filter returns `0` (definitely absent):

```sql
SELECT bloomFilterContains(groupBloomFilterState(1000)(number), toUInt64(200)) AS result
FROM numbers(100)
```

```text
┌─result─┐
│      0 │
└────────┘
```

**Find new values using WITH clause**

Count values in `100..199` that are absent from a filter built on `0..99`:

```sql
WITH (
    SELECT groupBloomFilterState(1000)(number)
    FROM numbers(100)
) AS old_bloom
SELECT count() AS new_values_count
FROM numbers(200)
WHERE number >= 100
  AND NOT bloomFilterContains(old_bloom, number)
```

```text
┌─new_values_count─┐
│              100 │
└──────────────────┘
```

**See Also**

- [groupBloomFilter](/sql-reference/aggregate-functions/reference/groupbloomfilter) — aggregate function that builds a Bloom filter state
- [AggregateFunction data type](/sql-reference/data-types/aggregatefunction)
