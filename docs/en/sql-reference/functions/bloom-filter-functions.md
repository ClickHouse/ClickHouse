---
description: 'Documentation for Bloom Filter Functions'
sidebar_label: 'Bloom filter'
sidebar_position: 70
slug: /sql-reference/functions/bloom-filter-functions
title: 'Bloom Filter Functions'
doc_type: 'reference'
---

# Bloom Filter Functions {#bloom-filter-functions}

Functions for working with Bloom filter states produced by the [`groupBloomFilter`](/sql-reference/aggregate-functions/reference/groupbloomfilter) aggregate function.

## bloomFilterContains {#bloomfiltercontains}

Checks whether a value is probably present in a Bloom filter built by [`groupBloomFilter`](/sql-reference/aggregate-functions/reference/groupbloomfilter).

Returns `1` if the value is probably in the filter (may have false positives), or `0` if the value is definitely not in the filter (no false negatives).

The false positive rate is controlled by the `false_positive_rate` parameter of `groupBloomFilter`.

### Syntax {#syntax}

```sql
bloomFilterContains(bloom_filter, value)
```

### Arguments {#arguments}

| Argument | Description | Type |
|----------|-------------|------|
| `bloom_filter` | Bloom filter state produced by `groupBloomFilterState`. [`AggregateFunction(groupBloomFilter, T)`](/sql-reference/data-types/aggregatefunction) for the default form, or `AggregateFunction(groupBloomFilter(params...), T)` for a parameterized form (e.g. `AggregateFunction(groupBloomFilter(1000), String)`). The parameters must resolve to the same effective Bloom filter configuration as the state. | [`AggregateFunction(groupBloomFilter[(parameters...)], T)`](/sql-reference/data-types/aggregatefunction) |
| `value` | Value to check for membership. For numeric filters, it may be any compatible numeric type and is converted to the filter value type `T` with an accurate cast. Values that cannot be represented in `T` are treated as definitely absent and return `0`. Incompatible types cause an exception. | `T` or a compatible numeric type |

### Returned value {#returned-value}

- `1` — the value is probably present in the filter.
- `0` — the value is definitely absent from the filter, including numeric probe values that cannot be represented in the filter value type.
- `NULL` — the probe value is `NULL` for a `Nullable` argument.

Type: [UInt8](/sql-reference/data-types/int-uint). For a `Nullable` probe argument, the result is wrapped as `Nullable(UInt8)` by the default nullable handling.

### Examples {#examples}

#### Basic usage {#basic-usage}

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

#### Check a value absent from the filter {#check-a-value-absent-from-the-filter}

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

#### Check a non-representable numeric value {#check-a-non-representable-numeric-value}

A numeric probe value that cannot be represented in the filter value type is treated as definitely absent. For example, `300` cannot be represented in a `UInt8` filter:

```sql
SELECT bloomFilterContains(groupBloomFilterState(1000)(toUInt8(number)), toUInt16(300)) AS result
FROM numbers(100)
```

```text
┌─result─┐
│      0 │
└────────┘
```

#### Find new values using WITH clause {#find-new-values-using-with-clause}

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

## See Also {#see-also}

- [groupBloomFilter](/sql-reference/aggregate-functions/reference/groupbloomfilter) — aggregate function that builds a Bloom filter state
- [AggregateFunction data type](/sql-reference/data-types/aggregatefunction)
