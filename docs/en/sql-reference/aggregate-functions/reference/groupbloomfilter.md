---
description: 'Builds a probabilistic Bloom filter from column values. Use with the -State combinator and bloomFilterContains to efficiently check set membership.'
slug: /sql-reference/aggregate-functions/reference/groupbloomfilter
title: 'groupBloomFilter'
doc_type: 'reference'
---

# groupBloomFilter

Builds a probabilistic [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) from column values and returns it as an aggregate state.

The Bloom filter can be used with [`bloomFilterContains`](/sql-reference/functions/bloom-filter-functions#bloomFilterContains) to efficiently check whether a value was present in the aggregated dataset.

This is useful for finding new values that appeared in one time interval but were absent in another, with low memory usage compared to exact methods like `NOT IN` or `EXCEPT`.

:::note
A Bloom filter is a probabilistic data structure. It may return **false positives** (report a value as present when it is not), but never **false negatives** (a value that was added will always be found). The false positive rate is controlled by the `false_positive_rate` parameter.
:::

**Syntax**

```sql
groupBloomFilter([expected_elements[, false_positive_rate[, seed]]])(column)
groupBloomFilterState([expected_elements[, false_positive_rate[, seed]]])(column)
```

The `-State` combinator is required to obtain the Bloom filter state for use with `bloomFilterContains`. Without it, the function returns `0` as a placeholder.

**Parameters**

| Parameter | Description | Default |
|-----------|-------------|---------|
| `expected_elements` | Expected number of distinct elements to be inserted into the filter. | `10000` |
| `false_positive_rate` | Desired false positive probability, a float in `(0, 1)`. Lower values require more memory. | `0.025` |
| `seed` | Seed for the hash functions. | `0` |

Alternatively, you can specify filter parameters directly:

| Parameter | Description |
|-----------|-------------|
| `filter_size_bytes` | Size of the Bloom filter in bytes. |
| `num_hashes` | Number of hash functions. |

The maximum allowed filter size is 256 MB.

**Arguments**

- `column` — Column values to add to the Bloom filter. Supported types: `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`, `Float32`, `Float64`, `String`, `FixedString`.

**Returned value**

- Without `-State` combinator: returns `0` (placeholder value). [UInt64](/sql-reference/data-types/int-uint).
- With `-State` combinator: returns the Bloom filter state as [`AggregateFunction(groupBloomFilter, T)`](/sql-reference/data-types/aggregatefunction).

**Implementation details**

The filter size and number of hash functions are computed automatically from `expected_elements` and `false_positive_rate` using the standard formulas:

- Optimal size in bits: `m = -n × ln(p) / (ln 2)²`
- Optimal number of hashes: `k = (m / n) × ln 2`

where `n` is the expected number of elements and `p` is the false positive rate.

When merging states (e.g. in distributed queries), both filters must have identical parameters (`filter_size_bytes`, `num_hashes`, `seed`). Merging is performed by bitwise OR of the filter arrays.

**Examples**

**Basic usage**

Build a Bloom filter from a set of numbers and check membership:

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

- [bloomFilterContains](/sql-reference/functions/bloom-filter-functions#bloomFilterContains) — checks whether a value is present in a Bloom filter state
- [AggregateFunction data type](/sql-reference/data-types/aggregatefunction)
- [Aggregate function combinators](/sql-reference/aggregate-functions/combinators)
