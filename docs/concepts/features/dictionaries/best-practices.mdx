---
slug: /dictionary/best-practices
title: 'Dictionary best practices'
sidebarTitle: 'Best practices'
description: 'Guidelines for choosing dictionary layouts, when to use dictionaries vs JOINs, and monitoring dictionary usage.'
doc_type: 'guide'
keywords: ['dictionary', 'dictionaries', 'layout', 'dictGet', 'JOIN', 'hashed', 'flat', 'performance']
---

This page covers practical guidance for choosing the right dictionary layout, understanding when dictionaries outperform JOINs (and when they don't), and monitoring dictionary usage.

For an introduction to dictionaries with worked examples, see the [main Dictionary guide](/concepts/features/dictionaries/index).

## When to use dictionaries vs JOINs {#when-to-use-dictionaries-vs-joins}

Dictionaries work best when one side of a JOIN is a lookup table that fits in memory. With a standard JOIN, ClickHouse builds a hash table from the right-hand side before probing it with the left — even if most rows are later discarded by `WHERE` filters. Although recent versions (24.12+) push filters before JOINs in many cases, this doesn't always eliminate the overhead. With a dictionary, you call `dictGet` inline, so lookups only happen on rows that already survived filtering.

However, `dictGet` isn't always the right choice. If you need to call `dictGet` on a large percentage of the rows in a table — for example, in a `WHERE` condition like `dictGet('dict', 'elevation', id) > 1800` — you may be better off using a regular column with native indexes. ClickHouse can use `PREWHERE` to skip granules for a regular column, but `dictGet` evaluates row by row with no index support.

As a rule of thumb:
- Use dictionaries to replace JOINs against small dimension tables where the lookup key is already available.
- Use regular columns and indexes when filtering against the looked-up value across many rows.

## Choosing a layout {#choosing-a-layout}

The `LAYOUT` clause controls the internal data structure for the dictionary. All available layouts are documented in the [layouts reference](/reference/statements/create/dictionary/layouts/overview#storing-dictionaries-in-memory).

When choosing a layout, use the following guidelines:

- **`flat`** — the fastest layout (simple array offset lookup), but keys must be `UInt64` and are limited to 500,000 by default (`max_array_size`). Best for monotonically increasing integer keys in small-to-medium tables. Sparse key distributions (e.g. key values of 1 and 500,000) waste memory since the array is sized to the largest key. If you're hitting the 500k limit, that's a signal to switch to `hashed_array`.
- **`hashed_array`** — the recommended default for most use cases. Stores attributes in arrays with a hash table mapping keys to array indices. Nearly as fast as `hashed` but more memory-efficient, especially with many attributes.
- **`hashed`** — stores the full dictionary in a hash table. Can be faster than `hashed_array` when you have very few attributes, but consumes more memory as attribute count grows.
- **`complex_key_hashed` / `complex_key_hashed_array`** — use these when keys aren't castable to `UInt64` (for example, `String` keys). They follow the same performance tradeoffs as their non-complex counterparts.
- **`sparse_hashed`** — trades CPU for lower memory usage compared to `hashed`. Rarely the best choice — it's only efficient when you have a single attribute. In most cases, `hashed_array` is a better fit.
- **`cache` / `ssd_cache`** — only cache frequently accessed keys. Useful when the full dataset doesn't fit in memory, but lookups may hit the source on cache misses. Not recommended for latency-sensitive workloads.
- **`direct`** — queries the source for every lookup with no in-memory storage. Use when the data changes too frequently to cache or when the dictionary is too large for memory.

## Monitoring dictionary usage {#monitoring-dictionary-usage}

Track memory consumption and health via the [`system.dictionaries`](/reference/system-tables/dictionaries) table:

```sql
SELECT
    name,
    status,
    element_count,
    formatReadableSize(bytes_allocated) AS size,
    query_count,
    hit_rate,
    found_rate,
    last_exception
FROM system.dictionaries
```

Key columns:
- `bytes_allocated` — memory consumed by the dictionary. Dictionaries store data uncompressed, so this can be significantly larger than the compressed table size.
- `hit_rate` and `found_rate` — useful for evaluating `cache` layout effectiveness.
- `last_exception` — check this when a dictionary fails to load or refresh.
