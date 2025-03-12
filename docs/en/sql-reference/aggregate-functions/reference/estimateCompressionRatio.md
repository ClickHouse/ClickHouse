---
slug: /sql-reference/aggregate-functions/reference/estimateCompressionRatio
sidebar_position: 132
title: 'estimateCompressionRatio'
description: 'Estimates the compression ratio of a given column without compressing it.'
---

## estimateCompressionRatio {#estimatecompressionration}

Estimates the compression ratio of a given column without compressing it.

**Syntax**

```sql
estimateCompressionRatio(codec, block_size_bytes)(column)
```

**Arguments**

- `column` - Column of any type

**Parameters**

- `codec` - [String](../../../sql-reference/data-types/string.md) containing a [compression codec](/sql-reference/statements/create/table#column_compression_codec).
- `block_size_bytes` - Block size of compressed data. This is similar to setting both [`max_compress_block_size`](../../../operations/settings/merge-tree-settings.md#max_compress_block_size) and [`min_compress_block_size`](../../../operations/settings/merge-tree-settings.md#min_compress_block_size). The default value is 1 MiB (1048576 bytes).

Both parameters are optional.

**Returned values**

- Returns an estimate compression ratio for the given column.

Type: [Float64](/sql-reference/data-types/float).

**Examples**

Input table:

``` sql
CREATE TABLE compression_estimate_example
(
    `number` UInt64
)
ENGINE = MergeTree()
ORDER BY number
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO compression_estimate_example
SELECT number FROM system.numbers LIMIT 100_000;
```

Query:

```sql
SELECT estimateCompressionRatio(number) as estimate from compression_estimate_example;
```

Result:

``` text
┌───────────estimate─┐
│ 1.9988506608699999 │
└────────────────────┘
```

:::note
The result above will differ based on the default compression codec of the server. See [Column Compression Codecs](/sql-reference/statements/create/table#column_compression_codec).
:::

Query:

```sql
SELECT estimateCompressionRatio('T64')(number) as estimate from compression_estimate_example;
```

Result:

``` text
┌──────────estimate─┐
│ 3.762758101688538 │
└───────────────────┘
```
