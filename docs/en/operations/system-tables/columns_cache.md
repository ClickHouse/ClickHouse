---
description: 'Contains information about entries in the in-memory cache of deserialized columns (`columns cache`).'
keywords: ['system table', 'columns_cache']
slug: /operations/system-tables/columns_cache
title: 'system.columns_cache'
doc_type: 'reference'
---

## Description {#description}

Contains one row per entry currently stored in the deserialized columns cache.
The cache keeps previously read and deserialized columns of `MergeTree` data
parts in memory, so repeated reads of the same row range do not pay for
decompression and deserialization again.

Each entry corresponds to a contiguous row range `[row_begin, row_end)` of a
single column of a single data part.

The cache is controlled by the server settings `columns_cache_size` and
`columns_cache_size_ratio`, and by the query-level settings `use_columns_cache`,
`enable_reads_from_columns_cache`, and `enable_writes_to_columns_cache`. It can
be dropped manually with `SYSTEM DROP COLUMNS CACHE`.

## Columns {#columns}

- `database` ([String](/sql-reference/data-types/string)) — Database name of the table the part belongs to. Empty if the table has been dropped but some of its entries are still in the cache.
- `table` ([String](/sql-reference/data-types/string)) — Table name. Empty if the table has been dropped.
- `table_uuid` ([UUID](/sql-reference/data-types/uuid)) — UUID of the table. Stable across renames.
- `part` ([String](/sql-reference/data-types/string)) — Name of the data part.
- `column` ([String](/sql-reference/data-types/string)) — Name of the column.
- `row_begin` ([UInt64](/sql-reference/data-types/int-uint)) — Starting row index of the cached range (inclusive).
- `row_end` ([UInt64](/sql-reference/data-types/int-uint)) — Ending row index of the cached range (exclusive).
- `rows` ([UInt64](/sql-reference/data-types/int-uint)) — Number of rows in the cached range (`row_end - row_begin`).
- `bytes` ([UInt64](/sql-reference/data-types/int-uint)) — In-memory size of the cached column data in bytes.

## Example {#example}

Total memory consumed by cached columns, per table:

```sql
SELECT
    database,
    table,
    formatReadableSize(sum(bytes)) AS size,
    sum(rows) AS rows,
    count() AS entries
FROM system.columns_cache
GROUP BY database, table
ORDER BY sum(bytes) DESC
```

Largest individual cache entries:

```sql
SELECT database, table, part, column, row_begin, row_end, rows, formatReadableSize(bytes) AS size
FROM system.columns_cache
ORDER BY bytes DESC
LIMIT 10
```

## See Also {#see-also}

- [`columns_cache_size`](/operations/server-configuration-parameters/settings#columns_cache_size) server setting
- [`use_columns_cache`](/operations/settings/settings#use_columns_cache) query setting
- [`SYSTEM DROP COLUMNS CACHE`](/sql-reference/statements/system#drop-columns-cache) query
