---
description: 'Reports, per (part, column, substream) of a MergeTree table, how many compressed blocks use each codec.'
sidebar_label: 'mergeTreeCodecBlockCounts'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeCodecBlockCounts
title: 'mergeTreeCodecBlockCounts'
doc_type: 'reference'
---

Reports, per (part, column, substream) of a MergeTree table, how many compressed blocks use each codec.

Selecting `codec_block_counts` reads `.bin` data files, not just metadata. The other columns are metadata-only.

Parts that do not record their substreams in `columns_substreams.txt` are not listed.

If a row policy applies to the table for the current user, reading `codec_block_counts` throws `ACCESS_DENIED`, because the counts would cover rows the policy hides. The other columns stay readable, `system.parts_columns` reports them regardless of row policies.

## Syntax {#syntax}

```sql
mergeTreeCodecBlockCounts(database, table)
```

## Arguments {#arguments}

| Argument   | Description                          |
|------------|--------------------------------------|
| `database` | The database name of the table.      |
| `table`    | The MergeTree table name.            |

## Returned value {#returned-value}

A table object with one row per (active part, column, substream) of the source table:

- `part_name` ([String](/sql-reference/data-types/string)) — The active data part the column belongs to.
- `column` ([String](/sql-reference/data-types/string)) — The column name.
- `substream` ([String](/sql-reference/data-types/string)) — The physical stream of the column the counts are for. Matches `system.parts_columns.substreams`.
- `data_compressed_bytes` ([Nullable(UInt64)](/sql-reference/data-types/nullable)) — Size of compressed data in the substream, in bytes. `NULL` for `Compact` parts.
- `data_uncompressed_bytes` ([Nullable(UInt64)](/sql-reference/data-types/nullable)) — Size of uncompressed data in the substream, in bytes. `NULL` for `Compact` parts.
- `codec_block_counts` ([Map(String, UInt64)](/sql-reference/data-types/map)) — The number of compressed blocks of this substream grouped by codec. Empty for `Compact` parts, whose columns share one data file and so have no per-stream codec attribution.

## Usage example {#usage-example}

```sql
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO mt SELECT number FROM numbers(100000);

SELECT column, substream, codec_block_counts
FROM mergeTreeCodecBlockCounts(currentDatabase(), mt);
```

```text
┌─column─┬─substream─┬─codec_block_counts─┐
│ a      │ a         │ {'LZ4':13}         │
└────────┴───────────┴────────────────────┘
```

Column-level totals are a `GROUP BY column` with [`sumMap`](/sql-reference/aggregate-functions/reference/summap):

```sql
SELECT column, sumMap(codec_block_counts)
FROM mergeTreeCodecBlockCounts(currentDatabase(), mt)
GROUP BY column;
```

A `LowCardinality` column reports its dictionary and indexes streams separately:

```sql
CREATE TABLE mt_lc (s LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO mt_lc SELECT toString(number % 1000) FROM numbers(1000000);

SELECT substream, codec_block_counts
FROM mergeTreeCodecBlockCounts(currentDatabase(), mt_lc)
WHERE column = 's'
ORDER BY substream;
```

```text
┌─substream─┬─codec_block_counts─┐
│ s         │ {'LZ4':31}         │
│ s.dict    │ {'LZ4':1}          │
└───────────┴────────────────────┘
```
