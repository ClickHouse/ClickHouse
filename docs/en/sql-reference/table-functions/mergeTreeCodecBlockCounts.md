---
description: 'Reports, per (part, column) of a MergeTree table, how many compressed blocks use each codec.'
sidebar_label: 'mergeTreeCodecBlockCounts'
sidebar_position: 77
slug: /sql-reference/table-functions/mergeTreeCodecBlockCounts
title: 'mergeTreeCodecBlockCounts'
doc_type: 'reference'
---

Reports, for each (part, column) of a MergeTree table, how many compressed blocks use each codec. This is the way to observe adaptive codec selection (see the `allow_experimental_adaptive_codec_selection` setting), which can pick a different codec per block for columns that use the default codec.

Selecting a codec-counts column walks the column's `.bin` block headers, so such a query reads the table's data files, not just metadata. Selecting only `part_name`/`column`/`subcolumns.names` stays metadata-only.

## Syntax {#syntax}

```sql
mergeTreeCodecBlockCounts(database, table)
```

## Arguments {#arguments}

| Argument   | Description                          |
|------------|--------------------------------------|
| `database` | The database name of the table.      |
| `table`    | The MergeTree table name.            |

## Returned value {#returned_value}

A table object with one row per (active part, column) of the source table:

- `part_name` ([String](/sql-reference/data-types/string)) — The active data part the column belongs to.
- `column` ([String](/sql-reference/data-types/string)) — The column name.
- `codec_block_counts` ([Map(String, UInt64)](/sql-reference/data-types/map)) — The number of compressed blocks grouped by codec across all substreams of the column. Empty for `Compact` parts, whose columns share one data file and so have no per-column codec attribution.
- `subcolumns.names` ([Array(String)](/sql-reference/data-types/array)) — The names of the subcolumns of the column.
- `subcolumns.codec_block_counts` ([Array(Map(String, UInt64))](/sql-reference/data-types/array)) — The number of compressed blocks of each subcolumn grouped by codec.

## Usage example {#usage-example}

```sql
CREATE TABLE mt (a UInt64) ENGINE = MergeTree ORDER BY a
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO mt SELECT number FROM numbers(100000);

SELECT column, codec_block_counts
FROM mergeTreeCodecBlockCounts(currentDatabase(), mt);
```

```text
┌─column─┬─codec_block_counts─┐
│ a      │ {'LZ4':13}         │
└────────┴────────────────────┘
```
