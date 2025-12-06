---
alias: []
description: 'Documentation for the Buffers format'
input_format: true
keywords: ['Buffers']
output_format: true
slug: /interfaces/formats/Buffers
title: 'Buffers'
doc_type: 'reference'
---

| Input | Output | Alias |
|-------|--------|-------|
| ✔     | ✔      |       |

## Description {#description}

`Buffers` is a very simple binary format for **ephemeral** data exchange, where both the consumer and producer already know the schema and column order.

Unlike [Native](./Native.md), it does **not** store column names, column types, or any extra metadata.

In this format, data is written and read by [blocks](/development/architecture#block) in a binary format. Buffers uses the same per-column binary representation as the [Native](./Native.md) format and respects the same Native format settings.

For each block, the following sequence is written:
1. Number of columns (UInt64, little-endian).
2. Number of rows (UInt64, little-endian).
3. For each column:
- Total byte size of the serialized column data (UInt64, little-endian).
- Serialized column data bytes, exactly as in the [Native](./Native.md) format.

## Example usage {#example-usage}

Write to a file:

```sql
SELECT
    number AS num,
    number * number AS num_square
FROM numbers(10)
INTO OUTFILE 'squares.buffers'
FORMAT Buffers;
```

Read back with an explicit column types:

```sql
SELECT
    *
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

If you have a table with same column types, you can populate it directly:

```sql
CREATE TABLE number_squares
(
    a UInt64,
    b UInt64
) ENGINE = Memory;

INSERT INTO number_squares
FROM INFILE 'squares.buffers'
FORMAT Buffers;
```

Inspect the table:

```sql
SELECT * FROM number_squares;
```

```txt
  ┌─a─┬──b─┐
  │ 0 │  0 │
  │ 1 │  1 │
  │ 2 │  4 │
  │ 3 │  9 │
  │ 4 │ 16 │
  │ 5 │ 25 │
  │ 6 │ 36 │
  │ 7 │ 49 │
  │ 8 │ 64 │
  │ 9 │ 81 │
  └───┴────┘
```

## Format settings {#format-settings}
