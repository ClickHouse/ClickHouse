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

In this format, data is written and read by [blocks](/development/architecture#block) in a binary format.
For each block, the number of columns, the size of each column in bytes, and then the raw bytes of each column are recorded one after another.

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
    count() AS cnt,
    sum(col_1) AS sum_col_1,
    sum(col_2)  AS sum_col_2
FROM file(
    'squares.buffers',
    'Buffers',
    'col_1 UInt64, col_2 UInt64'
);
```

```txt
  ┌─cnt─┬─sum_col_1─┬─sum_col_2─┐
  │  10 │        45 │       285 │
  └─────┴───────────┴───────────┘
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
