---
slug: /sql-reference/table-functions/generate_series
sidebar_position: 146
sidebar_label: 'generate_series'
title: 'generate_series (generateSeries)'
description: 'Returns a table with the single `generate_series` column (UInt64) that contains integers from start to stop inclusively.'
doc_type: 'reference'
---

Alias: `generateSeries`

## Syntax {#syntax}

Returns a table with the single 'generate_series' column (`UInt64`) that contains integers from start to stop inclusively:

```sql
generate_series(START, STOP)
```

Returns a table with the single 'generate_series' column (`UInt64`) that contains integers from start to stop inclusively with spacing between values given by `STEP`:

```sql
generate_series(START, STOP, STEP)
```

`STEP` can be negative, in which case the series is generated in descending order from `START` down to `STOP`. If `STEP` is negative and `START < STOP`, the result is empty.

## Examples {#examples}

The following queries return tables with the same content but different column names:

```sql
SELECT * FROM numbers(10, 5);
```

```response
┌─number─┐
│     10 │
│     11 │
│     12 │
│     13 │
│     14 │
└────────┘
```

```sql
SELECT * FROM generate_series(10, 14);
```

```response
┌─generate_series─┐
│              10 │
│              11 │
│              12 │
│              13 │
│              14 │
└─────────────────┘
```

And the following queries return tables with the same content but different column names (but the second option is more efficient):

```sql
SELECT * FROM numbers(10, 11) WHERE number % 3 == (10 % 3);
```

```response
┌─number─┐
│     10 │
│     13 │
│     16 │
│     19 │
└────────┘
```

```sql
SELECT * FROM generate_series(10, 20, 3);
```

```response
┌─generate_series─┐
│              10 │
│              13 │
│              16 │
│              19 │
└─────────────────┘
```

Generate a descending series:

```sql
SELECT * FROM generate_series(9, 0, -1);
```

```response
┌─generate_series─┐
│               9 │
│               8 │
│               7 │
│               6 │
│               5 │
│               4 │
│               3 │
│               2 │
│               1 │
│               0 │
└─────────────────┘
```
