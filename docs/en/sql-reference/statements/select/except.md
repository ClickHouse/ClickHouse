---
toc_title: EXCEPT
---

# EXCEPT Clause {#select-except}

A `SELECT * EXCEPT` statement specifies the names of one or more columns to exclude from the result. All matching column names are omitted from the output.

For example: 

``` sql
SHOW CREATE [db.]table
```

```
┌─statement────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE [db.]table
(
    `i` Int64,
    `j` Int16,
    `k` Int64
)
ENGINE = TinyLog │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

``` sql
INSERT INTO [db.]table (100, 10, 324), (120, 8, 23)
```

``` sql
SELECT * EXCEPT (i) from [db.]table
```

```
┌──j─┬───k─┐
│ 10 │ 324 │
│  8 │  23 │
└────┴─────┘
```

`SELECT * EXCEPT` can be also used with `APPLY`:

``` sql
SELECT * EXCEPT(i) APPLY(sum) from [db.]table
```
```
┌─sum(j)─┬─sum(k)─┐
│     18 │    347 │
└────────┴────────┘
```

ClickHouse database also supports `SELECT * EXCEPT (col1, col2, ...)` syntax. This would select all columns except for those declared within the EXCEPT keyword. This is specially useful for tables with many columns.

``` sql
SELECT * EXCEPT(i, j) from [db.]table
```

```
┌───k─┐
│ 324 │
│  23 │
└─────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/statements/select/except/) <!--hide-->