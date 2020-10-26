---
toc_title: REPLACE
---

# REPLACE Clause {#select-replace}

You can use SELECT * REPLACE to specify one or more `expression AS identifier` clauses.  
Each identifier must match a column name from the `SELECT * statement`. In the output column list, the column that matches the identifier in a `REPLACE` clause is replaced by the expression in that `REPLACE` clause.
A `SELECT * REPLACE` statement does not change the names or order of columns. However, it can change the value and the value type.

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
SELECT * REPLACE(i + 1 AS i) from [db.]table
```

```
┌───i─┬──j─┬───k─┐
│ 101 │ 10 │ 324 │
│ 121 │  8 │  23 │
└─────┴────┴─────┘
```

`SELECT * REPLACE` is also can be used with `APPLY`:

``` sql
SELECT * REPLACE(i + 1 AS i) APPLY(sum) from [db.]table
```

```
┌─sum(plus(i, 1))─┬─sum(j)─┬─sum(k)─┐
│             222 │     18 │    347 │
└─────────────────┴────────┴────────┘
```

ClickHouse database also supports multiple `REPLACE` in a row. You can choose as many columns as you need and change its values or value types. 

For example:

``` sql
SELECT [db]table.* REPLACE(j + 2 AS j, i + 1 AS i) APPLY(avg) from [db.]table
```

```
┌─avg(plus(i, 1))─┬─avg(plus(j, 2))─┬─avg(k)─┐
│             111 │              11 │  173.5 │
└─────────────────┴─────────────────┴────────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/statements/select/replace/) <!--hide-->