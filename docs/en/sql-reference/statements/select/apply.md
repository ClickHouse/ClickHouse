---
toc_title: APPLY 
---

# APPLY Clause {#apply-clause}

The `APPLY` operator allows you to sum the results of each row across columns.

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
 SELECT * APPLY(sum) from [db.]table
```

```
┌─sum(i)─┬─sum(j)─┬─sum(k)─┐
│    220 │     18 │    347 │
└────────┴────────┴────────┘
```

You can also use `APPLY` to find average values of each row across columns.

For example: 

``` sql
SELECT [db.]table.* APPLY(avg) from [db.]table
```

```
┌─avg(i)─┬─avg(j)─┬─avg(k)─┐
│    110 │      9 │  173.5 │
└────────┴────────┴────────┘
```

The operator can also help to find out the maxium length of all string columns:

``` sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) from [db.]table
```

```
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/statements/select/apply/) <!--hide-->