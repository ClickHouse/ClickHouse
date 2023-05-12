---
sidebar_label: LIMIT
---

# LIMIT Clause

`LIMIT m` allows to select the first `m` rows from the result.

`LIMIT n, m` allows to select the `m` rows from the result after skipping the first `n` rows. The `LIMIT m OFFSET n` syntax is equivalent.

`n` and `m` must be non-negative integers.

If there is no [ORDER BY](../../../sql-reference/statements/select/order-by.md) clause that explicitly sorts results, the choice of rows for the result may be arbitrary and non-deterministic.

:::note    
The number of rows in the result set can also depend on the [limit](../../../operations/settings/settings.md#limit) setting.
:::

## LIMIT … WITH TIES Modifier

When you set `WITH TIES` modifier for `LIMIT n[,m]` and specify `ORDER BY expr_list`, you will get in result first `n` or `n,m` rows and all rows with same `ORDER BY` fields values equal to row at position `n` for `LIMIT n` and `m` for `LIMIT n,m`.

This modifier also can be combined with [ORDER BY … WITH FILL modifier](../../../sql-reference/statements/select/order-by.md#orderby-with-fill).

For example, the following query

``` sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5
```

returns

``` text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

but after apply `WITH TIES` modifier

``` sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5 WITH TIES
```

it returns another rows set

``` text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

cause row number 6 have same value “2” for field `n` as row number 5
