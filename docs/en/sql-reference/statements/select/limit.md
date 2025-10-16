---
description: 'Documentation for LIMIT Clause'
sidebar_label: 'LIMIT'
slug: /sql-reference/statements/select/limit
title: 'LIMIT Clause'
doc_type: 'reference'
---

# LIMIT Clause

`LIMIT m` allows to select the first `m` rows from the result.

`LIMIT n, m` allows to select the `m` rows from the result after skipping the first `n` rows. The `LIMIT m OFFSET n` syntax is equivalent.

In the standard forms above, `n` and `m` are non-negative integers.

Negative limits are also supported:

`LIMIT -m` selects the last `m` rows from the result.

`LIMIT -m OFFSET -n` selects the last `m` rows after skipping the last `n` rows. The `LIMIT -n, -m` syntax is equivalent.

If there is no [ORDER BY](../../../sql-reference/statements/select/order-by.md) clause that explicitly sorts results, the choice of rows for the result may be arbitrary and non-deterministic.

:::note    
The number of rows in the result set can also depend on the [limit](../../../operations/settings/settings.md#limit) setting.
:::

## LIMIT ... WITH TIES Modifier {#limit--with-ties-modifier}

When you set `WITH TIES` modifier for `LIMIT n[,m]` and specify `ORDER BY expr_list`, you will get in result first `n` or `n,m` rows and all rows with same `ORDER BY` fields values equal to row at position `n` for `LIMIT n` and `m` for `LIMIT n,m`.

> **Note**  
> • `WITH TIES` is currently not supported with negative `LIMIT`.  

This modifier also can be combined with [ORDER BY ... WITH FILL modifier](/sql-reference/statements/select/order-by#order-by-expr-with-fill-modifier).

For example, the following query

```sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5
```

returns

```text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

but after apply `WITH TIES` modifier

```sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5 WITH TIES
```

it returns another rows set

```text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

cause row number 6 have same value "2" for field `n` as row number 5
