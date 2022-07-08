---
sidebar_label: WHERE
---

# WHERE Clause

`WHERE` clause allows to filter the data that is coming from [FROM](../../../sql-reference/statements/select/from.md) clause of `SELECT`.

If there is a `WHERE` clause, it must contain an expression with the `UInt8` type. This is usually an expression with comparison and logical operators. Rows where this expression evaluates to `0` are excluded from further transformations or result.

`WHERE` expression is evaluated on the ability to use indexes and partition pruning, if the underlying table engine supports that.

:::note    
There is a filtering optimization called [PREWHERE](../../../sql-reference/statements/select/prewhere.md).
:::

If you need to test a value for [NULL](../../../sql-reference/syntax.md#null-literal), use [IS NULL](../../operators/index.md#operator-is-null) and [IS NOT NULL](../../operators/index.md#is-not-null) operators or [isNull](../../../sql-reference/functions/functions-for-nulls.md#isnull) and [isNotNull](../../../sql-reference/functions/functions-for-nulls.md#isnotnull) functions.
Otherwise an expression with `NULL` never passes.

**Example**

To find numbers that are multiples of 3 and are greater than 10 execute the following query on the [numbers table](../../../sql-reference/table-functions/numbers.md):

``` sql
SELECT number FROM numbers(20) WHERE (number > 10) AND (number % 3 == 0);
```

Result:

``` text
┌─number─┐
│     12 │
│     15 │
│     18 │
└────────┘
```

Queries with `NULL` values:

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE=MergeTree() ORDER BY x;
INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT * FROM t_null WHERE y IS NULL;
SELECT * FROM t_null WHERE y != 0;
```

Result:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```
