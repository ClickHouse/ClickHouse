---
toc_title: WHERE
---

# WHERE Clause {#select-where}

`WHERE` clause allows to filter the data that is coming from [FROM](../../../sql-reference/statements/select/from.md) clause of `SELECT`.

If there is a `WHERE` clause, it must contain an expression with the `UInt8` type. This is usually an expression with comparison and logical operators. Rows where this expression evaluates to 0 are excluded from further transformations or result.

`WHERE` expression is evaluated on the ability to use indexes and partition pruning, if the underlying table engine supports that.

!!! note "Note"
    There is a filtering optimization called [prewhere](../../../sql-reference/statements/select/prewhere.md).

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

**See Also**

-   [EXISTS](../../../sql-reference/operators/exists.md) operator
