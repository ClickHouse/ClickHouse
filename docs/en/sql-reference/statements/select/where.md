---
toc_title: WHERE
---

# WHERE Clause {#select-where}

`WHERE` clause allows to filter the data that is coming from [FROM](../../../sql-reference/statements/select/from.md) clause of `SELECT`.

If there is a `WHERE` clause, it must contain an expression with the `UInt8` type. This is usually an expression with comparison and logical operators. Rows where this expression evaluates to 0 are excluded from further transformations or result.

`WHERE` expression is evaluated on the ability to use indexes and partition pruning, if the underlying table engine supports that.

!!! note "Note"
    There’s a filtering optimization called [prewhere](../../../sql-reference/statements/select/prewhere.md).
