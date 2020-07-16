---
toc_priority: 40
toc_title: UPDATE
---

# ALTER TABLE … UPDATE Statements {#alter-table-update-statements}

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

Allows to asynchronously manipulate data matching the specified filtering expression. Implemented as a [mutation](../../../sql-reference/statements/index.md#mutations).

!!! note "Note"
    The `ALTER TABLE` prefix makes this syntax different from most other systems supporting SQL. It is intended to signify that unlike similar queries in OLTP databases this is a heavy operation not designed for frequent use.

The `filter_expr` must be of type `UInt8`. This query updates values of specified columns to the values of corresponding expressions in rows for which the `filter_expr` takes a non-zero value. Values are casted to the column type using the `CAST` operator. Updating columns that are used in the calculation of the primary or the partition key is not supported.

One query can contain several commands separated by commas.
