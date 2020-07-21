---
toc_priority: 39
toc_title: DELETE
---

# ALTER TABLE … DELETE Statement {#alter-mutations}

``` sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE WHERE filter_expr
```

Allows to asynchronously delete data matching the specified filtering expression. Implemented as a [mutation](../../../sql-reference/statements/index.md#mutations).

!!! note "Note"
    The `ALTER TABLE` prefix makes this syntax different from most other systems supporting SQL. It is intended to signify that unlike similar queries in OLTP databases this is a heavy operation not designed for frequent use.

The `filter_expr` must be of type `UInt8`. The query deletes rows in the table for which this expression takes a non-zero value.

One query can contain several commands separated by commas.
