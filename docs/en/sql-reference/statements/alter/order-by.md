---
toc_priority: 41
toc_title: ORDER BY
---

# Manipulating Key Expressions {#manipulations-with-key-expressions}

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

The command changes the [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md) of the table to `new_expression` (an expression or a tuple of expressions). Primary key remains the same.

The command is lightweight in a sense that it only changes metadata. To keep the property that data part rows are ordered by the sorting key expression you cannot add expressions containing existing columns to the sorting key (only columns added by the `ADD COLUMN` command in the same `ALTER` query, without default column value).

!!! note "Note"
    It only works for tables in the [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) family (including [replicated](../../../engines/table-engines/mergetree-family/replication.md) tables).
