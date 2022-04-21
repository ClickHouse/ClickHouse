---
sidebar_position: 41
sidebar_label: SAMPLE BY
---

# Manipulating Sampling-Key Expressions {#manipulations-with-sampling-key-expressions}

Syntax:

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

The command changes the [sampling key](../../../engines/table-engines/mergetree-family/mergetree.md) of the table to `new_expression` (an expression or a tuple of expressions).

The command is lightweight in the sense that it only changes metadata. The primary key must contain the new sample key.

:::note    
It only works for tables in the [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) family (including [replicated](../../../engines/table-engines/mergetree-family/replication.md) tables).
:::