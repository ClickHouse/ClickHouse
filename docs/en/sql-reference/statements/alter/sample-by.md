---
slug: /en/sql-reference/statements/alter/sample-by
sidebar_position: 41
sidebar_label: SAMPLE BY
title: "Manipulating Sampling-Key Expressions"
---

# Manipulating SAMPLE BY expression

The following operations are available:

## MODIFY

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

The command changes the [sampling key](../../../engines/table-engines/mergetree-family/mergetree.md) of the table to `new_expression` (an expression or a tuple of expressions). The primary key must contain the new sample key.

## REMOVE

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

The command removes the [sampling key](../../../engines/table-engines/mergetree-family/mergetree.md) of the table.


The commands `MODIFY` and `REMOVE` are lightweight in the sense that they only change metadata or remove files.

:::note    
It only works for tables in the [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) family (including [replicated](../../../engines/table-engines/mergetree-family/replication.md) tables).
:::
