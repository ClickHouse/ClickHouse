---
description: 'Documentation for Manipulating Constraints'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: 'Manipulating Constraints'
doc_type: 'reference'
---

Constraints could be added, modified or deleted using following syntax:

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name CHECK expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name CHECK expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

`MODIFY CONSTRAINT` replaces the expression of an existing constraint, keeping its position in the table definition. It is equivalent to dropping the constraint and adding it again with the new expression. If the constraint does not exist, the query throws an error, unless `IF EXISTS` is specified.

See more on [constraints](../../../sql-reference/statements/create/table.md#constraints).

Queries will add, change or remove metadata about constraints from table, so they are processed immediately.

:::tip
Constraint check **will not be executed** on existing data if it was added or modified.
:::

All changes on replicated tables are broadcast to ZooKeeper and will be applied on other replicas as well.
