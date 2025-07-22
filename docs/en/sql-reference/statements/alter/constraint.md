---
description: 'Documentation for Manipulating Constraints'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: 'Manipulating Constraints'
---

# Manipulating Constraints

Constraints could be added or deleted using following syntax:

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name CHECK expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

See more on [constraints](../../../sql-reference/statements/create/table.md#constraints).

Queries will add or remove metadata about constraints from table, so they are processed immediately.

:::tip
Constraint check **will not be executed** on existing data if it was added.
:::

All changes on replicated tables are broadcast to ZooKeeper and will be applied on other replicas as well.
