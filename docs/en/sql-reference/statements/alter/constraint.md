---
slug: /en/sql-reference/statements/alter/constraint
sidebar_position: 43
sidebar_label: CONSTRAINT
---

# Manipulating Constraints

Constraints could be added or deleted using following syntax:

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name CHECK expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

See more on [constraints](../../../sql-reference/statements/create/table.md#constraints).

Queries will add or remove metadata about constraints from table, so they are processed immediately.

:::tip
Constraint check **will not be executed** on existing data if it was added.
:::

All changes on replicated tables are broadcasted to ZooKeeper and will be applied on other replicas as well.
