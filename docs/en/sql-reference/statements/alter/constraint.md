---
toc_priority: 43
toc_title: CONSTRAINT
---

# Manipulating Constraints {#manipulations-with-constraints}

Constraints could be added or deleted using following syntax:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

See more on [constraints](../../../sql-reference/statements/create/table.md#constraints).

Queries will add or remove metadata about constraints from table so they are processed immediately.

!!! warning "Warning"
    Constraint check **will not be executed** on existing data if it was added.

All changes on replicated tables are broadcasted to ZooKeeper and will be applied on other replicas as well.
