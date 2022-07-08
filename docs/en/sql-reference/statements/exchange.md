---
sidebar_position: 49
sidebar_label: EXCHANGE
---

# EXCHANGE Statement

Exchanges the names of two tables or dictionaries atomically.
This task can also be accomplished with a [RENAME](./rename.md) query using a temporary name, but the operation is not atomic in that case.

:::note    
The `EXCHANGE` query is supported by the [Atomic](../../engines/database-engines/atomic.md) database engine only.
:::

**Syntax**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

## EXCHANGE TABLES

Exchanges the names of two tables.

**Syntax**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

## EXCHANGE DICTIONARIES

Exchanges the names of two dictionaries.

**Syntax**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**See Also**

-   [Dictionaries](../../sql-reference/dictionaries/index.md)
