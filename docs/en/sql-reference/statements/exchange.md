---
description: 'Documentation for EXCHANGE Statement'
sidebar_label: 'EXCHANGE'
sidebar_position: 49
slug: /sql-reference/statements/exchange
title: 'EXCHANGE Statement'
doc_type: 'reference'
---

# EXCHANGE Statement

Exchanges the names of two tables or dictionaries atomically.
This task can also be accomplished with a [`RENAME`](./rename.md) query using a temporary name, but the operation is not atomic in that case.

:::note    
The `EXCHANGE` query is supported by the [`Atomic`](../../engines/database-engines/atomic.md) and [`Shared`](/cloud/reference/shared-catalog#shared-database-engine) database engines only.
:::

**Syntax**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

## EXCHANGE TABLES {#exchange-tables}

Exchanges the names of two tables.

**Syntax**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

### Multiple Table Exchanges

You can exchange multiple table pairs in a single query by separating them with commas:

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B, [db2.]table_C AND [db3.]table_D [, ...] [ON CLUSTER cluster]
```

:::note
When exchanging multiple table pairs, the exchanges are performed **sequentially, not atomically**. If an error occurs during the operation, some table pairs may have been exchanged while others have not. This is different from a single-pair exchange, which is atomic within `Atomic` and `Shared` database engines.
:::

**Example**

```sql title="Query"
-- Create tables
CREATE TABLE a (a UInt8) ENGINE=Memory;
CREATE TABLE b (b UInt8) ENGINE=Memory;
CREATE TABLE c (c UInt8) ENGINE=Memory;
CREATE TABLE d (d UInt8) ENGINE=Memory;

-- Exchange two pairs of tables in one query
EXCHANGE TABLES a AND b, c AND d;

SHOW TABLE a;
SHOW TABLE b;
SHOW TABLE c;
SHOW TABLE a;
-- Now table 'a' has the structure of 'b', table 'b' has the structure of 'a',
-- table 'c' has the structure of 'd', and table 'd' has the structure of 'c'
```

```sql title="Response"
┌─statement──────────────┐
│ CREATE TABLE default.a↴│
│↳(                     ↴│
│↳    `b` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
┌─statement──────────────┐
│ CREATE TABLE default.b↴│
│↳(                     ↴│
│↳    `a` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘

┌─statement──────────────┐
│ CREATE TABLE default.c↴│
│↳(                     ↴│
│↳    `d` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
┌─statement──────────────┐
│ CREATE TABLE default.a↴│
│↳(                     ↴│
│↳    `b` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
```


## EXCHANGE DICTIONARIES {#exchange-dictionaries}

Exchanges the names of two dictionaries.

**Syntax**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**See Also**

- [Dictionaries](../../sql-reference/dictionaries/index.md)
