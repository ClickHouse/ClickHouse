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

### EXCHANGE MULTIPLE TABLES {#exchange-multiple-tables}

You can exchange multiple table pairs in a single query by separating them with commas.

:::note
When exchanging multiple table pairs, the exchanges are performed **sequentially, not atomically**. If an error occurs during the operation, some table pairs may have been exchanged while others have not.
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
SHOW TABLE d;
```

```sql title="Response"
-- Now table 'a' has the structure of 'b', and table 'b' has the structure of 'a'
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

-- Now table 'c' has the structure of 'd', and table 'd' has the structure of 'c'
┌─statement──────────────┐
│ CREATE TABLE default.c↴│
│↳(                     ↴│
│↳    `d` UInt8         ↴│
│↳)                     ↴│
│↳ENGINE = Memory        │
└────────────────────────┘
┌─statement──────────────┐
│ CREATE TABLE default.d↴│
│↳(                     ↴│
│↳    `c` UInt8         ↴│
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

- [Dictionaries](./create/dictionary/overview.md)
