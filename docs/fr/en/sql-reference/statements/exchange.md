---
description: "Documentation de l’instruction EXCHANGE"
sidebar_label: 'EXCHANGE'
sidebar_position: 49
slug: /sql-reference/statements/exchange
title: "Instruction EXCHANGE"
doc_type: 'reference'
---

Échange de façon atomique les noms de deux tables ou dictionnaires.
Cette opération peut également être réalisée avec une requête [`RENAME`](./rename.md) utilisant un nom temporaire, mais, dans ce cas, elle n’est pas atomique.

:::note
La requête `EXCHANGE` est prise en charge uniquement par les moteurs de base de données [`Atomic`](../../engines/database-engines/atomic.md) et [`Shared`](/fr/cloud/reference/shared-catalog#shared-database-engine).
:::

**Syntaxe**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

<div id="exchange-tables">
  ## EXCHANGE TABLES
</div>

Échange les noms de deux tables.

**Syntaxe**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

<div id="exchange-multiple-tables">
  ### EXCHANGE DE PLUSIEURS TABLES
</div>

Vous pouvez échanger plusieurs paires de tables dans une même requête en les séparant par des virgules.

:::note
Lors de l’échange de plusieurs paires de tables, les échanges sont effectués **séquentiellement, et non de façon atomique**. Si une erreur se produit pendant l’opération, certaines paires de tables peuvent avoir été échangées alors que d’autres ne l’ont pas été.
:::

**Exemple**

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

<div id="exchange-dictionaries">
  ## EXCHANGE DICTIONARIES
</div>

Échange les noms de deux dictionnaires.

**Syntaxe**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**Voir aussi**

* [Dictionnaires](./create/dictionary/overview.md)