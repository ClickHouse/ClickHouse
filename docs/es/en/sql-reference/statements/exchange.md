---
description: 'Documentación de la Sentencia EXCHANGE'
sidebar_label: 'EXCHANGE'
sidebar_position: 49
slug: /sql-reference/statements/exchange
title: 'Sentencia EXCHANGE'
doc_type: 'reference'
---

Intercambia de forma atómica los nombres de dos tablas o diccionarios.
Esta tarea también puede realizarse con una consulta [`RENAME`](./rename.md) usando un nombre temporal, pero en ese caso la operación no es atómica.

:::note
La consulta `EXCHANGE` solo es compatible con los motores de base de datos [`Atomic`](../../engines/database-engines/atomic.md) y [`Shared`](/es/cloud/reference/shared-catalog#shared-database-engine).
:::

**Sintaxis**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

<div id="exchange-tables">
  ## EXCHANGE TABLES
</div>

Intercambia los nombres de dos tablas.

**Sintaxis**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

<div id="exchange-multiple-tables">
  ### EXCHANGE DE VARIAS TABLAS
</div>

Puede intercambiar varios pares de tablas en una sola consulta separándolos con comas.

:::note
Al intercambiar varios pares de tablas, los intercambios se realizan **de forma secuencial, no atómica**. Si se produce un error durante la operación, es posible que algunos pares de tablas se hayan intercambiado y otros no.
:::

**Ejemplo**

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

Intercambia los nombres de dos diccionarios.

**Sintaxis**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**Véase también**

* [Diccionarios](./create/dictionary/overview.md)