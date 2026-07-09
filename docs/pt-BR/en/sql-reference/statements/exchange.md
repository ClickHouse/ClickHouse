---
description: 'Documentação da instrução EXCHANGE'
sidebar_label: 'EXCHANGE'
sidebar_position: 49
slug: /sql-reference/statements/exchange
title: 'Instrução EXCHANGE'
doc_type: 'reference'
---

Troca atomicamente os nomes de duas tabelas ou dicionários.
Essa tarefa também pode ser realizada com uma consulta [`RENAME`](./rename.md) usando um nome temporário, mas, nesse caso, a operação não é atômica.

:::note
A consulta `EXCHANGE` é compatível apenas com os motores de banco de dados [`Atomic`](../../engines/database-engines/atomic.md) e [`Shared`](/pt-BR/cloud/reference/shared-catalog#shared-database-engine).
:::

**Sintaxe**

```sql
EXCHANGE TABLES|DICTIONARIES [db0.]name_A AND [db1.]name_B [ON CLUSTER cluster]
```

<div id="exchange-tables">
  ## EXCHANGE TABLES
</div>

Troca os nomes de duas tabelas.

**Sintaxe**

```sql
EXCHANGE TABLES [db0.]table_A AND [db1.]table_B [ON CLUSTER cluster]
```

<div id="exchange-multiple-tables">
  ### EXCHANGE DE MÚLTIPLAS TABELAS
</div>

Você pode trocar vários pares de tabelas em uma única consulta, separando-os por vírgulas.

:::note
Ao trocar vários pares de tabelas, as trocas são realizadas **de forma sequencial, não atômica**. Se ocorrer um erro durante a operação, alguns pares de tabelas podem ter sido trocados, enquanto outros não.
:::

**Exemplo**

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

Troca os nomes de dois dicionários.

**Sintaxe**

```sql
EXCHANGE DICTIONARIES [db0.]dict_A AND [db1.]dict_B [ON CLUSTER cluster]
```

**Veja também**

* [Dicionários](./create/dictionary/overview.md)