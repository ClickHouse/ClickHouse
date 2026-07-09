---
description: 'Documentação para o operador `EXISTS`'
slug: /sql-reference/operators/exists
title: 'EXISTS'
doc_type: 'reference'
---

O operador `EXISTS` verifica quantos registros há no resultado de uma subconsulta. Se o resultado estiver vazio, o operador retornará `0`. Caso contrário, retornará `1`.

`EXISTS` também pode ser usado em uma cláusula [WHERE](../../sql-reference/statements/select/where.md).

:::tip
Não há suporte a referências a tabelas e colunas da consulta principal em uma subconsulta.
:::

**Sintaxe**

```sql
EXISTS(subquery)
```

**Exemplo**

Consulta que verifica a existência de valores em uma subconsulta:

```sql title="Query"
SELECT EXISTS(SELECT * FROM numbers(10) WHERE number > 8), EXISTS(SELECT * FROM numbers(10) WHERE number > 11)
```

```text title="Response"
┌─in(1, _subquery1)─┬─in(1, _subquery2)─┐
│                 1 │                 0 │
└───────────────────┴───────────────────┘
```

Consulta com uma subconsulta que retorna várias linhas:

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 8);
```

```text title="Response"
┌─count()─┐
│      10 │
└─────────┘
```

Consulta com uma subconsulta que retorna um resultado vazio:

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 11);
```

```text title="Response"
┌─count()─┐
│       0 │
└─────────┘
```