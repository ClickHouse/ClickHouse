---
description: 'Documentación del operador `EXISTS`'
slug: /sql-reference/operators/exists
title: 'EXISTS'
doc_type: 'reference'
---

El operador `EXISTS` comprueba si el resultado de una subconsulta contiene registros. Si está vacío, el operador devuelve `0`. En caso contrario, devuelve `1`.

`EXISTS` también puede usarse en una cláusula [WHERE](../../sql-reference/statements/select/where.md).

:::tip
Las referencias a tablas y columnas de la consulta principal no son compatibles en una subconsulta.
:::

**Sintaxis**

```sql
EXISTS(subquery)
```

**Ejemplo**

Consulta para comprobar la existencia de valores en una subconsulta:

```sql title="Query"
SELECT EXISTS(SELECT * FROM numbers(10) WHERE number > 8), EXISTS(SELECT * FROM numbers(10) WHERE number > 11)
```

```text title="Response"
┌─in(1, _subquery1)─┬─in(1, _subquery2)─┐
│                 1 │                 0 │
└───────────────────┴───────────────────┘
```

Consulta con una subconsulta que devuelve varias filas:

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 8);
```

```text title="Response"
┌─count()─┐
│      10 │
└─────────┘
```

Consulta con una subconsulta que devuelve un resultado vacío:

```sql title="Query"
SELECT count() FROM numbers(10) WHERE EXISTS(SELECT number FROM numbers(10) WHERE number > 11);
```

```text title="Response"
┌─count()─┐
│       0 │
└─────────┘
```