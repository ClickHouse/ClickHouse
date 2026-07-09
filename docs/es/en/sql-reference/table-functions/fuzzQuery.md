---
description: 'Introduce variaciones aleatorias en la cadena de consulta dada.'
sidebar_label: 'fuzzQuery'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzQuery
title: 'fuzzQuery'
doc_type: 'reference'
---

Introduce variaciones aleatorias en la cadena de consulta dada.

<div id="syntax">
  ## Sintaxis
</div>

```sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

<div id="arguments">
  ## Argumentos
</div>

| Argumento          | Descripción                                                                              |
| ------------------ | ---------------------------------------------------------------------------------------- |
| `query`            | (String) - La consulta de origen sobre la que se realizará el fuzzing.                   |
| `max_query_length` | (UInt64) - Longitud máxima que puede alcanzar la consulta durante el proceso de fuzzing. |
| `random_seed`      | (UInt64) - Una semilla aleatoria para producir resultados estables.                      |

<div id="returned_value">
  ## Valor devuelto
</div>

Un objeto de tabla con una única columna que contiene cadenas de consulta modificadas.

<div id="usage-example">
  ## Ejemplo de uso
</div>

```sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```response
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```