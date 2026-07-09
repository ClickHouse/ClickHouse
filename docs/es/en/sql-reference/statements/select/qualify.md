---
description: 'Documentación de la cláusula QUALIFY'
sidebar_label: 'QUALIFY'
slug: /sql-reference/statements/select/qualify
title: 'Cláusula QUALIFY'
doc_type: 'reference'
---

Permite filtrar los resultados de las funciones de ventana. Es similar a la cláusula [WHERE](../../../sql-reference/statements/select/where.md), pero la diferencia es que `WHERE` se aplica antes de evaluar las funciones de ventana, mientras que `QUALIFY` se aplica después.

Es posible hacer referencia desde la cláusula `QUALIFY` a los resultados de las funciones de ventana de la cláusula `SELECT` mediante su alias. Como alternativa, la cláusula `QUALIFY` puede filtrar por los resultados de funciones de ventana adicionales que no se devuelven en los resultados de la consulta.

<div id="limitations">
  ## Limitaciones
</div>

No se puede usar `QUALIFY` si no hay ninguna función de ventana que evaluar. En su lugar, usa `WHERE`.

<div id="examples">
  ## Ejemplos
</div>

Ejemplo:

```sql
SELECT number, COUNT() OVER (PARTITION BY number % 3) AS partition_count
FROM numbers(10)
QUALIFY partition_count = 4
ORDER BY number;
```

```text
┌─number─┬─partition_count─┐
│      0 │               4 │
│      3 │               4 │
│      6 │               4 │
│      9 │               4 │
└────────┴─────────────────┘
```