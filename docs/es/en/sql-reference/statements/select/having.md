---
description: 'Documentación de la cláusula HAVING'
sidebar_label: 'HAVING'
slug: /sql-reference/statements/select/having
title: 'Cláusula HAVING'
doc_type: 'reference'
---

Permite filtrar los resultados de la agregación generados por [GROUP BY](/es/sql-reference/statements/select/group-by). Es similar a la cláusula [WHERE](../../../sql-reference/statements/select/where.md), pero la diferencia es que `WHERE` se aplica antes de la agregación, mientras que `HAVING` se aplica después.

Es posible hacer referencia en la cláusula `HAVING` a los resultados de agregación de la cláusula `SELECT` mediante su alias. Como alternativa, la cláusula `HAVING` puede filtrar resultados de agregaciones adicionales que no se devuelven en los resultados de la consulta.

<div id="example">
  ## Ejemplo
</div>

Si tiene una tabla `sales` como la siguiente:

```sql
CREATE TABLE sales
(
    region String,
    salesperson String,
    amount Float64
)
ORDER BY (region, salesperson);
```

Puede consultarlo así:

```sql
SELECT
    region,
    salesperson,
    sum(amount) AS total_sales
FROM sales
GROUP BY
    region,
    salesperson
HAVING total_sales > 10000
ORDER BY total_sales DESC;
```

Esto mostrará a los vendedores cuya suma total de ventas en su región supere los 10.000.

<div id="limitations">
  ## Limitaciones
</div>

No se puede usar `HAVING` si no se realiza ninguna agregación. Use `WHERE` en su lugar.