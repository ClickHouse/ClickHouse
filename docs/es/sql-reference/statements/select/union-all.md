---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# UNION ALL Cláusula {#union-all-clause}

Usted puede utilizar `UNION ALL` combinar cualquier número de `SELECT` consultas extendiendo sus resultados. Ejemplo:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Las columnas de resultados coinciden con su índice (orden dentro `SELECT`). Si los nombres de columna no coinciden, los nombres del resultado final se toman de la primera consulta.

La fundición de tipo se realiza para uniones. Por ejemplo, si dos consultas que se combinan tienen el mismo campo-`Nullable` y `Nullable` tipos de un tipo compatible, el resultado `UNION ALL` tiene una `Nullable` campo de tipo.

Consultas que son parte de `UNION ALL` no se puede encerrar entre corchetes redondos. [ORDER BY](order-by.md) y [LIMIT](limit.md) se aplican a consultas separadas, no al resultado final. Si necesita aplicar una conversión al resultado final, puede colocar todas las consultas con `UNION ALL` en una subconsulta en el [FROM](from.md) clausula.

## Limitacion {#limitations}

Solo `UNION ALL` se admite. Regular `UNION` (`UNION DISTINCT`) no es compatible. Si necesita `UNION DISTINCT` usted puede escribir `SELECT DISTINCT` de una subconsulta que contiene `UNION ALL`.

## Detalles de implementación {#implementation-details}

Consultas que son parte de `UNION ALL` se puede ejecutar simultáneamente, y sus resultados se pueden mezclar juntos.
