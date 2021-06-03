---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Cláusula DISTINCT {#select-distinct}

Si `SELECT DISTINCT` se especifica, sólo las filas únicas permanecerán en un resultado de consulta. Por lo tanto, solo quedará una sola fila de todos los conjuntos de filas completamente coincidentes en el resultado.

## Procesamiento nulo {#null-processing}

`DISTINCT` trabaja con [NULL](../../syntax.md#null-literal) como si `NULL` Era un valor específico, y `NULL==NULL`. En otras palabras, en el `DISTINCT` resultados, diferentes combinaciones con `NULL` se producen sólo una vez. Se diferencia de `NULL` procesamiento en la mayoría de los otros contextos.

## Alternativa {#alternatives}

Es posible obtener el mismo resultado aplicando [GROUP BY](group-by.md) en el mismo conjunto de valores especificados como `SELECT` cláusula, sin utilizar ninguna función agregada. Pero hay pocas diferencias de `GROUP BY` enfoque:

-   `DISTINCT` se puede aplicar junto con `GROUP BY`.
-   Cuando [ORDER BY](order-by.md) se omite y [LIMIT](limit.md) se define, la consulta deja de ejecutarse inmediatamente después de que se haya leído el número requerido de filas diferentes.
-   Los bloques de datos se generan a medida que se procesan, sin esperar a que finalice la ejecución de toda la consulta.

## Limitacion {#limitations}

`DISTINCT` no se admite si `SELECT` tiene al menos una columna de matriz.

## Ejemplos {#examples}

ClickHouse admite el uso de `DISTINCT` y `ORDER BY` para diferentes columnas en una consulta. El `DISTINCT` cláusula se ejecuta antes de `ORDER BY` clausula.

Tabla de ejemplo:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

Al seleccionar datos con el `SELECT DISTINCT a FROM t1 ORDER BY b ASC` consulta, obtenemos el siguiente resultado:

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

Si cambiamos la dirección de clasificación `SELECT DISTINCT a FROM t1 ORDER BY b DESC`, obtenemos el siguiente resultado:

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

Fila `2, 4` se cortó antes de clasificar.

Tenga en cuenta esta especificidad de implementación al programar consultas.
