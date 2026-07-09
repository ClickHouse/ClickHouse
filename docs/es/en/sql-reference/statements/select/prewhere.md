---
description: 'Documentación de la cláusula PREWHERE'
sidebar_label: 'PREWHERE'
slug: /sql-reference/statements/select/prewhere
title: 'Cláusula PREWHERE'
doc_type: 'reference'
---

PREWHERE es una optimización para aplicar el filtrado de forma más eficiente. Está habilitada de forma predeterminada incluso si la cláusula `PREWHERE` no se especifica explícitamente. Funciona moviendo automáticamente parte de la condición [WHERE](../../../sql-reference/statements/select/where.md) a la fase de PREWHERE. La función de la cláusula `PREWHERE` es únicamente controlar esta optimización si considera que puede hacerlo mejor que el comportamiento predeterminado.

Con la optimización de PREWHERE, primero solo se leen las columnas necesarias para ejecutar la expresión de PREWHERE. Después se leen las demás columnas necesarias para ejecutar el resto de la consulta, pero solo en aquellos bloques en los que la expresión de PREWHERE es `true` para al menos alguna fila. Si hay muchos bloques en los que la expresión de PREWHERE es `false` para todas las filas y PREWHERE necesita menos columnas que otras partes de la consulta, esto a menudo permite leer muchos menos datos del disco durante la ejecución de la consulta.

<div id="controlling-prewhere-manually">
  ## Control manual de PREWHERE
</div>

La cláusula tiene el mismo significado que la cláusula `WHERE`. La diferencia radica en qué datos se leen de la tabla. Al controlar manualmente `PREWHERE` para condiciones de filtrado que solo usan una minoría de las columnas de la consulta, pero que proporcionan un filtrado de datos significativo, se reduce el volumen de datos que debe leerse.

Una consulta puede especificar simultáneamente `PREWHERE` y `WHERE`. En este caso, `PREWHERE` precede a `WHERE`.

Si la configuración [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) está establecida en 0, se deshabilitan las heurísticas que mueven automáticamente partes de las expresiones de `WHERE` a `PREWHERE`.

Si la consulta tiene el modificador [FINAL](/es/sql-reference/statements/select/from#final-modifier), la optimización de `PREWHERE` no siempre es correcta. Solo se habilita si ambas configuraciones [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) y [optimize&#95;move&#95;to&#95;prewhere&#95;if&#95;final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final) están activadas.

:::note
La sección `PREWHERE` se ejecuta antes de `FINAL`, por lo que los resultados de las consultas `FROM ... FINAL` pueden verse sesgados al usar `PREWHERE` con campos que no están en la sección `ORDER BY` de una tabla.
:::

<div id="limitations">
  ## Limitaciones
</div>

`PREWHERE` solo es compatible con las tablas de la familia [*MergeTree](../../../engines/table-engines/mergetree-family/index.md).

<div id="example">
  ## Ejemplo
</div>

```sql
CREATE TABLE mydata
(
    `A` Int64,
    `B` Int8,
    `C` String
)
ENGINE = MergeTree
ORDER BY A AS
SELECT
    number,
    0,
    if(number between 1000 and 2000, 'x', toString(number))
FROM numbers(10000000);

SELECT count()
FROM mydata
WHERE (B = 0) AND (C = 'x');

1 row in set. Elapsed: 0.074 sec. Processed 10.00 million rows, 168.89 MB (134.98 million rows/s., 2.28 GB/s.)

-- let's enable tracing to see which predicate are moved to PREWHERE
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE  
-- Clickhouse moves automatically `B = 0` to PREWHERE, but it has no sense because B is always 0.

-- Let's move other predicate `C = 'x'` 

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- This query with manual `PREWHERE` processes slightly less data: 158.89 MB VS 168.89 MB
```