---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# GRUPO POR Cláusula {#select-group-by-clause}

`GROUP BY` cláusula cambia el `SELECT` consulta en un modo de agregación, que funciona de la siguiente manera:

-   `GROUP BY` clause contiene una lista de expresiones (o una sola expresión, que se considera la lista de longitud uno). Esta lista actúa como un “grouping key”, mientras que cada expresión individual será referida como un “key expressions”.
-   Todas las expresiones en el [SELECT](index.md), [HAVING](having.md), y [ORDER BY](order-by.md) clausula **deber** se calculará basándose en expresiones clave **o** en [funciones agregadas](../../../sql-reference/aggregate-functions/index.md) sobre expresiones no clave (incluidas columnas simples). En otras palabras, cada columna seleccionada de la tabla debe usarse en una expresión clave o dentro de una función agregada, pero no en ambas.
-   Resultado de la agregación `SELECT` la consulta contendrá tantas filas como valores únicos de “grouping key” en la tabla de origen. Por lo general, esto reduce significativamente el recuento de filas, a menudo en órdenes de magnitud, pero no necesariamente: el recuento de filas se mantiene igual si todo “grouping key” los valores fueron distintos.

!!! note "Nota"
    Hay una forma adicional de ejecutar la agregación sobre una tabla. Si una consulta contiene columnas de tabla solo dentro de funciones agregadas, el `GROUP BY clause` se puede omitir, y se asume la agregación por un conjunto vacío de claves. Tales consultas siempre devuelven exactamente una fila.

## Procesamiento NULL {#null-processing}

Para agrupar, ClickHouse interpreta [NULL](../../syntax.md#null-literal) como valor, y `NULL==NULL`. Se diferencia de `NULL` procesamiento en la mayoría de los otros contextos.

Aquí hay un ejemplo para mostrar lo que esto significa.

Supongamos que tienes esta tabla:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Consulta `SELECT sum(x), y FROM t_null_big GROUP BY y` resultados en:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

Se puede ver que `GROUP BY` para `y = NULL` resumir `x` como si `NULL` es este valor.

Si pasa varias teclas a `GROUP BY` el resultado le dará todas las combinaciones de la selección, como si `NULL` fueron un valor específico.

## CON TOTALS Modificador {#with-totals-modifier}

Si el `WITH TOTALS` se especifica el modificador, se calculará otra fila. Esta fila tendrá columnas clave que contienen valores predeterminados (zeros o líneas vacías) y columnas de funciones agregadas con los valores calculados en todas las filas (el “total” valor).

Esta fila adicional solo se produce en `JSON*`, `TabSeparated*`, y `Pretty*` formatos, por separado de las otras filas:

-   En `JSON*` formatos, esta fila se muestra como una ‘totals’ campo.
-   En `TabSeparated*` formatea, la fila viene después del resultado principal, precedida por una fila vacía (después de los otros datos).
-   En `Pretty*` formatea, la fila se muestra como una tabla separada después del resultado principal.
-   En los otros formatos no está disponible.

`WITH TOTALS` se puede ejecutar de diferentes maneras cuando HAVING está presente. El comportamiento depende de la ‘totals\_mode’ configuración.

### Configuración del procesamiento de totales {#configuring-totals-processing}

Predeterminada, `totals_mode = 'before_having'`. En este caso, ‘totals’ se calcula en todas las filas, incluidas las que no pasan por HAVING y `max_rows_to_group_by`.

Las otras alternativas incluyen solo las filas que pasan por HAVING en ‘totals’, y comportarse de manera diferente con el ajuste `max_rows_to_group_by` y `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. En otras palabras, ‘totals’ tendrá menos o el mismo número de filas que si `max_rows_to_group_by` se omitieron.

`after_having_inclusive` – Include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ en ‘totals’. En otras palabras, ‘totals’ tendrá más o el mismo número de filas como lo haría si `max_rows_to_group_by` se omitieron.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ en ‘totals’. De lo contrario, no los incluya.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

Si `max_rows_to_group_by` y `group_by_overflow_mode = 'any'` no se utilizan, todas las variaciones de `after_having` son los mismos, y se puede utilizar cualquiera de ellos (por ejemplo, `after_having_auto`).

Puede usar WITH TOTALS en subconsultas, incluidas las subconsultas en la cláusula JOIN (en este caso, se combinan los valores totales respectivos).

## Ejemplos {#examples}

Ejemplo:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

Sin embargo, a diferencia del SQL estándar, si la tabla no tiene ninguna fila (o no hay ninguna, o no hay ninguna después de usar WHERE para filtrar), se devuelve un resultado vacío, y no el resultado de una de las filas que contienen los valores iniciales de las funciones agregadas.

A diferencia de MySQL (y conforme a SQL estándar), no puede obtener algún valor de alguna columna que no esté en una función clave o agregada (excepto expresiones constantes). Para evitar esto, puede usar el ‘any’ función de agregado (obtener el primer valor encontrado) o ‘min/max’.

Ejemplo:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

Para cada valor de clave diferente encontrado, GROUP BY calcula un conjunto de valores de función agregados.

GROUP BY no se admite para columnas de matriz.

No se puede especificar una constante como argumentos para funciones agregadas. Ejemplo: sum(1). En lugar de esto, puedes deshacerte de la constante. Ejemplo: `count()`.

## Detalles de implementación {#implementation-details}

La agregación es una de las características más importantes de un DBMS orientado a columnas, y por lo tanto su implementación es una de las partes más optimizadas de ClickHouse. De forma predeterminada, la agregación se realiza en la memoria utilizando una tabla hash. Tiene más de 40 especializaciones que se eligen automáticamente dependiendo de “grouping key” tipos de datos.

### GROUP BY en memoria externa {#select-group-by-in-external-memory}

Puede habilitar el volcado de datos temporales en el disco para restringir el uso de memoria durante `GROUP BY`.
El [max\_bytes\_before\_external\_group\_by](../../../operations/settings/settings.md#settings-max_bytes_before_external_group_by) determina el umbral de consumo de RAM para el dumping `GROUP BY` datos temporales al sistema de archivos. Si se establece en 0 (el valor predeterminado), está deshabilitado.

Cuando se utiliza `max_bytes_before_external_group_by`, le recomendamos que establezca `max_memory_usage` aproximadamente el doble de alto. Esto es necesario porque hay dos etapas para la agregación: leer los datos y formar datos intermedios (1) y fusionar los datos intermedios (2). El volcado de datos al sistema de archivos solo puede ocurrir durante la etapa 1. Si los datos temporales no se volcaron, entonces la etapa 2 puede requerir hasta la misma cantidad de memoria que en la etapa 1.

Por ejemplo, si [Método de codificación de datos:](../../../operations/settings/settings.md#settings_max_memory_usage) se estableció en 10000000000 y desea usar agregación externa, tiene sentido establecer `max_bytes_before_external_group_by` a 10000000000, y `max_memory_usage` a 20000000000. Cuando se activa la agregación externa (si hubo al menos un volcado de datos temporales), el consumo máximo de RAM es solo un poco más que `max_bytes_before_external_group_by`.

Con el procesamiento de consultas distribuidas, la agregación externa se realiza en servidores remotos. Para que el servidor solicitante use solo una pequeña cantidad de RAM, establezca `distributed_aggregation_memory_efficient` a 1.

Al fusionar datos en el disco, así como al fusionar resultados de servidores remotos cuando `distributed_aggregation_memory_efficient` la configuración está habilitada, consume hasta `1/256 * the_number_of_threads` de la cantidad total de RAM.

Cuando la agregación externa está habilitada, si `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

Si usted tiene un [ORDER BY](order-by.md) con un [LIMIT](limit.md) despues `GROUP BY`, entonces la cantidad de RAM usada depende de la cantidad de datos en `LIMIT`, no en toda la tabla. Pero si el `ORDER BY` no tiene `LIMIT`, no se olvide de habilitar la clasificación externa (`max_bytes_before_external_sort`).
