---
machine_translated: true
machine_translated_rev: 0f7ef7704d018700049223525bad4a63911b6e70
toc_priority: 33
toc_title: SELECT
---

# SELECCIONAR consultas Sintaxis {#select-queries-syntax}

`SELECT` realiza la recuperación de datos.

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN (subquery)|table USING columns_list
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

Todas las cláusulas son opcionales, excepto la lista requerida de expresiones inmediatamente después de SELECT.
Las siguientes cláusulas se describen casi en el mismo orden que en el transportador de ejecución de consultas.

Si la consulta omite el `DISTINCT`, `GROUP BY` y `ORDER BY` cláusulas y el `IN` y `JOIN` subconsultas, la consulta se procesará por completo, utilizando O (1) cantidad de RAM.
De lo contrario, la consulta podría consumir mucha RAM si no se especifican las restricciones adecuadas: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. Para obtener más información, consulte la sección “Settings”. Es posible utilizar la clasificación externa (guardar tablas temporales en un disco) y la agregación externa. `The system does not have "merge join"`.

### CON Cláusula {#with-clause}

Esta sección proporciona soporte para expresiones de tabla común ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), con algunas limitaciones:
1. No se admiten consultas recursivas
2. Cuando se usa una subconsulta dentro de la sección WITH, su resultado debe ser escalar con exactamente una fila
3. Los resultados de la expresión no están disponibles en las subconsultas
Los resultados de las expresiones de la cláusula WITH se pueden usar dentro de la cláusula SELECT.

Ejemplo 1: Usar expresión constante como “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

Ejemplo 2: Evictar el resultado de la expresión de sum(bytes) de la lista de columnas de la cláusula SELECT

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

Ejemplo 3: Uso de los resultados de la subconsulta escalar

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

Ejemplo 4: Reutilización de la expresión en subconsulta
Como solución alternativa para la limitación actual para el uso de expresiones en subconsultas, puede duplicarla.

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```

### Cláusula FROM {#select-from}

Si se omite la cláusula FROM, los datos se leerán desde el `system.one` tabla.
El `system.one` table contiene exactamente una fila (esta tabla cumple el mismo propósito que la tabla DUAL que se encuentra en otros DBMS).

El `FROM` cláusula especifica la fuente de la que se leen los datos:

-   Tabla
-   Subconsultas
-   [Función de la tabla](../table-functions/index.md#table-functions)

`ARRAY JOIN` y el regular `JOIN` también se pueden incluir (ver más abajo).

En lugar de una tabla, el `SELECT` subconsulta se puede especificar entre paréntesis.
A diferencia del SQL estándar, no es necesario especificar un sinónimo después de una subconsulta.

Para ejecutar una consulta, todas las columnas enumeradas en la consulta se extraen de la tabla adecuada. Las columnas no necesarias para la consulta externa se eliminan de las subconsultas.
Si una consulta no muestra ninguna columnas (por ejemplo, `SELECT count() FROM t`), alguna columna se extrae de la tabla de todos modos (se prefiere la más pequeña), para calcular el número de filas.

#### Modificador FINAL {#select-from-final}

Aplicable al seleccionar datos de tablas del [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md)-Familia de motores distintos de `GraphiteMergeTree`. Cuando `FINAL` se especifica, ClickHouse fusiona completamente los datos antes de devolver el resultado y, por lo tanto, realiza todas las transformaciones de datos que ocurren durante las fusiones para el motor de tabla dado.

También soportado para:
- [Replicado](../../engines/table-engines/mergetree-family/replication.md) versiones de `MergeTree` motor.
- [Vista](../../engines/table-engines/special/view.md), [Búfer](../../engines/table-engines/special/buffer.md), [Distribuido](../../engines/table-engines/special/distributed.md), y [Método de codificación de datos:](../../engines/table-engines/special/materializedview.md) motores que funcionan sobre otros motores, siempre que se hayan creado sobre `MergeTree`-mesas de motor.

Consultas que usan `FINAL` se ejecutan no tan rápido como consultas similares que no lo hacen, porque:

-   La consulta se ejecuta en un solo subproceso y los datos se combinan durante la ejecución de la consulta.
-   Consultas con `FINAL` leer columnas de clave primaria además de las columnas especificadas en la consulta.

En la mayoría de los casos, evite usar `FINAL`.

### Cláusula SAMPLE {#select-sample-clause}

El `SAMPLE` cláusula permite un procesamiento de consultas aproximado.

Cuando se habilita el muestreo de datos, la consulta no se realiza en todos los datos, sino solo en una cierta fracción de datos (muestra). Por ejemplo, si necesita calcular estadísticas para todas las visitas, es suficiente ejecutar la consulta en la fracción 1/10 de todas las visitas y luego multiplicar el resultado por 10.

El procesamiento de consultas aproximado puede ser útil en los siguientes casos:

-   Cuando tiene requisitos de temporización estrictos (como \<100 ms) pero no puede justificar el costo de recursos de hardware adicionales para cumplirlos.
-   Cuando sus datos sin procesar no son precisos, la aproximación no degrada notablemente la calidad.
-   Los requisitos comerciales se centran en los resultados aproximados (por rentabilidad o para comercializar los resultados exactos a los usuarios premium).

!!! note "Nota"
    Sólo puede utilizar el muestreo con las tablas en el [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md) familia, y sólo si la expresión de muestreo se especificó durante la creación de la tabla (ver [Motor MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).

Las características del muestreo de datos se enumeran a continuación:

-   El muestreo de datos es un mecanismo determinista. El resultado de la misma `SELECT .. SAMPLE` la consulta es siempre la misma.
-   El muestreo funciona consistentemente para diferentes tablas. Para tablas con una sola clave de muestreo, una muestra con el mismo coeficiente siempre selecciona el mismo subconjunto de datos posibles. Por ejemplo, una muestra de ID de usuario toma filas con el mismo subconjunto de todos los ID de usuario posibles de diferentes tablas. Esto significa que puede utilizar el ejemplo en subconsultas [IN](#select-in-operators) clausula. Además, puede unir muestras usando el [JOIN](#select-join) clausula.
-   El muestreo permite leer menos datos de un disco. Tenga en cuenta que debe especificar la clave de muestreo correctamente. Para obtener más información, consulte [Creación de una tabla MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

Para el `SAMPLE` cláusula se admite la siguiente sintaxis:

| SAMPLE Clause Syntax | Descripci                                                                                                                                                                                                                                                                     |
|----------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | Aqui `k` es el número de 0 a 1.</br>La consulta se ejecuta en `k` de datos. Por ejemplo, `SAMPLE 0.1` ejecuta la consulta en el 10% de los datos. [Leer más](#select-sample-k)                                                                                                |
| `SAMPLE n`           | Aqui `n` es un entero suficientemente grande.</br>La consulta se ejecuta en una muestra de al menos `n` filas (pero no significativamente más que esto). Por ejemplo, `SAMPLE 10000000` ejecuta la consulta en un mínimo de 10.000.000 de filas. [Leer más](#select-sample-n) |
| `SAMPLE k OFFSET m`  | Aqui `k` y `m` son los números del 0 al 1.</br>La consulta se ejecuta en una muestra de `k` de los datos. Los datos utilizados para el ejemplo se compensan por `m` fracción. [Leer más](#select-sample-offset)                                                               |

#### SAMPLE K {#select-sample-k}

Aqui `k` es el número de 0 a 1 (se admiten notaciones fraccionarias y decimales). Por ejemplo, `SAMPLE 1/2` o `SAMPLE 0.5`.

En un `SAMPLE k` cláusula, la muestra se toma de la `k` de datos. El ejemplo se muestra a continuación:

``` sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

En este ejemplo, la consulta se ejecuta en una muestra de 0,1 (10%) de datos. Los valores de las funciones agregadas no se corrigen automáticamente, por lo que para obtener un resultado aproximado, el valor `count()` se multiplica manualmente por 10.

#### SAMPLE N {#select-sample-n}

Aqui `n` es un entero suficientemente grande. Por ejemplo, `SAMPLE 10000000`.

En este caso, la consulta se ejecuta en una muestra de al menos `n` filas (pero no significativamente más que esto). Por ejemplo, `SAMPLE 10000000` ejecuta la consulta en un mínimo de 10.000.000 de filas.

Dado que la unidad mínima para la lectura de datos es un gránulo (su tamaño se establece mediante el `index_granularity` ajuste), tiene sentido establecer una muestra que es mucho más grande que el tamaño del gránulo.

Cuando se utiliza el `SAMPLE n` cláusula, no sabe qué porcentaje relativo de datos se procesó. Por lo tanto, no sabe el coeficiente por el que se deben multiplicar las funciones agregadas. Utilice el `_sample_factor` columna virtual para obtener el resultado aproximado.

El `_sample_factor` columna contiene coeficientes relativos que se calculan dinámicamente. Esta columna se crea automáticamente cuando [crear](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) una tabla con la clave de muestreo especificada. Los ejemplos de uso del `_sample_factor` columna se muestran a continuación.

Consideremos la tabla `visits`, que contiene las estadísticas sobre las visitas al sitio. El primer ejemplo muestra cómo calcular el número de páginas vistas:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

El siguiente ejemplo muestra cómo calcular el número total de visitas:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

El siguiente ejemplo muestra cómo calcular la duración media de la sesión. Tenga en cuenta que no necesita usar el coeficiente relativo para calcular los valores promedio.

``` sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

#### SAMPLE K OFFSET M {#select-sample-offset}

Aqui `k` y `m` son números del 0 al 1. Los ejemplos se muestran a continuación.

**Ejemplo 1**

``` sql
SAMPLE 1/10
```

En este ejemplo, la muestra es 1/10 de todos los datos:

`[++------------]`

**Ejemplo 2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

Aquí, se toma una muestra del 10% de la segunda mitad de los datos.

`[------++------]`

### ARRAY JOIN Cláusula {#select-array-join-clause}

Permite ejecutar `JOIN` con una matriz o estructura de datos anidada. La intención es similar a la [arrayJoin](../functions/array-join.md#functions_arrayjoin) función, pero su funcionalidad es más amplia.

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

Sólo puede especificar una sola `ARRAY JOIN` cláusula en una consulta.

El orden de ejecución de la consulta se optimiza cuando se ejecuta `ARRAY JOIN`. Aunque `ARRAY JOIN` debe especificarse siempre antes de la `WHERE/PREWHERE` cláusula, se puede realizar ya sea antes `WHERE/PREWHERE` (si el resultado es necesario en esta cláusula), o después de completarlo (para reducir el volumen de cálculos). El optimizador de consultas controla el orden de procesamiento.

Tipos admitidos de `ARRAY JOIN` se enumeran a continuación:

-   `ARRAY JOIN` - En este caso, las matrices vacías no se incluyen en el resultado de `JOIN`.
-   `LEFT ARRAY JOIN` - El resultado de `JOIN` contiene filas con matrices vacías. El valor de una matriz vacía se establece en el valor predeterminado para el tipo de elemento de matriz (normalmente 0, cadena vacía o NULL).

Los siguientes ejemplos demuestran el uso de la `ARRAY JOIN` y `LEFT ARRAY JOIN` clausula. Vamos a crear una tabla con un [Matriz](../../sql-reference/data-types/array.md) escriba la columna e inserte valores en ella:

``` sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

``` text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

El siguiente ejemplo utiliza el `ARRAY JOIN` clausula:

``` sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

``` text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

El siguiente ejemplo utiliza el `LEFT ARRAY JOIN` clausula:

``` sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

``` text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

#### Uso de alias {#using-aliases}

Se puede especificar un alias para una matriz en el `ARRAY JOIN` clausula. En este caso, este alias puede acceder a un elemento de matriz, pero el nombre original tiene acceso a la matriz en sí. Ejemplo:

``` sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

``` text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

Usando alias, puede realizar `ARRAY JOIN` con una matriz externa. Por ejemplo:

``` sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

``` text
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

Múltiples matrices se pueden separar por comas en el `ARRAY JOIN` clausula. En este caso, `JOIN` se realiza con ellos simultáneamente (la suma directa, no el producto cartesiano). Tenga en cuenta que todas las matrices deben tener el mismo tamaño. Ejemplo:

``` sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

El siguiente ejemplo utiliza el [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) función:

``` sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

#### ARRAY JOIN con estructura de datos anidada {#array-join-with-nested-data-structure}

`ARRAY`JOIN\`\` también funciona con [estructuras de datos anidados](../../sql-reference/data-types/nested-data-structures/nested.md). Ejemplo:

``` sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

``` text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Al especificar nombres de estructuras de datos anidadas en `ARRAY JOIN` el significado es el mismo `ARRAY JOIN` con todos los elementos de la matriz en los que consiste. Los ejemplos se enumeran a continuación:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

Esta variación también tiene sentido:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

Se puede usar un alias para una estructura de datos anidada, con el fin de seleccionar `JOIN` resultado o la matriz de origen. Ejemplo:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

Ejemplo de uso del [arrayEnumerate](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) función:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

### Cláusula JOIN {#select-join}

Se une a los datos en el [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) sentido.

!!! info "Nota"
    No relacionado con [ARRAY JOIN](#select-array-join-clause).

``` sql
SELECT <expr_list>
FROM <left_subquery>
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN <right_subquery>
(ON <expr_list>)|(USING <column_list>) ...
```

Los nombres de tabla se pueden especificar en lugar de `<left_subquery>` y `<right_subquery>`. Esto es equivalente a la `SELECT * FROM table` subconsulta, excepto en un caso especial cuando la tabla tiene [Unir](../../engines/table-engines/special/join.md) engine – an array prepared for joining.

#### Tipos admitidos de `JOIN` {#select-join-types}

-   `INNER JOIN` (o `JOIN`)
-   `LEFT JOIN` (o `LEFT OUTER JOIN`)
-   `RIGHT JOIN` (o `RIGHT OUTER JOIN`)
-   `FULL JOIN` (o `FULL OUTER JOIN`)
-   `CROSS JOIN` (o `,` )

Ver el estándar [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) descripci.

#### ÚNETE Múltiple {#multiple-join}

Al realizar consultas, ClickHouse reescribe las uniones de varias tablas en la secuencia de uniones de dos tablas. Por ejemplo, si hay cuatro tablas para unir ClickHouse une la primera y la segunda, luego une el resultado con la tercera tabla, y en el último paso, se une a la cuarta.

Si una consulta contiene el `WHERE` cláusula, ClickHouse intenta empujar hacia abajo los filtros de esta cláusula a través de la unión intermedia. Si no puede aplicar el filtro a cada unión intermedia, ClickHouse aplica los filtros después de que se completen todas las combinaciones.

Recomendamos el `JOIN ON` o `JOIN USING` sintaxis para crear consultas. Por ejemplo:

``` sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a = t3.a
```

Puede utilizar listas de tablas separadas por comas `FROM` clausula. Por ejemplo:

``` sql
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a
```

No mezcle estas sintaxis.

ClickHouse no admite directamente la sintaxis con comas, por lo que no recomendamos usarlas. El algoritmo intenta reescribir la consulta en términos de `CROSS JOIN` y `INNER JOIN` y luego procede al procesamiento de consultas. Al reescribir la consulta, ClickHouse intenta optimizar el rendimiento y el consumo de memoria. De forma predeterminada, ClickHouse trata las comas como `INNER JOIN` cláusula y convierte `INNER JOIN` a `CROSS JOIN` cuando el algoritmo no puede garantizar que `INNER JOIN` devuelve los datos requeridos.

#### Rigor {#select-join-strictness}

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Producto cartesiano](https://en.wikipedia.org/wiki/Cartesian_product) de filas coincidentes. Este es el estándar `JOIN` comportamiento en SQL.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of queries with `ANY` y `ALL` palabras clave son las mismas.
-   `ASOF` — For joining sequences with a non-exact match. `ASOF JOIN` el uso se describe a continuación.

**ASOF JOIN Uso**

`ASOF JOIN` es útil cuando necesita unir registros que no tienen una coincidencia exacta.

Tablas para `ASOF JOIN` debe tener una columna de secuencia ordenada. Esta columna no puede estar sola en una tabla y debe ser uno de los tipos de datos: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date`, y `DateTime`.

Sintaxis `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

Puede usar cualquier número de condiciones de igualdad y exactamente una condición de coincidencia más cercana. Por ejemplo, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Condiciones admitidas para la coincidencia más cercana: `>`, `>=`, `<`, `<=`.

Sintaxis `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` utilizar `equi_columnX` para unirse a la igualdad y `asof_column` para unirse en el partido más cercano con el `table_1.asof_column >= table_2.asof_column` condición. El `asof_column` columna siempre el último en el `USING` clausula.

Por ejemplo, considere las siguientes tablas:

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN` puede tomar la marca de tiempo de un evento de usuario de `table_1` y encontrar un evento en `table_2` donde la marca de tiempo es la más cercana a la marca de tiempo del evento `table_1` correspondiente a la condición de coincidencia más cercana. Los valores de marca de tiempo iguales son los más cercanos si están disponibles. Aquí, el `user_id` se puede utilizar para unirse a la igualdad y el `ev_time` columna se puede utilizar para unirse en el partido más cercano. En nuestro ejemplo, `event_1_1` se puede unir con `event_2_1` y `event_1_2` se puede unir con `event_2_3`, pero `event_2_2` no se puede unir.

!!! note "Nota"
    `ASOF` unirse es **ni** apoyado en el [Unir](../../engines/table-engines/special/join.md) motor de mesa.

Para establecer el valor de rigor predeterminado, utilice el parámetro de configuración de sesión [Por favor, introduzca su dirección de correo electrónico](../../operations/settings/settings.md#settings-join_default_strictness).

#### GLOBAL JOIN {#global-join}

Cuando se utiliza una normal `JOIN`, la consulta se envía a servidores remotos. Las subconsultas se ejecutan en cada una de ellas para crear la tabla correcta, y la unión se realiza con esta tabla. En otras palabras, la tabla correcta se forma en cada servidor por separado.

Cuando se utiliza `GLOBAL ... JOIN`, primero el servidor requestor ejecuta una subconsulta para calcular la tabla correcta. Esta tabla temporal se pasa a cada servidor remoto y las consultas se ejecutan en ellos utilizando los datos temporales que se transmitieron.

Tenga cuidado al usar `GLOBAL`. Para obtener más información, consulte la sección [Subconsultas distribuidas](#select-distributed-subqueries).

#### Recomendaciones de uso {#usage-recommendations}

Cuando se ejecuta un `JOIN`, no hay optimización del orden de ejecución en relación con otras etapas de la consulta. La combinación (una búsqueda en la tabla de la derecha) se ejecuta antes de filtrar `WHERE` y antes de la agregación. Para establecer explícitamente el orden de procesamiento, recomendamos ejecutar un `JOIN` subconsulta con una subconsulta.

Ejemplo:

``` sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

``` text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

Las subconsultas no permiten establecer nombres ni usarlos para hacer referencia a una columna de una subconsulta específica.
Las columnas especificadas en `USING` debe tener los mismos nombres en ambas subconsultas, y las otras columnas deben tener un nombre diferente. Puede usar alias para cambiar los nombres de las columnas en subconsultas (el ejemplo usa los alias `hits` y `visits`).

El `USING` clause especifica una o más columnas a unir, lo que establece la igualdad de estas columnas. La lista de columnas se establece sin corchetes. No se admiten condiciones de unión más complejas.

La tabla correcta (el resultado de la subconsulta) reside en la RAM. Si no hay suficiente memoria, no puede ejecutar una `JOIN`.

Cada vez que se ejecuta una consulta `JOIN`, la subconsulta se ejecuta de nuevo porque el resultado no se almacena en caché. Para evitar esto, use el especial [Unir](../../engines/table-engines/special/join.md) motor de tabla, que es una matriz preparada para unirse que siempre está en RAM.

En algunos casos, es más eficiente de usar `IN` en lugar de `JOIN`.
Entre los diversos tipos de `JOIN` el más eficiente es `ANY LEFT JOIN`, entonces `ANY INNER JOIN`. Los menos eficientes son `ALL LEFT JOIN` y `ALL INNER JOIN`.

Si necesita un `JOIN` para unirse a tablas de dimensión (son tablas relativamente pequeñas que contienen propiedades de dimensión, como nombres para campañas publicitarias), un `JOIN` podría no ser muy conveniente debido al hecho de que se vuelve a acceder a la tabla correcta para cada consulta. Para tales casos, hay un “external dictionaries” característica que debe utilizar en lugar de `JOIN`. Para obtener más información, consulte la sección [Diccionarios externos](../dictionaries/external-dictionaries/external-dicts.md).

**Limitaciones de memoria**

ClickHouse utiliza el [hash unirse](https://en.wikipedia.org/wiki/Hash_join) algoritmo. ClickHouse toma el `<right_subquery>` y crea una tabla hash para ello en RAM. Si necesita restringir el consumo de memoria de la operación de unión, use la siguiente configuración:

-   [Método de codificación de datos:](../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [Método de codificación de datos:](../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

Cuando se alcanza cualquiera de estos límites, ClickHouse actúa como el [join\_overflow\_mode](../../operations/settings/query-complexity.md#settings-join_overflow_mode) configuración instruye.

#### Procesamiento de celdas vacías o NULL {#processing-of-empty-or-null-cells}

Al unir tablas, pueden aparecer las celdas vacías. Configuración [Sistema abierto.](../../operations/settings/settings.md#join_use_nulls) definir cómo ClickHouse llena estas celdas.

Si el `JOIN` las llaves son [NULL](../data-types/nullable.md) campos, las filas donde al menos una de las claves tiene el valor [NULL](../syntax.md#null-literal) no se unen.

#### Limitaciones de sintaxis {#syntax-limitations}

Para múltiples `JOIN` cláusulas en una sola `SELECT` consulta:

-   Tomando todas las columnas a través de `*` está disponible solo si se unen tablas, no subconsultas.
-   El `PREWHERE` cláusula no está disponible.

Para `ON`, `WHERE`, y `GROUP BY` clausula:

-   Las expresiones arbitrarias no se pueden utilizar en `ON`, `WHERE`, y `GROUP BY` cláusulas, pero puede definir una expresión en un `SELECT` cláusula y luego usarla en estas cláusulas a través de un alias.

### DONDE Cláusula {#select-where}

Si hay una cláusula where, debe contener una expresión con el tipo UInt8. Esta suele ser una expresión con comparación y operadores lógicos.
Esta expresión se usará para filtrar datos antes de todas las demás transformaciones.

Si los índices son compatibles con el motor de tablas de base de datos, la expresión se evalúa en función de la capacidad de usar índices.

### PREWHERE Cláusula {#prewhere-clause}

Esta cláusula tiene el mismo significado que la cláusula where. La diferencia radica en qué datos se leen de la tabla.
Al usar PREWHERE, primero solo se leen las columnas necesarias para ejecutar PREWHERE. Luego se leen las otras columnas que son necesarias para ejecutar la consulta, pero solo aquellos bloques donde la expresión PREWHERE es verdadera.

Tiene sentido usar PREWHERE si hay condiciones de filtración utilizadas por una minoría de las columnas de la consulta, pero que proporcionan una filtración de datos fuerte. Esto reduce el volumen de datos a leer.

Por ejemplo, es útil escribir PREWHERE para consultas que extraen un gran número de columnas, pero que solo tienen filtración para unas pocas columnas.

PREWHERE solo es compatible con tablas de la `*MergeTree` familia.

Una consulta puede especificar simultáneamente PREWHERE y WHERE. En este caso, PREWHERE precede WHERE.

Si el ‘optimize\_move\_to\_prewhere’ La configuración se establece en 1 y PREWHERE se omite, el sistema utiliza la heurística para mover automáticamente partes de expresiones de WHERE a PREWHERE.

### GRUPO POR Cláusula {#select-group-by-clause}

Esta es una de las partes más importantes de un DBMS orientado a columnas.

Si hay una cláusula GROUP BY, debe contener una lista de expresiones. Cada expresión se mencionará aquí como una “key”.
Todas las expresiones de las cláusulas SELECT, HAVING y ORDER BY deben calcularse a partir de claves o de funciones agregadas. En otras palabras, cada columna seleccionada de la tabla debe usarse en claves o dentro de funciones agregadas.

Si una consulta solo contiene columnas de tabla dentro de funciones agregadas, se puede omitir la cláusula GROUP BY y se asume la agregación mediante un conjunto vacío de claves.

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

#### Procesamiento NULL {#null-processing}

Para agrupar, ClickHouse interpreta [NULL](../syntax.md) como valor, y `NULL=NULL`.

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

#### CON TOTALS Modificador {#with-totals-modifier}

Si se especifica el modificador WITH TOTALS, se calculará otra fila. Esta fila tendrá columnas clave que contienen valores predeterminados (zeros o líneas vacías) y columnas de funciones agregadas con los valores calculados en todas las filas (el “total” valor).

Esta fila adicional se genera en formatos JSON \*, TabSeparated \* y Pretty \*, por separado de las otras filas. En los otros formatos, esta fila no se genera.

En los formatos JSON\*, esta fila se muestra como una ‘totals’ campo. En los formatos TabSeparated\*, la fila viene después del resultado principal, precedida por una fila vacía (después de los otros datos). En los formatos Pretty\*, la fila se muestra como una tabla separada después del resultado principal.

`WITH TOTALS` se puede ejecutar de diferentes maneras cuando HAVING está presente. El comportamiento depende de la ‘totals\_mode’ configuración.
Predeterminada, `totals_mode = 'before_having'`. En este caso, ‘totals’ se calcula en todas las filas, incluidas las que no pasan por HAVING y ‘max\_rows\_to\_group\_by’.

Las otras alternativas incluyen solo las filas que pasan por HAVING en ‘totals’, y comportarse de manera diferente con el ajuste `max_rows_to_group_by` y `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. En otras palabras, ‘totals’ tendrá menos o el mismo número de filas que si `max_rows_to_group_by` se omitieron.

`after_having_inclusive` – Include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ en ‘totals’. En otras palabras, ‘totals’ tendrá más o el mismo número de filas como lo haría si `max_rows_to_group_by` se omitieron.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ en ‘totals’. De lo contrario, no los incluya.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

Si `max_rows_to_group_by` y `group_by_overflow_mode = 'any'` no se utilizan, todas las variaciones de `after_having` son los mismos, y se puede utilizar cualquiera de ellos (por ejemplo, `after_having_auto`).

Puede usar WITH TOTALS en subconsultas, incluidas las subconsultas en la cláusula JOIN (en este caso, se combinan los valores totales respectivos).

#### GROUP BY en memoria externa {#select-group-by-in-external-memory}

Puede habilitar el volcado de datos temporales en el disco para restringir el uso de memoria durante `GROUP BY`.
El [max\_bytes\_before\_external\_group\_by](../../operations/settings/settings.md#settings-max_bytes_before_external_group_by) determina el umbral de consumo de RAM para el dumping `GROUP BY` datos temporales al sistema de archivos. Si se establece en 0 (el valor predeterminado), está deshabilitado.

Cuando se utiliza `max_bytes_before_external_group_by`, le recomendamos que establezca `max_memory_usage` aproximadamente el doble de alto. Esto es necesario porque hay dos etapas para la agregación: leer la fecha y formar datos intermedios (1) y fusionar los datos intermedios (2). El volcado de datos al sistema de archivos solo puede ocurrir durante la etapa 1. Si los datos temporales no se volcaron, entonces la etapa 2 puede requerir hasta la misma cantidad de memoria que en la etapa 1.

Por ejemplo, si [Método de codificación de datos:](../../operations/settings/settings.md#settings_max_memory_usage) se estableció en 10000000000 y desea usar agregación externa, tiene sentido establecer `max_bytes_before_external_group_by` a 10000000000, y max\_memory\_usage a 20000000000. Cuando se activa la agregación externa (si hubo al menos un volcado de datos temporales), el consumo máximo de RAM es solo un poco más que `max_bytes_before_external_group_by`.

Con el procesamiento de consultas distribuidas, la agregación externa se realiza en servidores remotos. Para que el servidor solicitante use solo una pequeña cantidad de RAM, establezca `distributed_aggregation_memory_efficient` a 1.

Al fusionar datos en el disco, así como al fusionar resultados de servidores remotos cuando `distributed_aggregation_memory_efficient` la configuración está habilitada, consume hasta `1/256 * the_number_of_threads` de la cantidad total de RAM.

Cuando la agregación externa está habilitada, si `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

Si usted tiene un `ORDER BY` con un `LIMIT` despues `GROUP BY`, entonces la cantidad de RAM usada depende de la cantidad de datos en `LIMIT`, no en toda la tabla. Pero si el `ORDER BY` no tiene `LIMIT`, no se olvide de habilitar la clasificación externa (`max_bytes_before_external_sort`).

### LIMITAR POR Cláusula {#limit-by-clause}

Una consulta con el `LIMIT n BY expressions` cláusula selecciona la primera `n` para cada valor distinto de `expressions`. La clave para `LIMIT BY` puede contener cualquier número de [expresiones](../syntax.md#syntax-expressions).

ClickHouse admite la siguiente sintaxis:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

Durante el procesamiento de consultas, ClickHouse selecciona los datos ordenados por clave de ordenación. La clave de ordenación se establece explícitamente utilizando un [ORDER BY](#select-order-by) cláusula o implícitamente como una propiedad del motor de tablas. Entonces se aplica ClickHouse `LIMIT n BY expressions` y devuelve la primera `n` filas para cada combinación distinta de `expressions`. Si `OFFSET` se especifica, a continuación, para cada bloque de datos que pertenece a una combinación distinta de `expressions`, ClickHouse salta `offset_value` número de filas desde el principio del bloque y devuelve un máximo de `n` filas como resultado. Si `offset_value` es mayor que el número de filas en el bloque de datos, ClickHouse devuelve cero filas del bloque.

`LIMIT BY` no está relacionado con `LIMIT`. Ambos se pueden usar en la misma consulta.

**Ejemplos**

Tabla de muestra:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by values(1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Consulta:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

El `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` query devuelve el mismo resultado.

La siguiente consulta devuelve las 5 referencias principales para cada `domain, device_type` par con un máximo de 100 filas en total (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

### Cláusula HAVING {#having-clause}

Permite filtrar el resultado recibido después de GROUP BY, similar a la cláusula WHERE.
WHERE y HAVING difieren en que WHERE se realiza antes de la agregación (GROUP BY), mientras que HAVING se realiza después de ella.
Si no se realiza la agregación, no se puede usar HAVING.

### ORDEN POR CLÁUSULA {#select-order-by}

La cláusula ORDER BY contiene una lista de expresiones, a las que se puede asignar DESC o ASC (la dirección de clasificación). Si no se especifica la dirección, se supone ASC. ASC se ordena en orden ascendente y DESC en orden descendente. La dirección de ordenación se aplica a una sola expresión, no a toda la lista. Ejemplo: `ORDER BY Visits DESC, SearchPhrase`

Para ordenar por valores de cadena, puede especificar la intercalación (comparación). Ejemplo: `ORDER BY SearchPhrase COLLATE 'tr'` - para ordenar por palabra clave en orden ascendente, utilizando el alfabeto turco, insensible a mayúsculas y minúsculas, suponiendo que las cadenas están codificadas en UTF-8. COLLATE se puede especificar o no para cada expresión en ORDER BY de forma independiente. Si se especifica ASC o DESC, se especifica COLLATE después de él. Cuando se usa COLLATE, la clasificación siempre distingue entre mayúsculas y minúsculas.

Solo recomendamos usar COLLATE para la clasificación final de un pequeño número de filas, ya que la clasificación con COLLATE es menos eficiente que la clasificación normal por bytes.

Las filas que tienen valores idénticos para la lista de expresiones de clasificación se generan en un orden arbitrario, que también puede ser no determinista (diferente cada vez).
Si se omite la cláusula ORDER BY, el orden de las filas tampoco está definido y también puede ser no determinista.

`NaN` y `NULL` orden de clasificación:

-   Con el modificador `NULLS FIRST` — First `NULL`, entonces `NaN`, luego otros valores.
-   Con el modificador `NULLS LAST` — First the values, then `NaN`, entonces `NULL`.
-   Default — The same as with the `NULLS LAST` modificador.

Ejemplo:

Para la mesa

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Ejecute la consulta `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` conseguir:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Cuando se ordenan los números de coma flotante, los NaN están separados de los otros valores. Independientemente del orden de clasificación, los NaN vienen al final. En otras palabras, para la clasificación ascendente se colocan como si fueran más grandes que todos los demás números, mientras que para la clasificación descendente se colocan como si fueran más pequeños que el resto.

Se usa menos RAM si se especifica un LIMIT lo suficientemente pequeño además de ORDER BY. De lo contrario, la cantidad de memoria gastada es proporcional al volumen de datos para clasificar. Para el procesamiento de consultas distribuidas, si se omite GROUP BY, la ordenación se realiza parcialmente en servidores remotos y los resultados se combinan en el servidor solicitante. Esto significa que para la ordenación distribuida, el volumen de datos a ordenar puede ser mayor que la cantidad de memoria en un único servidor.

Si no hay suficiente RAM, es posible realizar la clasificación en la memoria externa (creando archivos temporales en un disco). Utilice el ajuste `max_bytes_before_external_sort` para este propósito. Si se establece en 0 (el valor predeterminado), la ordenación externa está deshabilitada. Si está habilitada, cuando el volumen de datos a ordenar alcanza el número especificado de bytes, los datos recopilados se ordenan y se vuelcan en un archivo temporal. Después de leer todos los datos, todos los archivos ordenados se fusionan y se generan los resultados. Los archivos se escriben en el directorio /var/lib/clickhouse/tmp/ en la configuración (de forma predeterminada, pero puede ‘tmp\_path’ parámetro para cambiar esta configuración).

La ejecución de una consulta puede usar más memoria que ‘max\_bytes\_before\_external\_sort’. Por este motivo, esta configuración debe tener un valor significativamente menor que ‘max\_memory\_usage’. Como ejemplo, si su servidor tiene 128 GB de RAM y necesita ejecutar una sola consulta, establezca ‘max\_memory\_usage’ de hasta 100 GB, y ‘max\_bytes\_before\_external\_sort’ para 80 GB.

La clasificación externa funciona con mucha menos eficacia que la clasificación en RAM.

### SELECT Cláusula {#select-select}

[Expresiones](../syntax.md#syntax-expressions) especificado en el `SELECT` cláusula se calculan después de que todas las operaciones en las cláusulas descritas anteriormente hayan finalizado. Estas expresiones funcionan como si se aplicaran a filas separadas en el resultado. Si las expresiones en el `SELECT` cláusula contiene funciones agregadas, a continuación, ClickHouse procesa funciones agregadas y expresiones utilizadas como sus argumentos durante el [GROUP BY](#select-group-by-clause) agregación.

Si desea incluir todas las columnas en el resultado, use el asterisco (`*`) simbolo. Por ejemplo, `SELECT * FROM ...`.

Para hacer coincidir algunas columnas en el resultado con un [Re2](https://en.wikipedia.org/wiki/RE2_(software)) expresión regular, puede utilizar el `COLUMNS` expresion.

``` sql
COLUMNS('regexp')
```

Por ejemplo, considere la tabla:

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

La siguiente consulta selecciona datos de todas las columnas que contienen `a` símbolo en su nombre.

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Las columnas seleccionadas no se devuelven en orden alfabético.

Puede utilizar múltiples `COLUMNS` expresiones en una consulta y aplicarles funciones.

Por ejemplo:

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Cada columna devuelta por el `COLUMNS` expresión se pasa a la función como un argumento separado. También puede pasar otros argumentos a la función si los admite. Tenga cuidado al usar funciones. Si una función no admite la cantidad de argumentos que le ha pasado, ClickHouse lanza una excepción.

Por ejemplo:

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2.
```

En este ejemplo, `COLUMNS('a')` devuelve dos columnas: `aa` y `ab`. `COLUMNS('c')` devuelve el `bc` columna. El `+` el operador no puede aplicar a 3 argumentos, por lo que ClickHouse lanza una excepción con el mensaje relevante.

Columnas que coinciden con el `COLUMNS` expresión puede tener diferentes tipos de datos. Si `COLUMNS` no coincide con ninguna columna y es la única expresión en `SELECT`, ClickHouse lanza una excepción.

### Cláusula DISTINCT {#select-distinct}

Si se especifica DISTINCT, sólo quedará una sola fila de todos los conjuntos de filas totalmente coincidentes en el resultado.
El resultado será el mismo que si GROUP BY se especificara en todos los campos especificados en SELECT sin funciones agregadas. Pero hay varias diferencias con GROUP BY:

-   DISTINCT se puede aplicar junto con GROUP BY.
-   Cuando ORDER BY se omite y se define LIMIT, la consulta deja de ejecutarse inmediatamente después de leer el número necesario de filas diferentes.
-   Los bloques de datos se generan a medida que se procesan, sin esperar a que finalice la ejecución de toda la consulta.

DISTINCT no se admite si SELECT tiene al menos una columna de matriz.

`DISTINCT` trabaja con [NULL](../syntax.md) como si `NULL` Era un valor específico, y `NULL=NULL`. En otras palabras, en el `DISTINCT` resultados, diferentes combinaciones con `NULL` sólo ocurren una vez.

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

### Cláusula LIMIT {#limit-clause}

`LIMIT m` permite seleccionar la primera `m` filas del resultado.

`LIMIT n, m` permite seleccionar la primera `m` el resultado después de omitir la primera `n` filas. El `LIMIT m OFFSET n` sintaxis también es compatible.

`n` y `m` deben ser enteros no negativos.

Si no hay una `ORDER BY` cláusula que ordena explícitamente los resultados, el resultado puede ser arbitrario y no determinista.

### UNION ALL Cláusula {#union-all-clause}

Puede utilizar UNION ALL para combinar cualquier número de consultas. Ejemplo:

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

Solo se admite UNION ALL. La UNIÓN regular (UNION DISTINCT) no es compatible. Si necesita UNION DISTINCT, puede escribir SELECT DISTINCT desde una subconsulta que contenga UNION ALL.

Las consultas que forman parte de UNION ALL se pueden ejecutar simultáneamente y sus resultados se pueden mezclar.

La estructura de los resultados (el número y el tipo de columnas) debe coincidir con las consultas. Pero los nombres de columna pueden diferir. En este caso, los nombres de columna para el resultado final se tomarán de la primera consulta. La fundición de tipo se realiza para uniones. Por ejemplo, si dos consultas que se combinan tienen el mismo campo-`Nullable` y `Nullable` tipos de un tipo compatible, el resultado `UNION ALL` tiene una `Nullable` campo de tipo.

Las consultas que forman parte de UNION ALL no se pueden encerrar entre paréntesis. ORDER BY y LIMIT se aplican a consultas separadas, no al resultado final. Si necesita aplicar una conversión al resultado final, puede colocar todas las consultas con UNION ALL en una subconsulta en la cláusula FROM.

### INTO OUTFILE Cláusula {#into-outfile-clause}

Añadir el `INTO OUTFILE filename` cláusula (donde filename es un literal de cadena) para redirigir la salida de la consulta al archivo especificado.
A diferencia de MySQL, el archivo se crea en el lado del cliente. La consulta fallará si ya existe un archivo con el mismo nombre de archivo.
Esta funcionalidad está disponible en el cliente de línea de comandos y clickhouse-local (una consulta enviada a través de la interfaz HTTP fallará).

El formato de salida predeterminado es TabSeparated (el mismo que en el modo de lote de cliente de línea de comandos).

### FORMAT Cláusula {#format-clause}

Especificar ‘FORMAT format’ para obtener datos en cualquier formato especificado.
Puede usar esto por conveniencia o para crear volcados.
Para obtener más información, consulte la sección “Formats”.
Si se omite la cláusula FORMAT, se utiliza el formato predeterminado, que depende tanto de la configuración como de la interfaz utilizada para acceder a la base de datos. Para la interfaz HTTP y el cliente de línea de comandos en modo por lotes, el formato predeterminado es TabSeparated. Para el cliente de línea de comandos en modo interactivo, el formato predeterminado es PrettyCompact (tiene tablas atractivas y compactas).

Cuando se utiliza el cliente de línea de comandos, los datos se pasan al cliente en un formato interno eficiente. El cliente interpreta independientemente la cláusula FORMAT de la consulta y da formato a los datos en sí (aliviando así la red y el servidor de la carga).

### IN Operadores {#select-in-operators}

El `IN`, `NOT IN`, `GLOBAL IN`, y `GLOBAL NOT IN` están cubiertos por separado, ya que su funcionalidad es bastante rica.

El lado izquierdo del operador es una sola columna o una tupla.

Ejemplos:

``` sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

Si el lado izquierdo es una sola columna que está en el índice, y el lado derecho es un conjunto de constantes, el sistema usa el índice para procesar la consulta.

Don't list too many values explicitly (i.e. millions). If a data set is large, put it in a temporary table (for example, see the section “External data for query processing”), luego use una subconsulta.

El lado derecho del operador puede ser un conjunto de expresiones constantes, un conjunto de tuplas con expresiones constantes (mostradas en los ejemplos anteriores) o el nombre de una tabla de base de datos o subconsulta SELECT entre paréntesis.

Si el lado derecho del operador es el nombre de una tabla (por ejemplo, `UserID IN users`), esto es equivalente a la subconsulta `UserID IN (SELECT * FROM users)`. Úselo cuando trabaje con datos externos que se envían junto con la consulta. Por ejemplo, la consulta se puede enviar junto con un conjunto de ID de usuario ‘users’ tabla temporal, que debe ser filtrada.

Si el lado derecho del operador es un nombre de tabla que tiene el motor Set (un conjunto de datos preparado que siempre está en RAM), el conjunto de datos no se volverá a crear para cada consulta.

La subconsulta puede especificar más de una columna para filtrar tuplas.
Ejemplo:

``` sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

Las columnas a la izquierda y a la derecha del operador IN deben tener el mismo tipo.

El operador IN y la subconsulta pueden aparecer en cualquier parte de la consulta, incluidas las funciones agregadas y las funciones lambda.
Ejemplo:

``` sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

``` text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

Para cada día después del 17 de marzo, cuente el porcentaje de páginas vistas realizadas por los usuarios que visitaron el sitio el 17 de marzo.
Una subconsulta en la cláusula IN siempre se ejecuta una sola vez en un único servidor. No hay subconsultas dependientes.

#### Procesamiento NULL {#null-processing-1}

Durante el procesamiento de la solicitud, el operador IN asume que el resultado de una operación [NULL](../syntax.md) siempre es igual a `0`, independientemente de si `NULL` está en el lado derecho o izquierdo del operador. `NULL` Los valores no se incluyen en ningún conjunto de datos, no se corresponden entre sí y no se pueden comparar.

Aquí hay un ejemplo con el `t_null` tabla:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Ejecución de la consulta `SELECT x FROM t_null WHERE y IN (NULL,3)` da el siguiente resultado:

``` text
┌─x─┐
│ 2 │
└───┘
```

Se puede ver que la fila en la que `y = NULL` se expulsa de los resultados de la consulta. Esto se debe a que ClickHouse no puede decidir si `NULL` está incluido en el `(NULL,3)` conjunto, devuelve `0` como resultado de la operación, y `SELECT` excluye esta fila de la salida final.

``` sql
SELECT y IN (NULL, 3)
FROM t_null
```

``` text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

#### Subconsultas distribuidas {#select-distributed-subqueries}

Hay dos opciones para IN-s con subconsultas (similar a JOINs): normal `IN` / `JOIN` y `GLOBAL IN` / `GLOBAL JOIN`. Se diferencian en cómo se ejecutan para el procesamiento de consultas distribuidas.

!!! attention "Atención"
    Recuerde que los algoritmos descritos a continuación pueden funcionar de manera diferente dependiendo de la [configuración](../../operations/settings/settings.md) `distributed_product_mode` configuración.

Cuando se utiliza el IN normal, la consulta se envía a servidores remotos, y cada uno de ellos ejecuta las subconsultas en el `IN` o `JOIN` clausula.

Cuando se utiliza `GLOBAL IN` / `GLOBAL JOINs`, primero todas las subconsultas se ejecutan para `GLOBAL IN` / `GLOBAL JOINs`, y los resultados se recopilan en tablas temporales. A continuación, las tablas temporales se envían a cada servidor remoto, donde las consultas se ejecutan utilizando estos datos temporales.

Para una consulta no distribuida, utilice el `IN` / `JOIN`.

Tenga cuidado al usar subconsultas en el `IN` / `JOIN` para el procesamiento de consultas distribuidas.

Veamos algunos ejemplos. Supongamos que cada servidor del clúster tiene un **local\_table**. Cada servidor también tiene un **distributed\_table** mesa con el **Distribuido** tipo, que mira todos los servidores del clúster.

Para una consulta al **distributed\_table**, la consulta se enviará a todos los servidores remotos y se ejecutará en ellos usando el **local\_table**.

Por ejemplo, la consulta

``` sql
SELECT uniq(UserID) FROM distributed_table
```

se enviará a todos los servidores remotos como

``` sql
SELECT uniq(UserID) FROM local_table
```

y ejecutar en cada uno de ellos en paralelo, hasta que llegue a la etapa donde se pueden combinar resultados intermedios. Luego, los resultados intermedios se devolverán al servidor solicitante y se fusionarán en él, y el resultado final se enviará al cliente.

Ahora examinemos una consulta con IN:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

-   Cálculo de la intersección de audiencias de dos sitios.

Esta consulta se enviará a todos los servidores remotos como

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

En otras palabras, los datos establecidos en la cláusula IN se recopilarán en cada servidor de forma independiente, solo a través de los datos que se almacenan localmente en cada uno de los servidores.

Esto funcionará correctamente y de manera óptima si está preparado para este caso y ha distribuido datos en los servidores de clúster de modo que los datos de un único ID de usuario residen completamente en un único servidor. En este caso, todos los datos necesarios estarán disponibles localmente en cada servidor. De lo contrario, el resultado será inexacto. Nos referimos a esta variación de la consulta como “local IN”.

Para corregir cómo funciona la consulta cuando los datos se distribuyen aleatoriamente entre los servidores de clúster, puede especificar **distributed\_table** dentro de una subconsulta. La consulta se vería así:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Esta consulta se enviará a todos los servidores remotos como

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

La subconsulta comenzará a ejecutarse en cada servidor remoto. Dado que la subconsulta utiliza una tabla distribuida, la subconsulta que se encuentra en cada servidor remoto se reenviará a cada servidor remoto como

``` sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

Por ejemplo, si tiene un clúster de 100 servidores, la ejecución de toda la consulta requerirá 10.000 solicitudes elementales, lo que generalmente se considera inaceptable.

En tales casos, siempre debe usar GLOBAL IN en lugar de IN. Veamos cómo funciona para la consulta

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

El servidor del solicitante ejecutará la subconsulta

``` sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

y el resultado se colocará en una tabla temporal en la RAM. A continuación, la solicitud se enviará a cada servidor remoto como

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

y la tabla temporal `_data1` se enviará a cada servidor remoto con la consulta (el nombre de la tabla temporal está definido por la implementación).

Esto es más óptimo que usar el IN normal. Sin embargo, tenga en cuenta los siguientes puntos:

1.  Al crear una tabla temporal, los datos no se hacen únicos. Para reducir el volumen de datos transmitidos a través de la red, especifique DISTINCT en la subconsulta. (No necesita hacer esto para un IN normal.)
2.  La tabla temporal se enviará a todos los servidores remotos. La transmisión no tiene en cuenta la topología de red. Por ejemplo, si 10 servidores remotos residen en un centro de datos que es muy remoto en relación con el servidor solicitante, los datos se enviarán 10 veces a través del canal al centro de datos remoto. Intente evitar grandes conjuntos de datos cuando use GLOBAL IN.
3.  Al transmitir datos a servidores remotos, las restricciones en el ancho de banda de la red no son configurables. Puede sobrecargar la red.
4.  Intente distribuir datos entre servidores para que no necesite usar GLOBAL IN de forma regular.
5.  Si necesita utilizar GLOBAL IN con frecuencia, planifique la ubicación del clúster ClickHouse para que un único grupo de réplicas resida en no más de un centro de datos con una red rápida entre ellos, de modo que una consulta se pueda procesar completamente dentro de un único centro de datos.

También tiene sentido especificar una tabla local en el `GLOBAL IN` cláusula, en caso de que esta tabla local solo esté disponible en el servidor solicitante y desee usar datos de ella en servidores remotos.

### Valores extremos {#extreme-values}

Además de los resultados, también puede obtener valores mínimos y máximos para las columnas de resultados. Para hacer esto, establezca el **extremo** a 1. Los mínimos y máximos se calculan para tipos numéricos, fechas y fechas con horas. Para otras columnas, se generan los valores predeterminados.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`, y `Pretty*` [formato](../../interfaces/formats.md), separado de las otras filas. No se emiten para otros formatos.

En `JSON*` los valores extremos se emiten en un formato separado. ‘extremes’ campo. En `TabSeparated*` , la fila viene después del resultado principal, y después de ‘totals’ si está presente. Está precedido por una fila vacía (después de los otros datos). En `Pretty*` formatea, la fila se muestra como una tabla separada después del resultado principal, y después de `totals` si está presente.

Los valores extremos se calculan para las filas anteriores `LIMIT`, pero después `LIMIT BY`. Sin embargo, cuando se usa `LIMIT offset, size`, las filas antes `offset` están incluidos en `extremes`. En las solicitudes de secuencia, el resultado también puede incluir un pequeño número de filas que pasaron por `LIMIT`.

### Nota {#notes}

El `GROUP BY` y `ORDER BY` las cláusulas no admiten argumentos posicionales. Esto contradice MySQL, pero se ajusta al SQL estándar.
Por ejemplo, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

Puedes usar sinónimos (`AS` aliases) en cualquier parte de una consulta.

Puede poner un asterisco en cualquier parte de una consulta en lugar de una expresión. Cuando se analiza la consulta, el asterisco se expande a una lista de todas las columnas de la tabla `MATERIALIZED` y `ALIAS` columna). Solo hay unos pocos casos en los que se justifica el uso de un asterisco:

-   Al crear un volcado de tabla.
-   Para tablas que contienen solo unas pocas columnas, como las tablas del sistema.
-   Para obtener información sobre qué columnas están en una tabla. En este caso, establezca `LIMIT 1`. Pero es mejor usar el `DESC TABLE` consulta.
-   Cuando hay una filtración fuerte en un pequeño número de columnas usando `PREWHERE`.
-   En subconsultas (ya que las columnas que no son necesarias para la consulta externa están excluidas de las subconsultas).

En todos los demás casos, no recomendamos usar el asterisco, ya que solo le da los inconvenientes de un DBMS columnar en lugar de las ventajas. En otras palabras, no se recomienda usar el asterisco.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/select/) <!--hide-->
