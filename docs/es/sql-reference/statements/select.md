---
machine_translated: true
machine_translated_rev: 3e185d24c9fe772c7cf03d5475247fb829a21dfa
toc_priority: 33
toc_title: SELECT
---

# SELECCIONAR Consultas Sintaxis {#select-queries-syntax}

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

Permite ejecutar `JOIN` con una matriz o estructura de datos anidada. La intención es similar a la [arrayJoin](../../sql-reference/functions/array-join.md#functions_arrayjoin) función, pero su funcionalidad es más amplia.

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

#### Uso De Alias {#using-aliases}

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

#### ARRAY JOIN Con Estructura De Datos Anidada {#array-join-with-nested-data-structure}

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

#### Tipos Compatibles De `JOIN` {#select-join-types}

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

\`\`\` texto
table\_1 table\_2

evento \| ev\_time \| user\_id evento \| ev\_time \| user\_id
