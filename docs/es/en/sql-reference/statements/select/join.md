---
description: 'Documentación de la cláusula JOIN'
sidebar_label: 'JOIN'
slug: /sql-reference/statements/select/join
title: 'Cláusula JOIN'
keywords: ['INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', 'RIGHT JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN', 'LEFT SEMI JOIN', 'RIGHT SEMI JOIN', 'LEFT ANTI JOIN', 'RIGHT ANTI JOIN', 'LEFT ANY JOIN', 'RIGHT ANY JOIN', 'INNER ANY JOIN', 'ASOF JOIN', 'LEFT ASOF JOIN', 'PASTE JOIN', 'NATURAL JOIN']
doc_type: 'reference'
---

La cláusula `JOIN` genera una nueva tabla al combinar columnas de una o varias tablas mediante valores comunes entre ellas. Es una operación habitual en bases de datos compatibles con SQL y se corresponde con la operación [join del álgebra relacional](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators). El caso especial de unir una tabla consigo misma suele denominarse &quot;autounión&quot;.

**Sintaxis**

```sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

Las expresiones de la cláusula `ON` y las columnas de la cláusula `USING` se llaman &quot;clave de JOIN&quot;. A menos que se indique lo contrario, un `JOIN` produce un [producto cartesiano](https://en.wikipedia.org/wiki/Cartesian_product) de las filas con &quot;clave de JOIN&quot; coincidentes, lo que puede generar resultados con muchas más filas que las tablas de origen.

<div id="supported-types-of-join">
  ## Tipos de JOIN compatibles
</div>

Se admiten todos los tipos estándar de [SQL JOIN](https://en.wikipedia.org/wiki/Join_\(SQL\)):

| Tipo               | Descripción                                                                                                                                                                                                                                                                                                         |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `INNER JOIN`       | solo se devuelven las filas coincidentes.                                                                                                                                                                                                                                                                           |
| `LEFT OUTER JOIN`  | se devuelven las filas no coincidentes de la tabla izquierda, además de las filas coincidentes.                                                                                                                                                                                                                     |
| `RIGHT OUTER JOIN` | se devuelven las filas no coincidentes de la tabla derecha, además de las filas coincidentes.                                                                                                                                                                                                                       |
| `FULL OUTER JOIN`  | se devuelven las filas no coincidentes de ambas tablas, además de las filas coincidentes.                                                                                                                                                                                                                           |
| `CROSS JOIN`       | produce el producto cartesiano de tablas completas; **no** se especifican &quot;clave de JOIN&quot;.                                                                                                                                                                                                                |
| `NATURAL JOIN`     | une automáticamente todas las columnas con el mismo nombre en ambas tablas; cada columna común aparece una sola vez en el resultado. Admite las variantes `INNER` (predeterminada), `LEFT`, `RIGHT` y `FULL`. Equivale a `JOIN ... USING (col1, col2, ...)`, donde la lista de columnas se obtiene automáticamente. |

* `JOIN` sin un tipo especificado implica `INNER`.
* La palabra clave `OUTER` puede omitirse sin problema.
* Una sintaxis alternativa para `CROSS JOIN` es especificar varias tablas en la cláusula [`FROM`](../../../sql-reference/statements/select/from.md), separadas por comas.
* Si no hay columnas coincidentes para un `NATURAL JOIN`, funciona como un `CROSS JOIN`.

Los tipos adicionales de JOIN disponibles en ClickHouse son:

| Tipo                                                | Descripción                                                                                                                                                    |
| --------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`                 | Una lista de permitidos basada en &quot;clave de JOIN&quot;, sin producir un producto cartesiano.                                                             |
| `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`                 | Una lista de excluidos basada en &quot;clave de JOIN&quot;, sin producir un producto cartesiano.                                                              |
| `LEFT ANY JOIN`, `RIGHT ANY JOIN`, `INNER ANY JOIN` | Desactiva parcial (para el lado opuesto de `LEFT` y `RIGHT`) o completamente (para `INNER` y `FULL`) el producto cartesiano para los tipos estándar de `JOIN`. |
| `ASOF JOIN`, `LEFT ASOF JOIN`                       | Une secuencias con una coincidencia no exacta. El uso de `ASOF JOIN` se describe a continuación.                                                               |
| `PASTE JOIN`                                        | Realiza una concatenación horizontal de dos tablas.                                                                                                            |

:::note
Cuando [join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm) se establece en `partial_merge`, `RIGHT JOIN` y `FULL JOIN` solo son compatibles con strictness `ALL` (`SEMI`, `ANTI`, `ANY` y `ASOF` no son compatibles).
:::

<div id="settings">
  ## Configuración
</div>

El tipo de JOIN predeterminado se puede sobrescribir mediante la configuración [`join_default_strictness`](../../../operations/settings/settings.md#join_default_strictness).

El comportamiento del servidor de ClickHouse para las operaciones `ANY JOIN` depende de la configuración [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys).

**Véase también**

* [`join_algorithm`](../../../operations/settings/settings.md#join_algorithm)
* [`join_any_take_last_row`](../../../operations/settings/settings.md#join_any_take_last_row)
* [`join_use_nulls`](../../../operations/settings/settings.md#join_use_nulls)
* [`partial_merge_join_rows_in_right_blocks`](../../../operations/settings/settings.md#partial_merge_join_rows_in_right_blocks)
* [`join_on_disk_max_files_to_merge`](../../../operations/settings/settings.md#join_on_disk_max_files_to_merge)
* [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)

Use la configuración `cross_to_inner_join_rewrite` para definir el comportamiento cuando ClickHouse no puede reescribir un `CROSS JOIN` como un `INNER JOIN`. El valor predeterminado es `1`, lo que permite que el JOIN continúe, aunque será más lento. Establezca `cross_to_inner_join_rewrite` en `0` si desea que se genere un error, y establézcalo en `2` para no ejecutar los CROSS JOIN, sino forzar la reescritura de todos los JOIN con coma/CROSS. Si la reescritura falla cuando el valor es `2`, recibirá un mensaje de error que dice &quot;Please, try to simplify `WHERE` section&quot;.

<div id="on-section-conditions">
  ## Condiciones de la sección ON
</div>

Una sección `ON` puede contener varias condiciones combinadas mediante los operadores `AND` y `OR`. Las condiciones que especifican las claves de JOIN deben:

* hacer referencia tanto a la tabla izquierda como a la derecha
* usar el operador de igualdad

Otras condiciones pueden usar otros operadores lógicos, pero deben hacer referencia a la tabla izquierda o a la derecha de una consulta.

Las filas se unen si se cumple la condición compuesta en su totalidad. Si no se cumplen las condiciones, las filas aún pueden incluirse en el resultado según el tipo de `JOIN`. Tenga en cuenta que, si esas mismas condiciones se colocan en una sección `WHERE` y no se cumplen, las filas siempre se filtran del resultado.

El operador `OR` dentro de la cláusula `ON` funciona con el algoritmo hash join: para cada argumento `OR` con claves de JOIN para `JOIN`, se crea una tabla hash independiente, por lo que el consumo de memoria y el tiempo de ejecución de la consulta aumentan linealmente a medida que crece el número de expresiones `OR` en la cláusula `ON`.

:::note
Si una condición hace referencia a columnas de distintas tablas, por ahora solo se admite el operador de igualdad (`=`).
:::

**Ejemplo**

Considere `table_1` y `table_2`:

```response
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1 │ A    │     │  1 │ Text A         │     10 │
│  2 │ B    │     │  1 │ Another text A │     12 │
│  3 │ C    │     │  2 │ Text B         │     15 │
└────┴──────┘     └────┴────────────────┴────────┘
```

Consulta con una condición sobre la clave de JOIN y una condición adicional para `table_2`:

```sql title="Query"
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

Tenga en cuenta que el resultado contiene la fila con el nombre `C` y la columna de texto vacía. Se incluye en el resultado porque se utiliza un join de tipo `OUTER`.

```response title="Response"
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

Consulta con un JOIN de tipo `INNER` y múltiples condiciones:

```sql title="Query"
SELECT name, text, scores FROM table_1 INNER JOIN table_2
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

```sql title="Response"
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │     15 │
└──────┴────────┴────────┘
```

Consulta con un join de tipo `INNER` y una condición con `OR`:

```sql title="Query"
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

Consulta con un `JOIN` de tipo `INNER` y condiciones con `OR` y `AND`:

:::note

De forma predeterminada, se admiten condiciones de desigualdad siempre que usen columnas de la misma tabla.
Por ejemplo, `t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c`, porque `t1.b > 0` usa únicamente columnas de `t1` y `t2.b > t2.c` usa únicamente columnas de `t2`.
Sin embargo, puede probar la compatibilidad experimental para condiciones como `t1.a = t2.key AND t1.b > t2.key`; consulte la sección siguiente para obtener más información.

:::

```sql title="Query"
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

```response title="Response"
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

<div id="join-with-inequality-conditions-for-columns-from-different-tables">
  ## JOIN con condiciones de desigualdad para columnas de distintas tablas
</div>

Actualmente, ClickHouse admite `ALL/ANY/SEMI/ANTI INNER/LEFT/RIGHT/FULL JOIN` con condiciones de desigualdad, además de condiciones de igualdad. Las condiciones de desigualdad solo se admiten con los algoritmos de JOIN `hash` y `grace_hash`. Las condiciones de desigualdad no se admiten con `join_use_nulls`.

**Ejemplo**

Tabla `t1`:

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ a    │ 1 │ 1 │ 2 │
│ key1 │ b    │ 2 │ 3 │ 2 │
│ key1 │ c    │ 3 │ 2 │ 1 │
│ key1 │ d    │ 4 │ 7 │ 2 │
│ key1 │ e    │ 5 │ 5 │ 5 │
│ key2 │ a2   │ 1 │ 1 │ 1 │
│ key4 │ f    │ 2 │ 3 │ 4 │
└──────┴──────┴───┴───┴───┘
```

Tabla `t2`

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ A    │ 1 │ 2 │ 1 │
│ key1 │ B    │ 2 │ 1 │ 2 │
│ key1 │ C    │ 3 │ 4 │ 5 │
│ key1 │ D    │ 4 │ 1 │ 6 │
│ key3 │ a3   │ 1 │ 1 │ 1 │
│ key4 │ F    │ 1 │ 1 │ 1 │
└──────┴──────┴───┴───┴───┘
```

```sql
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.a < t2.a) ORDER BY (t1.key, t1.attr, t2.key, t2.attr);
```

```response
key1    a    1    1    2    key1    B    2    1    2
key1    a    1    1    2    key1    C    3    4    5
key1    a    1    1    2    key1    D    4    1    6
key1    b    2    3    2    key1    C    3    4    5
key1    b    2    3    2    key1    D    4    1    6
key1    c    3    2    1    key1    D    4    1    6
key1    d    4    7    2            0    0    \N
key1    e    5    5    5            0    0    \N
key2    a2    1    1    1            0    0    \N
key4    f    2    3    4            0    0    \N
```

<div id="null-values-in-join-keys">
  ## Valores `NULL` en las claves de `JOIN`
</div>

`NULL` no es igual a ningún valor, ni siquiera a sí mismo. Esto significa que, si una clave de `JOIN` tiene un valor `NULL` en una tabla, no coincidirá con un valor `NULL` de la otra tabla.

**Ejemplo**

Tabla `A`:

```response
┌───id─┬─name────┐
│    1 │ Alice   │
│    2 │ Bob     │
│ ᴺᵁᴸᴸ │ Charlie │
└──────┴─────────┘
```

Tabla `B`:

```response
┌───id─┬─score─┐
│    1 │    90 │
│    3 │    85 │
│ ᴺᵁᴸᴸ │    88 │
└──────┴───────┘
```

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON A.id = B.id
```

```response
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │     0 │
└─────────┴───────┘
```

Tenga en cuenta que la fila con `Charlie` de la tabla `A` y la fila con puntuación 88 de la tabla `B` no aparecen en el resultado debido al valor `NULL` en la clave de `JOIN`.

Si quiere hacer coincidir valores `NULL`, use la función `isNotDistinctFrom` para comparar las claves de `JOIN`.

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```markdown
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

<div id="asof-join-usage">
  ## Uso de ASOF JOIN
</div>

`ASOF JOIN` resulta útil cuando necesitas unir registros que no tienen una coincidencia exacta.

Este algoritmo de JOIN requiere una columna especial en las tablas. Esta columna:

* Debe contener una secuencia ordenada.
* Puede ser de uno de los siguientes tipos: [Int, UInt](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md), [Decimal](../../../sql-reference/data-types/decimal.md).
* En el algoritmo de join `hash`, no puede ser la única columna de la cláusula `JOIN`.

Sintaxis `ASOF JOIN ... ON`:

```sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

Puedes usar cualquier número de condiciones de igualdad y exactamente una condición de coincidencia más próxima. Por ejemplo, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Condiciones admitidas para la coincidencia más próxima: `>`, `>=`, `<`, `<=`.

Sintaxis de `ASOF JOIN ... USING`:

```sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` usa `equi_columnX` para unir por igualdad y `asof_column` para unir por la coincidencia más próxima con la condición `table_1.asof_column >= table_2.asof_column`. La columna `asof_column` es siempre la última de la cláusula `USING`.

Por ejemplo, considere las siguientes tablas:

```text
         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...
```

`ASOF JOIN` puede tomar la marca de tiempo de un evento de usuario de `table_1` y encontrar un evento en `table_2` cuya marca de tiempo sea la más cercana a la marca de tiempo del evento de `table_1`, según la condición de coincidencia más cercana. Los valores de marca de tiempo iguales son los más cercanos, si están disponibles. Aquí, la columna `user_id` puede usarse para hacer el JOIN por igualdad y la columna `ev_time` puede usarse para hacer el JOIN por coincidencia más cercana. En nuestro ejemplo, `event_1_1` puede unirse con `event_2_1` y `event_1_2` puede unirse con `event_2_3`, pero `event_2_2` no puede unirse.

:::note
`ASOF JOIN` solo es compatible con los algoritmos de JOIN `hash` y `full_sorting_merge`.
**No** es compatible con el motor de tabla [Join](../../../engines/table-engines/special/join.md).
:::

<div id="paste-join-usage">
  ## Uso de PASTE JOIN
</div>

El resultado de `PASTE JOIN` es una tabla que contiene todas las columnas de la subconsulta izquierda, seguidas de todas las columnas de la subconsulta derecha.
Las filas se emparejan según su posición en las tablas originales (el orden de las filas debe estar definido).
Si las subconsultas devuelven un número distinto de filas, las filas sobrantes se descartan.

Ejemplo:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers(2)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(2)
    ORDER BY a DESC
) AS t2

┌─a─┬─t2.a─┐
│ 0 │    1 │
│ 1 │    0 │
└───┴──────┘
```

Nota: en este caso, el resultado puede ser no determinista si la lectura se realiza en paralelo. Por ejemplo:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers_mt(5)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(10)
    ORDER BY a DESC
) AS t2
SETTINGS max_block_size = 2;

┌─a─┬─t2.a─┐
│ 2 │    9 │
│ 3 │    8 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 0 │    7 │
│ 1 │    6 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 4 │    5 │
└───┴──────┘
```

<div id="distributed-join">
  ## JOIN distribuido
</div>

Hay dos formas de ejecutar un JOIN que involucra tablas distribuidas:

* Al usar un `JOIN` normal, la consulta se envía a servidores remotos. Las subconsultas se ejecutan en cada uno de ellos para crear la tabla de la derecha, y el JOIN se realiza con esa tabla. En otras palabras, la tabla de la derecha se forma por separado en cada servidor.
* Al usar `GLOBAL ... JOIN`, primero el servidor solicitante ejecuta una subconsulta para calcular uno de los lados del JOIN y recopila el resultado en una tabla temporal. Esta tabla temporal se pasa luego a cada servidor remoto, y las consultas se ejecutan en ellos usando los datos temporales transmitidos. En los joins `LEFT` e `INNER`, la tabla de la derecha se calcula como la subconsulta. En los joins `RIGHT`, en cambio, se calcula la tabla de la izquierda, ya que la tabla de la derecha es la que se conserva y debe leerse desde los segmentos.

Tenga cuidado al usar `GLOBAL`. Para obtener más información, consulte la sección [Subconsultas distribuidas](/es/sql-reference/operators/in#distributed-subqueries).

<div id="implicit-type-conversion">
  ## Conversión implícita de tipos
</div>

Las consultas `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN` y `FULL JOIN` admiten la conversión implícita de tipos para las &quot;claves de JOIN&quot;. Sin embargo, la consulta no puede ejecutarse si las claves de JOIN de las tablas de la izquierda y de la derecha no pueden convertirse a un único tipo (por ejemplo, no existe ningún tipo de dato que pueda contener todos los valores de `UInt64` e `Int64`, o de `String` e `Int32`).

**Ejemplo**

Considere la tabla `t_1`:

```response
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```

y la tabla `t_2`:

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int16         │ Nullable(Int64) │
│  1 │   -1 │ Int16         │ Nullable(Int64) │
│  1 │    1 │ Int16         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

La consulta

```sql
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b);
```

devuelve el Set:

```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

<div id="usage-recommendations">
  ## Recomendaciones de uso
</div>

<div id="processing-of-empty-or-null-cells">
  ### Procesamiento de celdas vacías o NULL
</div>

Al unir tablas, pueden aparecer celdas vacías. La configuración [join&#95;use&#95;nulls](../../../operations/settings/settings.md#join_use_nulls) define cómo ClickHouse rellena estas celdas.

Si las claves de `JOIN` son campos [Nullable](../../../sql-reference/data-types/nullable.md), las filas en las que al menos una de las claves tiene el valor [NULL](/es/sql-reference/syntax#null) no se unen.

<div id="syntax">
  ### Sintaxis
</div>

Las columnas especificadas en `USING` deben tener el mismo nombre en ambas subconsultas, y las demás columnas deben tener nombres distintos. Puede usar alias para cambiar los nombres de las columnas en las subconsultas.

La cláusula `USING` especifica una o varias columnas para el join, lo que establece la igualdad entre esas columnas. La lista de columnas se indica sin paréntesis. No se admiten condiciones de join más complejas.

<div id="syntax-limitations">
  ### Limitaciones de la sintaxis
</div>

Para varias cláusulas `JOIN` en una sola consulta `SELECT`:

* Seleccionar todas las columnas con `*` solo está disponible si se unen tablas, no subconsultas.
* La cláusula `PREWHERE` no está disponible.
* La cláusula `USING` no está disponible.

Para las cláusulas `ON`, `WHERE` y `GROUP BY`:

* No se pueden usar expresiones arbitrarias en las cláusulas `ON`, `WHERE` y `GROUP BY`, pero se puede definir una expresión en una cláusula `SELECT` y luego usarla en estas cláusulas mediante un alias.

<div id="performance">
  ### Rendimiento
</div>

Al ejecutar un `JOIN`, no se optimiza el orden de ejecución con respecto a otras etapas de la consulta. El join (una búsqueda en la tabla de la derecha) se ejecuta antes del filtrado en `WHERE` y antes de la agregación.

Cada vez que se ejecuta una consulta con el mismo `JOIN`, la subconsulta vuelve a ejecutarse porque el resultado no se guarda en caché. Para evitarlo, use el motor de tabla especial [Join](../../../engines/table-engines/special/join.md), que es un array preparado para operaciones de join y que siempre reside en RAM.

En algunos casos, es más eficiente usar [IN](../../../sql-reference/operators/in.md) en lugar de `JOIN`.

Si necesita un `JOIN` para unir tablas de dimensión (tablas relativamente pequeñas que contienen propiedades de dimensión, como nombres de campañas publicitarias), puede que `JOIN` no sea la opción más conveniente, ya que se vuelve a acceder a la tabla de la derecha en cada consulta. Para estos casos, existe la funcionalidad de &quot;diccionarios&quot;, que debería usar en lugar de `JOIN`. Para más información, consulte la sección [Diccionarios](/es/sql-reference/statements/create/dictionary/overview.md).

<div id="memory-limitations">
  ### Limitaciones de memoria
</div>

De forma predeterminada, ClickHouse usa el algoritmo [hash join](https://en.wikipedia.org/wiki/Hash_join). ClickHouse toma la right&#95;table y crea una tabla hash para ella en RAM. Si `join_algorithm = 'auto'` está habilitado, al superar cierto umbral de consumo de memoria, ClickHouse recurre al algoritmo [merge](https://en.wikipedia.org/wiki/Sort-merge_join) join. Para ver la descripción de los algoritmos `JOIN`, consulte la configuración [join&#95;algorithm](../../../operations/settings/settings.md#join_algorithm).

Si necesita restringir el consumo de memoria de la operación `JOIN`, use la siguiente configuración:

* [max&#95;rows&#95;in&#95;join](/es/operations/settings/settings#max_rows_in_join) — Limita el número de filas en la tabla hash.
* [max&#95;bytes&#95;in&#95;join](/es/operations/settings/settings#max_bytes_in_join) — Limita el tamaño de la tabla hash.

Cuando se alcanza cualquiera de estos límites, ClickHouse actúa según lo que indique la configuración [join&#95;overflow&#95;mode](/es/operations/settings/settings#join_overflow_mode).

<div id="examples">
  ## Ejemplos
</div>

Ejemplo:

```sql
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

```text
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

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [ClickHouse: un SGBD rapidísimo con compatibilidad completa con SQL JOIN - Parte 1](https://clickhouse.com/blog/clickhouse-fully-supports-joins)
* Blog: [ClickHouse: un SGBD rapidísimo con compatibilidad completa con SQL JOIN - En profundidad - Parte 2](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)
* Blog: [ClickHouse: un SGBD rapidísimo con compatibilidad completa con SQL JOIN - En profundidad - Parte 3](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3)
* Blog: [ClickHouse: un SGBD rapidísimo con compatibilidad completa con SQL JOIN - En profundidad - Parte 4](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)