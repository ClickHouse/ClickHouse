---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Cláusula JOIN {#select-join}

Join produce una nueva tabla combinando columnas de una o varias tablas utilizando valores comunes a cada una. Es una operación común en bases de datos con soporte SQL, que corresponde a [álgebra relacional](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators) unir. El caso especial de una combinación de tabla a menudo se conoce como “self-join”.

Sintaxis:

``` sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

Expresiones de `ON` y columnas de `USING` cláusula se llaman “join keys”. A menos que se indique lo contrario, join produce un [Producto cartesiano](https://en.wikipedia.org/wiki/Cartesian_product) de filas con coincidencia “join keys”, lo que podría producir resultados con muchas más filas que las tablas de origen.

## Tipos admitidos de JOIN {#select-join-types}

Todo estándar [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) tipos son compatibles:

-   `INNER JOIN`, sólo se devuelven las filas coincidentes.
-   `LEFT OUTER JOIN`, filas no coincidentes de la tabla izquierda se devuelven además de las filas coincidentes.
-   `RIGHT OUTER JOIN`, filas no coincidentes de la tabla izquierda se devuelven además de las filas coincidentes.
-   `FULL OUTER JOIN`, las filas que no coinciden de ambas tablas se devuelven además de las filas coincidentes.
-   `CROSS JOIN`, produce el producto cartesiano de tablas enteras, “join keys” ser **ni** indicado.

`JOIN` sin tipo especificado implica `INNER`. Palabra clave `OUTER` se puede omitir con seguridad. Sintaxis alternativa para `CROSS JOIN` está especificando múltiples tablas en [Cláusula FROM](from.md) separados por comas.

Tipos de unión adicionales disponibles en ClickHouse:

-   `LEFT SEMI JOIN` y `RIGHT SEMI JOIN`, una lista blanca en “join keys”, sin producir un producto cartesiano.
-   `LEFT ANTI JOIN` y `RIGHT ANTI JOIN`, una lista negra sobre “join keys”, sin producir un producto cartesiano.
-   `LEFT ANY JOIN`, `RIGHT ANY JOIN` and `INNER ANY JOIN`, partially (for opposite side of `LEFT` and `RIGHT`) or completely (for `INNER` and `FULL`) disables the cartesian product for standard `JOIN` types.
-   `ASOF JOIN` and `LEFT ASOF JOIN`, joining sequences with a non-exact match. `ASOF JOIN` usage is described below.

## Setting {#join-settings}

!!! note "Nota"
    El valor de rigor predeterminado se puede anular usando [Por favor, introduzca su dirección de correo electrónico](../../../operations/settings/settings.md#settings-join_default_strictness) configuración.

### ASOF JOIN Uso {#asof-join-usage}

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
    `ASOF` unirse es **ni** apoyado en el [Unir](../../../engines/table-engines/special/join.md) motor de mesa.

## Unión distribuida {#global-join}

Hay dos formas de ejecutar la unión que involucra tablas distribuidas:

-   Cuando se utiliza una normal `JOIN`, la consulta se envía a servidores remotos. Las subconsultas se ejecutan en cada una de ellas para crear la tabla correcta, y la unión se realiza con esta tabla. En otras palabras, la tabla correcta se forma en cada servidor por separado.
-   Cuando se utiliza `GLOBAL ... JOIN`, primero el servidor requestor ejecuta una subconsulta para calcular la tabla correcta. Esta tabla temporal se pasa a cada servidor remoto y las consultas se ejecutan en ellos utilizando los datos temporales que se transmitieron.

Tenga cuidado al usar `GLOBAL`. Para obtener más información, consulte [Subconsultas distribuidas](../../operators/in.md#select-distributed-subqueries) apartado.

## Recomendaciones de uso {#usage-recommendations}

### Procesamiento de celdas vacías o NULL {#processing-of-empty-or-null-cells}

Al unir tablas, pueden aparecer las celdas vacías. Configuración [Sistema abierto.](../../../operations/settings/settings.md#join_use_nulls) definir cómo ClickHouse llena estas celdas.

Si el `JOIN` las llaves son [NULL](../../data-types/nullable.md) campos, las filas donde al menos una de las claves tiene el valor [NULL](../../../sql-reference/syntax.md#null-literal) no se unen.

### Sintaxis {#syntax}

Las columnas especificadas en `USING` debe tener los mismos nombres en ambas subconsultas, y las otras columnas deben tener un nombre diferente. Puede utilizar alias para cambiar los nombres de las columnas en subconsultas.

El `USING` clause especifica una o más columnas a unir, lo que establece la igualdad de estas columnas. La lista de columnas se establece sin corchetes. No se admiten condiciones de unión más complejas.

### Limitaciones de sintaxis {#syntax-limitations}

Para múltiples `JOIN` cláusulas en una sola `SELECT` consulta:

-   Tomando todas las columnas a través de `*` está disponible solo si se unen tablas, no subconsultas.
-   El `PREWHERE` cláusula no está disponible.

Para `ON`, `WHERE`, y `GROUP BY` clausula:

-   Las expresiones arbitrarias no se pueden utilizar en `ON`, `WHERE`, y `GROUP BY` cláusulas, pero puede definir una expresión en un `SELECT` cláusula y luego usarla en estas cláusulas a través de un alias.

### Rendimiento {#performance}

Cuando se ejecuta un `JOIN`, no hay optimización del orden de ejecución en relación con otras etapas de la consulta. La combinación (una búsqueda en la tabla de la derecha) se ejecuta antes de filtrar `WHERE` y antes de la agregación.

Cada vez que se ejecuta una consulta `JOIN`, la subconsulta se ejecuta de nuevo porque el resultado no se almacena en caché. Para evitar esto, use el especial [Unir](../../../engines/table-engines/special/join.md) motor de tabla, que es una matriz preparada para unirse que siempre está en RAM.

En algunos casos, es más eficiente de usar [IN](../../operators/in.md) en lugar de `JOIN`.

Si necesita un `JOIN` para unirse a tablas de dimensión (son tablas relativamente pequeñas que contienen propiedades de dimensión, como nombres para campañas publicitarias), un `JOIN` podría no ser muy conveniente debido al hecho de que se vuelve a acceder a la tabla correcta para cada consulta. Para tales casos, hay un “external dictionaries” característica que debe utilizar en lugar de `JOIN`. Para obtener más información, consulte [Diccionarios externos](../../dictionaries/external-dictionaries/external-dicts.md) apartado.

### Limitaciones de memoria {#memory-limitations}

De forma predeterminada, ClickHouse usa el [hash unirse](https://en.wikipedia.org/wiki/Hash_join) algoritmo. ClickHouse toma el `<right_table>` y crea una tabla hash para ello en RAM. Después de algún umbral de consumo de memoria, ClickHouse vuelve a fusionar el algoritmo de unión.

Si necesita restringir el consumo de memoria de la operación de unión, use la siguiente configuración:

-   [Método de codificación de datos:](../../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [Método de codificación de datos:](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

Cuando se alcanza cualquiera de estos límites, ClickHouse actúa como el [join_overflow_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode) configuración instruye.

## Ejemplos {#examples}

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
