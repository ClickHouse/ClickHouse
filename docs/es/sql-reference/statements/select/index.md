---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
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
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] 
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

Todas las cláusulas son opcionales, excepto la lista requerida de expresiones inmediatamente después `SELECT` que está cubierto con más detalle [debajo](#select-clause).

Los detalles de cada cláusula opcional se cubren en secciones separadas, que se enumeran en el mismo orden en que se ejecutan:

-   [CON cláusula](with.md)
-   [Cláusula DISTINCT](distinct.md)
-   [Cláusula FROM](from.md)
-   [Cláusula SAMPLE](sample.md)
-   [Cláusula JOIN](join.md)
-   [Cláusula PREWHERE](prewhere.md)
-   [Cláusula WHERE](where.md)
-   [Cláusula GROUP BY](group-by.md)
-   [Cláusula LIMIT BY](limit-by.md)
-   [Cláusula HAVING](having.md)
-   [Cláusula SELECT](#select-clause)
-   [Cláusula LIMIT](limit.md)
-   [UNION ALL cláusula](union-all.md)

## SELECT Cláusula {#select-clause}

[Expresiones](../../syntax.md#syntax-expressions) especificado en el `SELECT` cláusula se calculan después de que todas las operaciones en las cláusulas descritas anteriormente hayan finalizado. Estas expresiones funcionan como si se aplicaran a filas separadas en el resultado. Si las expresiones en el `SELECT` cláusula contiene funciones agregadas, a continuación, ClickHouse procesa funciones agregadas y expresiones utilizadas como sus argumentos durante el [GROUP BY](group-by.md) agregación.

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

### Asterisco {#asterisk}

Puede poner un asterisco en cualquier parte de una consulta en lugar de una expresión. Cuando se analiza la consulta, el asterisco se expande a una lista de todas las columnas de la tabla `MATERIALIZED` y `ALIAS` columna). Solo hay unos pocos casos en los que se justifica el uso de un asterisco:

-   Al crear un volcado de tabla.
-   Para tablas que contienen solo unas pocas columnas, como las tablas del sistema.
-   Para obtener información sobre qué columnas están en una tabla. En este caso, establezca `LIMIT 1`. Pero es mejor usar el `DESC TABLE` consulta.
-   Cuando hay una filtración fuerte en un pequeño número de columnas usando `PREWHERE`.
-   En subconsultas (ya que las columnas que no son necesarias para la consulta externa están excluidas de las subconsultas).

En todos los demás casos, no recomendamos usar el asterisco, ya que solo le da los inconvenientes de un DBMS columnar en lugar de las ventajas. En otras palabras, no se recomienda usar el asterisco.

### Valores extremos {#extreme-values}

Además de los resultados, también puede obtener valores mínimos y máximos para las columnas de resultados. Para hacer esto, establezca el **extremo** a 1. Los mínimos y máximos se calculan para tipos numéricos, fechas y fechas con horas. Para otras columnas, se generan los valores predeterminados.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`, y `Pretty*` [formato](../../../interfaces/formats.md), separado de las otras filas. No se emiten para otros formatos.

En `JSON*` los valores extremos se emiten en un formato separado. ‘extremes’ campo. En `TabSeparated*` , la fila viene después del resultado principal, y después de ‘totals’ si está presente. Está precedido por una fila vacía (después de los otros datos). En `Pretty*` formatea, la fila se muestra como una tabla separada después del resultado principal, y después de `totals` si está presente.

Los valores extremos se calculan para las filas anteriores `LIMIT`, pero después `LIMIT BY`. Sin embargo, cuando se usa `LIMIT offset, size`, las filas antes `offset` están incluidos en `extremes`. En las solicitudes de secuencia, el resultado también puede incluir un pequeño número de filas que pasaron por `LIMIT`.

### Nota {#notes}

Puedes usar sinónimos (`AS` aliases) en cualquier parte de una consulta.

El `GROUP BY` y `ORDER BY` las cláusulas no admiten argumentos posicionales. Esto contradice MySQL, pero se ajusta al SQL estándar. Por ejemplo, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

## Detalles de implementación {#implementation-details}

Si la consulta omite el `DISTINCT`, `GROUP BY` y `ORDER BY` cláusulas y el `IN` y `JOIN` subconsultas, la consulta se procesará por completo, utilizando O (1) cantidad de RAM. De lo contrario, la consulta podría consumir mucha RAM si no se especifican las restricciones adecuadas:

-   `max_memory_usage`
-   `max_rows_to_group_by`
-   `max_rows_to_sort`
-   `max_rows_in_distinct`
-   `max_bytes_in_distinct`
-   `max_rows_in_set`
-   `max_bytes_in_set`
-   `max_rows_in_join`
-   `max_bytes_in_join`
-   `max_bytes_before_external_sort`
-   `max_bytes_before_external_group_by`

Para obtener más información, consulte la sección “Settings”. Es posible utilizar la clasificación externa (guardar tablas temporales en un disco) y la agregación externa.

{## [Artículo Original](https://clickhouse.tech/docs/en/sql-reference/statements/select/) ##}
