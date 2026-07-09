---
description: 'Documentación de la consulta SELECT'
sidebar_label: 'SELECT'
sidebar_position: 32
slug: /sql-reference/statements/select/
title: 'Consulta SELECT'
doc_type: 'referencia'
---

Las consultas `SELECT` recuperan datos. De forma predeterminada, los datos solicitados se devuelven al client, mientras que, en combinación con [INSERT INTO](../../../sql-reference/statements/insert-into.md), pueden redirigirse a otra tabla.

<div id="syntax">
  ## Sintaxis
</div>

```sql
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

Todas las cláusulas son opcionales, excepto la lista obligatoria de expresiones inmediatamente después de `SELECT`, que se explica con más detalle [más abajo](#select-clause).

Los detalles de cada cláusula opcional se explican en secciones independientes, enumeradas en el mismo orden en que se ejecutan:

* [cláusula WITH](../../../sql-reference/statements/select/with.md)
* [cláusula SELECT](#select-clause)
* [cláusula DISTINCT](../../../sql-reference/statements/select/distinct.md)
* [cláusula FROM](../../../sql-reference/statements/select/from.md)
* [cláusula SAMPLE](../../../sql-reference/statements/select/sample.md)
* [cláusula JOIN](../../../sql-reference/statements/select/join.md)
* [cláusula PREWHERE](../../../sql-reference/statements/select/prewhere.md)
* [cláusula WHERE](../../../sql-reference/statements/select/where.md)
* [cláusula WINDOW](../../../sql-reference/window-functions/index.md)
* [cláusula GROUP BY](/es/sql-reference/statements/select/group-by)
* [cláusula LIMIT BY](../../../sql-reference/statements/select/limit-by.md)
* [cláusula HAVING](../../../sql-reference/statements/select/having.md)
* [cláusula QUALIFY](../../../sql-reference/statements/select/qualify.md)
* [cláusula LIMIT](../../../sql-reference/statements/select/limit.md)
* [cláusula OFFSET](../../../sql-reference/statements/select/offset.md)
* [cláusula UNION](../../../sql-reference/statements/select/union.md)
* [cláusula INTERSECT](../../../sql-reference/statements/select/intersect.md)
* [cláusula EXCEPT](../../../sql-reference/statements/select/except.md)
* [cláusula INTO OUTFILE](../../../sql-reference/statements/select/into-outfile.md)
* [cláusula FORMAT](../../../sql-reference/statements/select/format.md)

<div id="select-clause">
  ## Cláusula SELECT
</div>

Las [expresiones](/es/sql-reference/syntax#expressions) especificadas en la cláusula `SELECT` se calculan una vez finalizadas todas las operaciones de las cláusulas descritas anteriormente. Estas expresiones funcionan como si se aplicaran a filas independientes del resultado. Si las expresiones de la cláusula `SELECT` contienen funciones de agregación, ClickHouse procesa las funciones de agregación y las expresiones utilizadas como sus argumentos durante la agregación [GROUP BY](/es/sql-reference/statements/select/group-by).

Si desea incluir todas las columnas en el resultado, use el símbolo de asterisco (`*`). Por ejemplo, `SELECT * FROM ...`.

<div id="dynamic-column-selection">
  ### Selección dinámica de columnas
</div>

La selección dinámica de columnas (también conocida como expresión COLUMNS) permite hacer coincidir algunas columnas de un resultado mediante una [re2](https://en.wikipedia.org/wiki/RE2_\(software\)) expresión regular.

```sql
COLUMNS('regexp')
```

Por ejemplo, considere la tabla:

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

La siguiente consulta selecciona datos de todas las columnas cuyo nombre contiene el símbolo `a`.

```sql
SELECT COLUMNS('a') FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Las columnas seleccionadas no se devuelven en orden alfabético.

Puede usar varias expresiones `COLUMNS` en una consulta y aplicarles funciones.

Por ejemplo:

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Cada columna devuelta por la expresión `COLUMNS` se pasa a la función como un argumento independiente. Además, también puede pasarle otros argumentos a la función si los admite. Tenga cuidado al usar funciones. Si una función no admite la cantidad de argumentos que se le han pasado, ClickHouse lanza una excepción.

Por ejemplo:

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

En este ejemplo, `COLUMNS('a')` devuelve dos columnas: `aa` y `ab`. `COLUMNS('c')` devuelve la columna `bc`. El operador `+` no puede aplicarse a 3 argumentos, por lo que ClickHouse arroja una excepción con el mensaje correspondiente.

Las columnas que coinciden con la expresión `COLUMNS` pueden tener distintos tipos de datos. Si `COLUMNS` no coincide con ninguna columna y es la única expresión de `SELECT`, ClickHouse arroja una excepción.

<div id="select-columns-with-like-or-ilike">
  #### Seleccionar columnas con `LIKE` o `ILIKE`
</div>

También puedes seleccionar columnas haciendo que sus nombres coincidan con un patrón después de `*`, usando `LIKE`, que es sensible a mayúsculas y minúsculas, o `ILIKE`, que no distingue entre mayúsculas y minúsculas:

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

Los patrones `LIKE` e `ILIKE` siguen la semántica de `LIKE`, no la de las expresiones regulares. El carácter `%` coincide con cualquier secuencia de caracteres, el carácter `_` coincide con cualquier carácter individual y `\` actúa como carácter de escape para `%`, `_` y `\`. La única diferencia entre ambos es que `LIKE` hace coincidir los nombres de columna de forma sensible a mayúsculas y minúsculas, mientras que `ILIKE` no distingue entre mayúsculas y minúsculas. Por ejemplo:

```sql
SELECT * ILIKE 'a_' FROM col_names
```

La consulta selecciona columnas con nombres de dos caracteres que empiezan por `a`, como `aa` y `ab`.

`* LIKE` y `* ILIKE` también admiten asteriscos calificados y transformadores de columnas:

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

<div id="asterisk">
  ### Asterisco
</div>

Puede colocar un asterisco en cualquier parte de una consulta en lugar de una expresión. Cuando se analiza la consulta, el asterisco se expande a una lista de todas las columnas de la tabla (excepto las columnas `MATERIALIZED` y `ALIAS`). Solo hay unos pocos casos en los que se justifica usar un asterisco:

* Al crear un dump de una tabla.
* En tablas que contienen solo unas pocas columnas, como las tablas del sistema.
* Para obtener información sobre qué columnas hay en una tabla. En este caso, establezca `LIMIT 1`. Pero es mejor usar la consulta `DESC TABLE`.
* Cuando hay un filtrado intenso sobre un número reducido de columnas mediante `PREWHERE`.
* En subconsultas (ya que las columnas que no son necesarias para la consulta externa se excluyen de las subconsultas).

En todos los demás casos, no recomendamos usar el asterisco, ya que solo aporta las desventajas de un SGBD columnar en lugar de sus ventajas. En otras palabras, no se recomienda usar el asterisco.

<div id="extreme-values">
  ### Valores extremos
</div>

Además de los resultados, también puede obtener los valores mínimo y máximo de las columnas del resultado. Para ello, establezca la configuración **extremes** en 1. Los mínimos y máximos se calculan para tipos numéricos, fechas y valores de fecha y hora. Para las demás columnas, se devuelven los valores predeterminados.

Se calculan dos filas adicionales: una con los mínimos y otra con los máximos. Estas dos filas adicionales se muestran en los [formatos](../../../interfaces/formats.md) `XML`, `JSON*`, `TabSeparated*`, `CSV*`, `Vertical`, `Template` y `Pretty*`, por separado del resto de las filas. No se muestran en otros formatos.

En los formatos `JSON*` y `XML`, los valores extremos se muestran en un campo independiente llamado &#39;extremes&#39;. En los formatos `TabSeparated*`, `CSV*` y `Vertical`, la fila aparece después del resultado principal, y después de &#39;totals&#39; si está presente. Va precedida de una fila vacía (después de los demás datos). En los formatos `Pretty*`, la fila se muestra como una tabla independiente después del resultado principal, y después de `totals` si está presente. En el formato `Template`, los valores extremos se muestran según la plantilla especificada.

Los valores extremos se calculan para las filas anteriores a `LIMIT`, pero posteriores a `LIMIT BY`. Sin embargo, al usar `LIMIT offset, size`, las filas anteriores a `offset` se incluyen en `extremes`. En las solicitudes en streaming, el resultado también puede incluir una pequeña cantidad de filas que pasaron por `LIMIT`.

<div id="notes">
  ### Notas
</div>

Puede usar sinónimos (alias `AS`) en cualquier parte de una consulta.

Las cláusulas `GROUP BY`, `ORDER BY` y `LIMIT BY` admiten argumentos posicionales. Para habilitarlos, active la opción [enable&#95;positional&#95;arguments](/es/operations/settings/settings#enable_positional_arguments). Entonces, por ejemplo, `ORDER BY 1,2` ordenará las filas de la tabla por la primera columna y luego por la segunda.

<div id="implementation-details">
  ## Detalles de implementación
</div>

Si la consulta omite las cláusulas `DISTINCT`, `GROUP BY` y `ORDER BY`, así como las subconsultas `IN` y `JOIN`, se procesará por completo en streaming, con un uso de RAM de O(1). De lo contrario, la consulta puede consumir mucha RAM si no se especifican las restricciones adecuadas:

* `max_memory_usage`
* `max_rows_to_group_by`
* `max_rows_to_sort`
* `max_rows_in_distinct`
* `max_bytes_in_distinct`
* `max_rows_in_set`
* `max_bytes_in_set`
* `max_rows_in_join`
* `max_bytes_in_join`
* `max_bytes_before_external_sort`
* `max_bytes_ratio_before_external_sort`
* `max_bytes_before_external_group_by`
* `max_bytes_ratio_before_external_group_by`

Para obtener más información, consulte la sección &quot;Settings&quot;. Es posible usar ordenación externa (guardando tablas temporales en disco) y agregación externa.

<div id="select-modifiers">
  ## Modificadores de SELECT
</div>

Puede usar los siguientes modificadores en las consultas `SELECT`.

| Modificador                        | Descripción                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ---------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [`APPLY`](./apply_modifier.md)     | Permite invocar una función para cada fila devuelta por una expresión de tabla externa de una consulta.                                                                                                                                                                                                                                                                                                                       |
| [`EXCEPT`](./except_modifier.md)   | Especifica los nombres de una o más columnas que deben excluirse del resultado. Todos los nombres de columna coincidentes se omiten de la salida.                                                                                                                                                                                                                                                                             |
| [`REPLACE`](./replace_modifier.md) | Especifica uno o más [alias de expresión](/es/sql-reference/syntax#expression-aliases). Cada alias debe coincidir con el nombre de una columna de la instrucción `SELECT *`. En la lista de columnas de salida, la columna que coincide con el alias se reemplaza por la expresión de ese `REPLACE`. Este modificador no cambia los nombres ni el orden de las columnas. Sin embargo, puede cambiar el valor y el tipo de valor. |

<div id="modifier-combinations">
  ### Combinaciones de modificadores
</div>

Puede usar cada modificador por separado o combinarlos.

**Ejemplos:**

Uso del mismo modificador varias veces.

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

Uso de varios modificadores en una sola consulta.

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

<div id="settings-in-select-query">
  ## SETTINGS en la consulta SELECT
</div>

Puede especificar los ajustes necesarios directamente en la consulta `SELECT`. El valor del ajuste se aplica solo a esta consulta y se restablece al valor predeterminado o al valor anterior una vez ejecutada la consulta.

Para conocer otras formas de definir ajustes, consulte [aquí](/es/operations/settings/overview).

En los ajustes booleanos establecidos en true, puede usar una sintaxis abreviada omitiendo la asignación de valor. Cuando solo se especifica el nombre del ajuste, este se establece automáticamente en `1` (true).

**Ejemplo**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```