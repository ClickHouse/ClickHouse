---
description: 'Documentación sobre los operadores IN, excluyendo los operadores NOT IN, GLOBAL IN y GLOBAL
  NOT IN, que se tratan por separado'
slug: /sql-reference/operators/in
title: 'Operadores IN'
doc_type: 'reference'
---

Los operadores `IN`, `NOT IN`, `GLOBAL IN` y `GLOBAL NOT IN` se tratan por separado, dado que su funcionalidad es bastante extensa.

El lado izquierdo del operador es una sola columna o una tupla.

Ejemplos:

```sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

Si el lado izquierdo es una sola columna que forma parte del índice y el lado derecho es un conjunto de constantes, el sistema utiliza el índice para procesar la consulta.

No enumere demasiados valores de forma explícita (es decir, millones). Si un conjunto de datos es grande, colóquelo en una tabla temporal (por ejemplo, consulte la sección [External data for query processing](../../engines/table-engines/special/external-data.md)) y luego use una subconsulta.

El lado derecho del operador puede ser un conjunto de expresiones constantes, un conjunto de tuplas con expresiones constantes (como se muestra en los ejemplos anteriores), o el nombre de una tabla de base de datos o una subconsulta `SELECT` entre corchetes.

Por compatibilidad histórica, cuando el lado derecho es una única expresión `tuple`, puede interpretarse como un conjunto de valores o como un único tuple value, dependiendo del lado izquierdo del operador `IN`. Si el lado izquierdo es un valor escalar, ClickHouse trata los elementos de esta única expresión `tuple` del lado derecho como valores `IN` independientes:

```sql title="Query"
SELECT
    1 IN (tuple(1, 2)) AS one_in_tuple,
    2 IN (tuple(1, 2)) AS two_in_tuple,
    3 IN (tuple(1, 2)) AS three_in_tuple;
```

```text title="Response"
┌─one_in_tuple─┬─two_in_tuple─┬─three_in_tuple─┐
│            1 │            1 │              0 │
└──────────────┴──────────────┴────────────────┘
```

Esto se comporta como `SELECT 1 IN (1, 2)`. Si el lado izquierdo también es una tupla, el lado derecho se interpreta como un Set de valores de tupla:

```sql title="Query"
SELECT tuple(1, 2) IN (tuple(1, 2)) AS tuple_in_tuple;
```

```text title="Response"
┌─tuple_in_tuple─┐
│              1 │
└────────────────┘
```

Este tratamiento especial se aplica únicamente cuando el lado derecho es una expresión `tuple` simple. Un lado izquierdo escalar no puede compararse con un lado derecho que contenga múltiples valores de tupla:

```sql title="Query"
SELECT 1 IN (tuple(1, 2), tuple(3, 4));
```

```text title="Response"
Code: 43. DB::Exception: Unsupported types for IN. First argument type UInt8. Second argument type Tuple(Tuple(UInt8, UInt8), Tuple(UInt8, UInt8)). (ILLEGAL_TYPE_OF_ARGUMENT)
```

ClickHouse permite que los tipos difieran en las partes izquierda y derecha de la subconsulta `IN`.
En este caso, convierte el valor del lado derecho al tipo del lado izquierdo, como si se aplicara la función [accurateCastOrNull](/es/sql-reference/functions/type-conversion-functions#accurateCastOrNull) al lado derecho.

Esto significa que el tipo de dato pasa a ser [Nullable](../../sql-reference/data-types/nullable.md) y, si la conversión
no puede realizarse, devuelve [NULL](/es/operations/settings/formats#input_format_null_as_default).

**Ejemplo**

```sql title="Query"
SELECT '1' IN (SELECT 1);
```

```text title="Response"
┌─in('1', _subquery49)─┐
│                    1 │
└──────────────────────┘
```

Si el lado derecho del operador es el nombre de una tabla (por ejemplo, `UserID IN users`), esto equivale a la subconsulta `UserID IN (SELECT * FROM users)`. Utilice esto cuando trabaje con datos externos que se envían junto con la consulta. Por ejemplo, la consulta puede enviarse junto con un conjunto de IDs de usuario cargados en la tabla temporal &#39;users&#39;, que debe filtrarse.

Si el lado derecho del operador es un nombre de tabla que utiliza el motor Set (un conjunto de datos preparado que siempre reside en RAM), el conjunto de datos no se recreará en cada consulta.

La subconsulta puede especificar más de una columna para filtrar tuplas.

Ejemplo:

```sql title="Query"
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

Las columnas a la izquierda y a la derecha del operador `IN` deben ser del mismo tipo.

El operador `IN` y la subconsulta pueden aparecer en cualquier parte de la consulta, incluidas las funciones de agregación y las funciones lambda.
Ejemplo:

```sql title="Query"
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

```text title="Response"
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

Para cada día posterior al 17 de marzo, calcula el porcentaje de visitas de página realizadas por usuarios que visitaron el sitio el 17 de marzo.
Una subconsulta en la cláusula `IN` siempre se ejecuta una sola vez en un único servidor. No hay subconsultas dependientes.

<div id="null-processing">
  ## Procesamiento de NULL
</div>

Durante el procesamiento de la solicitud, el operador `IN` asume que el resultado de una operación con [NULL](/es/operations/settings/formats#input_format_null_as_default) siempre es igual a `0`, independientemente de si `NULL` está en el lado derecho o izquierdo del operador. Los valores `NULL` no se incluyen en ningún conjunto de datos, no se corresponden entre sí y no se pueden comparar si [transform&#95;null&#95;in = 0](../../operations/settings/settings.md#transform_null_in).

A continuación se muestra un ejemplo con la tabla `t_null`:

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Al ejecutar la consulta `SELECT x FROM t_null WHERE y IN (NULL,3)`, se obtiene el siguiente resultado:

```text
┌─x─┐
│ 2 │
└───┘
```

Puede ver que la fila en la que `y = NULL` queda excluida de los resultados de la consulta. Esto se debe a que ClickHouse no puede determinar si `NULL` está incluido en el conjunto `(NULL,3)`, devuelve `0` como resultado de la operación y `SELECT` excluye esta fila de la salida final.

```sql
SELECT y IN (NULL, 3)
FROM t_null
```

```text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

<div id="distributed-subqueries">
  ## Subconsultas distribuidas
</div>

Existen dos opciones para los operadores `IN` con subconsultas (similares a los operadores `JOIN`): `IN` / `JOIN` normal y `GLOBAL IN` / `GLOBAL JOIN`. Se diferencian en cómo se ejecutan durante el procesamiento distribuido de consultas.

:::note
Tenga en cuenta que los algoritmos descritos a continuación pueden funcionar de manera diferente según la configuración [settings](../../operations/settings/settings.md) `distributed_product_mode`.
:::

Al usar el `IN` regular, la consulta se envía a los servidores remotos, y cada uno de ellos ejecuta las subconsultas en la cláusula `IN` o `JOIN`.

Al usar `GLOBAL IN` / `GLOBAL JOIN`, primero se ejecutan todas las subconsultas para `GLOBAL IN` / `GLOBAL JOIN` y los resultados se recopilan en tablas temporales. Luego, las tablas temporales se envían a cada servidor remoto, donde las consultas se ejecutan con estos datos temporales.

En el caso de `GLOBAL ... JOIN`, el lado del join que se calcula como subconsulta depende del tipo de join: para los joins `LEFT` e `INNER`, se calcula la tabla derecha; para los joins `RIGHT`, en cambio, se calcula la tabla izquierda, ya que la tabla derecha es el lado preservado y debe leerse desde los segmentos.

Para una consulta no distribuida, utilice el `IN` / `JOIN` habitual.

Tenga cuidado al usar subconsultas en las cláusulas `IN` / `JOIN` para el procesamiento distribuido de consultas.

Veamos algunos ejemplos. Supongamos que cada servidor del clúster tiene una **local&#95;table** normal. Cada servidor también tiene una tabla **distributed&#95;table** de tipo **Distributed**, que abarca todos los servidores del clúster.

Para una consulta a la **distributed&#95;table**, la consulta se enviará a todos los servidores remotos y se ejecutará en ellos mediante la **local&#95;table**.

Por ejemplo, la consulta

```sql
SELECT uniq(UserID) FROM distributed_table
```

se enviará a todos los servidores remotos como

```sql
SELECT uniq(UserID) FROM local_table
```

y se ejecutan en cada uno de ellos en paralelo, hasta alcanzar la etapa en la que los resultados intermedios pueden combinarse. Luego, los resultados intermedios se devuelven al servidor solicitante y se fusionan en él, y el resultado final se envía al client.

Ahora examinemos una consulta con `IN`:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

* Cálculo de la intersección de las audiencias de dos sitios.

Esta consulta se enviará a todos los servidores remotos como

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

En otras palabras, el conjunto de datos de la cláusula `IN` se recopilará en cada servidor de forma independiente, considerando únicamente los datos almacenados localmente en cada uno de ellos.

Esto funcionará de forma correcta y óptima si se ha preparado para este caso y ha distribuido los datos entre los servidores del clúster de modo que los datos de un único UserID residan íntegramente en un único servidor. En ese caso, todos los datos necesarios estarán disponibles localmente en cada servidor. De lo contrario, el resultado será inexacto. A esta variante de la consulta la denominamos &quot;local IN&quot;.

Para corregir el funcionamiento de la consulta cuando los datos están distribuidos aleatoriamente entre los servidores del clúster, puede especificar **distributed&#95;table** dentro de una subconsulta. La consulta quedaría así:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

Esta consulta se enviará a todos los servidores remotos como

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

La subconsulta comenzará a ejecutarse en cada servidor remoto. Dado que la subconsulta utiliza una tabla distribuida, la subconsulta presente en cada servidor remoto se reenviará a todos los servidores remotos de la siguiente manera:

```sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

Por ejemplo, si tiene un clúster de 100 servidores, ejecutar la consulta completa requerirá 10 000 solicitudes elementales, lo cual generalmente se considera inaceptable.

En estos casos, siempre debe usar `GLOBAL IN` en lugar de `IN`. Veamos cómo funciona para la siguiente consulta:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

El servidor solicitante ejecutará la subconsulta:

```sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

y el resultado se almacenará en una tabla temporal en RAM. A continuación, la solicitud se enviará a cada servidor remoto como:

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

La tabla temporal `_data1` se enviará a cada servidor remoto con la consulta (el nombre de la tabla temporal depende de la implementación).

Esto es más eficiente que usar `IN` normal. Sin embargo, tenga en cuenta los siguientes puntos:

1. Al crear una tabla temporal, los datos no se deduplican. Para reducir el volumen de datos transmitidos por la red, especifique DISTINCT en la subconsulta. (No es necesario hacer esto con un `IN` normal).
2. La tabla temporal se enviará a todos los servidores remotos. La transmisión no tiene en cuenta la topología de la red. Por ejemplo, si 10 servidores remotos están en un centro de datos muy alejado del servidor solicitante, los datos se enviarán 10 veces por el canal hacia ese centro de datos remoto. Intente evitar conjuntos de datos grandes al usar `GLOBAL IN`.
3. Al transmitir datos a servidores remotos, las limitaciones del ancho de banda de la red no se pueden configurar. Podría sobrecargar la red.
4. Intente distribuir los datos entre los servidores para no tener que usar `GLOBAL IN` de forma habitual.
5. Si necesita usar `GLOBAL IN` con frecuencia, planifique la ubicación del clúster de ClickHouse de modo que un único grupo de réplicas resida en no más de un centro de datos, con una red rápida entre ellas, para que una consulta pueda procesarse por completo dentro de un único centro de datos.

También tiene sentido especificar una tabla local en la cláusula `GLOBAL IN` si esa tabla local solo está disponible en el servidor solicitante y quiere usar sus datos en servidores remotos.

<div id="distributed-subqueries-and-max_rows_in_set">
  ### Subconsultas distribuidas y max_rows_in_set
</div>

Puede usar [`max_rows_in_set`](/es/operations/settings/settings#max_rows_in_set) y [`max_bytes_in_set`](/es/operations/settings/settings#max_bytes_in_set) para controlar qué volumen de datos se transfiere durante las consultas distribuidas.

Esto es especialmente importante si la consulta `GLOBAL IN` devuelve una gran cantidad de datos. Considere el siguiente SQL:

```sql
SELECT * FROM table1 WHERE col1 GLOBAL IN (SELECT col1 FROM table2 WHERE <some_predicate>)
```

Si `some_predicate` no es lo bastante selectivo, devolverá una gran cantidad de datos y causará problemas de rendimiento. En esos casos, conviene limitar la transferencia de datos a través de la red. Además, tenga en cuenta que [`set_overflow_mode`](/es/operations/settings/settings#set_overflow_mode) está configurado como `throw` (de forma predeterminada), lo que significa que se genera una excepción cuando se alcanzan estos umbrales.

<div id="distributed-subqueries-and-max_parallel_replicas">
  ### Subconsultas distribuidas y max_parallel_replicas
</div>

Cuando [max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas) es mayor que 1, las consultas distribuidas se transforman aún más.

Por ejemplo:

```sql
SELECT CounterID, count() FROM distributed_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS max_parallel_replicas=3
```

se convierte en lo siguiente en cada servidor:

```sql
SELECT CounterID, count() FROM local_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS parallel_replicas_count=3, parallel_replicas_offset=M
```

donde `M` está entre `1` y `3`, según la réplica en la que se ejecute la consulta local.

Estos ajustes afectan a cada tabla de la familia MergeTree de la consulta y tienen el mismo efecto que aplicar `SAMPLE 1/3 OFFSET (M-1)/3` a cada tabla.

Por lo tanto, añadir la configuración [max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas) solo producirá resultados correctos si ambas tablas tienen el mismo esquema de replicación y se muestrean por UserID o por una subclave de este. En particular, si `local_table_2` no tiene una clave de muestreo, se producirán resultados incorrectos. La misma regla se aplica a `JOIN`.

Una alternativa, si `local_table_2` no cumple los requisitos, es usar `GLOBAL IN` o `GLOBAL JOIN`.

Si una tabla no tiene una clave de muestreo, se pueden usar opciones más flexibles para [parallel&#95;replicas&#95;custom&#95;key](/es/operations/settings/settings#parallel_replicas_custom_key), que pueden ofrecer un comportamiento diferente y más óptimo.