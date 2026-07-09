---
description: 'Almacena temporalmente en RAM los datos que se van a escribir y los
  vuelca periódicamente en otra tabla. Durante la operación de lectura, los datos
  se leen del búfer y de la otra tabla simultáneamente.'
sidebar_label: 'Buffer'
sidebar_position: 120
slug: /engines/table-engines/special/buffer
title: 'Motor de tabla Buffer'
doc_type: 'reference'
---

Almacena temporalmente en RAM los datos que se van a escribir y los vuelca periódicamente en otra tabla. Durante la operación de lectura, los datos se leen del búfer y de la otra tabla simultáneamente.

:::note
Una alternativa recomendada al motor de tabla Buffer es habilitar las [inserciones asíncronas](/es/guides/best-practices/asyncinserts.md).
:::

```sql
Buffer(database, table, num_layers, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes [,flush_time [,flush_rows [,flush_bytes]]])
```

<div id="engine-parameters">
  ### Parámetros del motor
</div>

<div id="database">
  #### `database`
</div>

`database` – Nombre de la base de datos. Puede usar `currentDatabase()` u otra expresión constante que devuelva una cadena.

<div id="table">
  #### `table`
</div>

`table` – Tabla en la que se vuelcan los datos.

<div id="num_layers">
  #### `num_layers`
</div>

`num_layers` – Capa de paralelismo. Físicamente, la tabla se representará en `num_layers` búferes independientes.

<div id="min_time-max_time-min_rows-max_rows-min_bytes-and-max_bytes">
  #### `min_time`, `max_time`, `min_rows`, `max_rows`, `min_bytes`, and `max_bytes`
</div>

Condiciones para volcar los datos del búfer.

<div id="optional-engine-parameters">
  ### Parámetros opcionales del motor
</div>

<div id="flush_time-flush_rows-and-flush_bytes">
  #### `flush_time`, `flush_rows`, and `flush_bytes`
</div>

Condiciones para volcar datos del búfer en segundo plano (si se omiten o valen cero, significa que no hay parámetros `flush*`).

Los datos se vuelcan del búfer y se escriben en la tabla de destino si se cumplen todas las condiciones `min*` o al menos una condición `max*`.

Además, si se cumple al menos una condición `flush*`, se inicia un volcado en segundo plano. Esto se diferencia de `max*`, ya que `flush*` permite configurar por separado los volcados en segundo plano para evitar añadir latencia a las consultas `INSERT` en tablas Buffer.

<div id="min_time-max_time-and-flush_time">
  #### `min_time`, `max_time`, and `flush_time`
</div>

Condición para el tiempo transcurrido, en segundos, desde la primera escritura en el búfer.

<div id="min_rows-max_rows-and-flush_rows">
  #### `min_rows`, `max_rows`, and `flush_rows`
</div>

Condición para el número de filas del búfer.

<div id="min_bytes-max_bytes-and-flush_bytes">
  #### `min_bytes`, `max_bytes`, and `flush_bytes`
</div>

Condición para el número de bytes en el búfer.

Durante la operación de escritura, los datos se insertan en uno o varios búferes aleatorios (configurados con `num_layers`). O bien, si la parte de datos que se va a insertar es lo suficientemente grande (mayor que `max_rows` o `max_bytes`), se escribe directamente en la tabla de destino, omitiendo el búfer.

Las condiciones para volcar los datos se calculan por separado para cada uno de los búferes de `num_layers`. Por ejemplo, si `num_layers = 16` y `max_bytes = 100000000`, el consumo máximo de RAM es de 1,6 GB.

Ejemplo:

```sql
CREATE TABLE merge.hits_buffer AS merge.hits ENGINE = Buffer(merge, hits, 1, 10, 100, 10000, 1000000, 10000000, 100000000)
```

Creación de una tabla `merge.hits_buffer` con la misma estructura que `merge.hits` y usando el motor Buffer. Al escribir en esta tabla, los datos se almacenan en búfer en RAM y más tarde se escriben en la tabla &#39;merge.hits&#39;. Se crea un único búfer y los datos se vuelcan si se cumple cualquiera de estas condiciones:

* han pasado 100 segundos desde el último volcado (`max_time`) o
* se han escrito 1 millón de filas (`max_rows`) o
* se han escrito 100 MB de datos (`max_bytes`) o
* han pasado 10 segundos (`min_time`) y se han escrito 10.000 filas (`min_rows`) y 10 MB (`min_bytes`) de datos

Por ejemplo, si se ha escrito solo una fila, se volcará al cabo de 100 segundos, pase lo que pase. Pero si se han escrito muchas filas, los datos se volcarán antes.

Cuando el servidor se detiene, con `DROP TABLE` o `DETACH TABLE`, los datos almacenados en búfer también se vuelcan en la tabla de destino.

Puede establecer cadenas vacías entre comillas simples para la base de datos y el nombre de la tabla. Esto indica que no hay tabla de destino. En este caso, cuando se alcanzan las condiciones de volcado de datos, el búfer simplemente se limpia. Esto puede ser útil para mantener una ventana de datos en memoria.

Al leer desde una tabla Buffer, los datos se procesan tanto desde el búfer como desde la tabla de destino (si existe).
Tenga en cuenta que la tabla Buffer no admite un índice. En otras palabras, los datos del búfer se examinan por completo, lo que puede resultar lento con búferes grandes. (Para los datos de una tabla subordinada, se usará el índice que esta admita).

Si el conjunto de columnas de la tabla Buffer no coincide con el conjunto de columnas de una tabla subordinada, se inserta un subconjunto de columnas que existe en ambas tablas.

Si los tipos no coinciden en una de las columnas de la tabla Buffer y una tabla subordinada, se registra un mensaje de error en el registro del servidor y el búfer se limpia.
Lo mismo ocurre si la tabla subordinada no existe cuando se vuelca el búfer.

:::note
Ejecutar ALTER en la tabla Buffer en versiones anteriores al 26 Oct 2021 provocará un error `Block structure mismatch` (consulte [#15117](https://github.com/ClickHouse/ClickHouse/issues/15117) y [#30565](https://github.com/ClickHouse/ClickHouse/pull/30565)), por lo que eliminar la tabla Buffer y volver a crearla es la única opción. Compruebe que este error se haya corregido en su versión antes de intentar ejecutar ALTER en la tabla Buffer.
:::

Si el servidor se reinicia de forma anómala, los datos del búfer se pierden.

`FINAL` y `SAMPLE` no funcionan correctamente para las tablas Buffer. Estas condiciones se pasan a la tabla de destino, pero no se usan para procesar los datos del búfer. Si necesita estas funciones, recomendamos usar la tabla Buffer solo para escritura y leer desde la tabla de destino.

Al agregar datos a una tabla Buffer, uno de los búferes se bloquea. Esto provoca retrasos si al mismo tiempo se está realizando una operación de lectura desde la tabla.

Los datos que se insertan en una tabla Buffer pueden acabar en la tabla subordinada en un orden distinto y en bloques diferentes. Debido a esto, es difícil usar una tabla Buffer para escribir correctamente en un CollapsingMergeTree. Para evitar problemas, puede establecer `num_layers` en 1.

Si la tabla de destino está replicada, se pierden algunas características esperadas de las tablas replicadas al escribir en una tabla Buffer. Los cambios aleatorios en el orden de las filas y en los tamaños de las partes de datos hacen que la deduplicación deje de funcionar, lo que significa que no es posible tener una escritura fiable de &#39;exactly once&#39; en tablas replicadas.

Debido a estas desventajas, solo podemos recomendar el uso de una tabla Buffer en casos excepcionales.

Una tabla Buffer se usa cuando se reciben demasiados INSERT de un gran número de servidores en un periodo de tiempo y los datos no pueden almacenarse en búfer antes de la inserción, lo que significa que los INSERT no pueden ejecutarse con la suficiente rapidez.

Ten en cuenta que no tiene sentido insertar datos fila por fila, ni siquiera en las tablas Buffer. Esto solo dará una velocidad de unos pocos miles de filas por segundo, mientras que insertar bloques de datos más grandes puede superar el millón de filas por segundo.