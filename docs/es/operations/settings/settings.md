---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# Configuración {#settings}

## distributed\_product\_mode {#distributed-product-mode}

Cambia el comportamiento de [subconsultas distribuidas](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

Restricción:

-   Solo se aplica para las subconsultas IN y JOIN.
-   Solo si la sección FROM utiliza una tabla distribuida que contiene más de un fragmento.
-   Si la subconsulta se refiere a una tabla distribuida que contiene más de un fragmento.
-   No se usa para un valor de tabla [remoto](../../sql-reference/table-functions/remote.md) función.

Valores posibles:

-   `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” salvedad).
-   `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
-   `global` — Replaces the `IN`/`JOIN` consulta con `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — Allows the use of these types of subqueries.

## enable\_optimize\_predicate\_expression {#enable-optimize-predicate-expression}

Activa el pushdown de predicado en `SELECT` consulta.

La extracción de predicados puede reducir significativamente el tráfico de red para consultas distribuidas.

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 1.

Uso

Considere las siguientes consultas:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

Si `enable_optimize_predicate_expression = 1`, entonces el tiempo de ejecución de estas consultas es igual porque se aplica ClickHouse `WHERE` a la subconsulta al procesarla.

Si `enable_optimize_predicate_expression = 0`, entonces el tiempo de ejecución de la segunda consulta es mucho más largo, porque el `WHERE` cláusula se aplica a todos los datos después de que finalice la subconsulta.

## fallback\_to\_stale\_replicas\_for\_distributed\_queries {#settings-fallback_to_stale_replicas_for_distributed_queries}

Fuerza una consulta a una réplica obsoleta si los datos actualizados no están disponibles. Ver [Replicación](../../engines/table-engines/mergetree-family/replication.md).

ClickHouse selecciona la más relevante de las réplicas obsoletas de la tabla.

Se utiliza al realizar `SELECT` desde una tabla distribuida que apunta a tablas replicadas.

De forma predeterminada, 1 (habilitado).

## Fecha de nacimiento {#settings-force_index_by_date}

Deshabilita la ejecución de consultas si el índice no se puede usar por fecha.

Funciona con tablas de la familia MergeTree.

Si `force_index_by_date=1`, ClickHouse comprueba si la consulta tiene una condición de clave de fecha que se puede usar para restringir intervalos de datos. Si no hay una condición adecuada, arroja una excepción. Sin embargo, no comprueba si la condición reduce la cantidad de datos a leer. Por ejemplo, la condición `Date != ' 2000-01-01 '` es aceptable incluso cuando coincide con todos los datos de la tabla (es decir, ejecutar la consulta requiere un escaneo completo). Para obtener más información acerca de los intervalos de datos en las tablas MergeTree, vea [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md).

## force\_primary\_key {#force-primary-key}

Deshabilita la ejecución de consultas si no es posible la indexación mediante la clave principal.

Funciona con tablas de la familia MergeTree.

Si `force_primary_key=1`, ClickHouse comprueba si la consulta tiene una condición de clave principal que se puede usar para restringir rangos de datos. Si no hay una condición adecuada, arroja una excepción. Sin embargo, no comprueba si la condición reduce la cantidad de datos a leer. Para obtener más información acerca de los intervalos de datos en las tablas MergeTree, consulte [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md).

## Formato\_esquema {#format-schema}

Este parámetro es útil cuando se utilizan formatos que requieren una definición de esquema, como [Cap'n Proto](https://capnproto.org/) o [Protobuf](https://developers.google.com/protocol-buffers/). El valor depende del formato.

## fsync\_metadata {#fsync-metadata}

Habilita o deshabilita [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) al escribir `.sql` file. Habilitado de forma predeterminada.

Tiene sentido desactivarlo si el servidor tiene millones de pequeñas tablas que se crean y destruyen constantemente.

## enable\_http\_compression {#settings-enable_http_compression}

Habilita o deshabilita la compresión de datos en la respuesta a una solicitud HTTP.

Para obtener más información, lea el [Descripción de la interfaz HTTP](../../interfaces/http.md).

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 0.

## http\_zlib\_compression\_level {#settings-http_zlib_compression_level}

Establece el nivel de compresión de datos en la respuesta a una solicitud HTTP si [enable\_http\_compression = 1](#settings-enable_http_compression).

Valores posibles: Números del 1 al 9.

Valor predeterminado: 3.

## http\_native\_compression\_disable\_checksumming\_on\_decompress {#settings-http_native_compression_disable_checksumming_on_decompress}

Habilita o deshabilita la verificación de suma de comprobación al descomprimir los datos HTTP POST del cliente. Se usa solo para el formato de compresión nativa ClickHouse (no se usa con `gzip` o `deflate`).

Para obtener más información, lea el [Descripción de la interfaz HTTP](../../interfaces/http.md).

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 0.

## send\_progress\_in\_http\_headers {#settings-send_progress_in_http_headers}

Habilita o deshabilita `X-ClickHouse-Progress` Encabezados de respuesta HTTP en `clickhouse-server` respuesta.

Para obtener más información, lea el [Descripción de la interfaz HTTP](../../interfaces/http.md).

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 0.

## Nombre de la red inalámbrica (SSID): {#setting-max_http_get_redirects}

Limita el número máximo de saltos de redirección HTTP GET para [URL](../../engines/table-engines/special/url.md)-mesas de motor. La configuración se aplica a ambos tipos de tablas: las creadas por [CREATE TABLE](../../sql-reference/statements/create.md#create-table-query) consulta y por el [URL](../../sql-reference/table-functions/url.md) función de la tabla.

Valores posibles:

-   Cualquier número entero positivo de saltos.
-   0 — No hops allowed.

Valor predeterminado: 0.

## Entrada\_format\_allow\_errors\_num {#settings-input_format_allow_errors_num}

Establece el número máximo de errores aceptables al leer desde formatos de texto (CSV, TSV, etc.).

El valor predeterminado es 0.

Siempre emparejarlo con `input_format_allow_errors_ratio`.

Si se produjo un error al leer filas, pero el contador de errores sigue siendo menor que `input_format_allow_errors_num`, ClickHouse ignora la fila y pasa a la siguiente.

Si ambos `input_format_allow_errors_num` y `input_format_allow_errors_ratio` se exceden, ClickHouse lanza una excepción.

## Entrada\_format\_allow\_errors\_ratio {#settings-input_format_allow_errors_ratio}

Establece el porcentaje máximo de errores permitidos al leer desde formatos de texto (CSV, TSV, etc.).
El porcentaje de errores se establece como un número de punto flotante entre 0 y 1.

El valor predeterminado es 0.

Siempre emparejarlo con `input_format_allow_errors_num`.

Si se produjo un error al leer filas, pero el contador de errores sigue siendo menor que `input_format_allow_errors_ratio`, ClickHouse ignora la fila y pasa a la siguiente.

Si ambos `input_format_allow_errors_num` y `input_format_allow_errors_ratio` se exceden, ClickHouse lanza una excepción.

## input\_format\_values\_interpret\_expressions {#settings-input_format_values_interpret_expressions}

Habilita o deshabilita el analizador SQL completo si el analizador de secuencias rápidas no puede analizar los datos. Esta configuración sólo se utiliza para [Valor](../../interfaces/formats.md#data-format-values) formato en la inserción de datos. Para obtener más información sobre el análisis de sintaxis, consulte [Sintaxis](../../sql-reference/syntax.md) apartado.

Valores posibles:

-   0 — Disabled.

    En este caso, debe proporcionar datos con formato. Ver el [Formato](../../interfaces/formats.md) apartado.

-   1 — Enabled.

    En este caso, puede usar una expresión SQL como valor, pero la inserción de datos es mucho más lenta de esta manera. Si inserta solo datos con formato, ClickHouse se comporta como si el valor de configuración fuera 0.

Valor predeterminado: 1.

Ejemplo de uso

Inserte el [FechaHora](../../sql-reference/data-types/datetime.md) valor de tipo con los diferentes ajustes.

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t VALUES (now())
```

``` text
Exception on client:
Code: 27. DB::Exception: Cannot parse input: expected ) before: now()): (at row 1)
```

``` sql
SET input_format_values_interpret_expressions = 1;
INSERT INTO datetime_t VALUES (now())
```

``` text
Ok.
```

La última consulta es equivalente a la siguiente:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

## input\_format\_values\_deduce\_templates\_of\_expressions {#settings-input_format_values_deduce_templates_of_expressions}

Habilita o deshabilita la deducción de plantilla para expresiones SQL en [Valor](../../interfaces/formats.md#data-format-values) formato. Permite analizar e interpretar expresiones en `Values` mucho más rápido si las expresiones en filas consecutivas tienen la misma estructura. ClickHouse intenta deducir la plantilla de una expresión, analizar las siguientes filas utilizando esta plantilla y evaluar la expresión en un lote de filas analizadas correctamente.

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 1.

Para la siguiente consulta:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   Si `input_format_values_interpret_expressions=1` y `format_values_deduce_templates_of_expressions=0`, las expresiones se interpretan por separado para cada fila (esto es muy lento para un gran número de filas).
-   Si `input_format_values_interpret_expressions=0` y `format_values_deduce_templates_of_expressions=1`, las expresiones en la primera, segunda y tercera filas se analizan usando la plantilla `lower(String)` e interpretados juntos, la expresión en la cuarta fila se analiza con otra plantilla (`upper(String)`).
-   Si `input_format_values_interpret_expressions=1` y `format_values_deduce_templates_of_expressions=1`, lo mismo que en el caso anterior, pero también permite la alternativa a la interpretación de expresiones por separado si no es posible deducir la plantilla.

## Entrada\_format\_values\_accurate\_types\_of\_literals {#settings-input-format-values-accurate-types-of-literals}

Esta configuración sólo se utiliza cuando `input_format_values_deduce_templates_of_expressions = 1`. Puede suceder que las expresiones para alguna columna tengan la misma estructura, pero contengan literales numéricos de diferentes tipos, por ejemplo

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

Valores posibles:

-   0 — Disabled.

    In this case, ClickHouse may use a more general type for some literals (e.g., `Float64` o `Int64` en lugar de `UInt64` para `42`), pero puede causar problemas de desbordamiento y precisión.

-   1 — Enabled.

    En este caso, ClickHouse comprueba el tipo real de literal y utiliza una plantilla de expresión del tipo correspondiente. En algunos casos, puede ralentizar significativamente la evaluación de expresiones en `Values`.

Valor predeterminado: 1.

## Entrada\_format\_defaults\_for\_omitted\_fields {#session_settings-input_format_defaults_for_omitted_fields}

Al realizar `INSERT` consultas, reemplace los valores de columna de entrada omitidos con valores predeterminados de las columnas respectivas. Esta opción sólo se aplica a [JSONEachRow](../../interfaces/formats.md#jsoneachrow), [CSV](../../interfaces/formats.md#csv) y [TabSeparated](../../interfaces/formats.md#tabseparated) formato.

!!! note "Nota"
    Cuando esta opción está habilitada, los metadatos de la tabla extendida se envían del servidor al cliente. Consume recursos informáticos adicionales en el servidor y puede reducir el rendimiento.

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 1.

## input\_format\_tsv\_empty\_as\_default {#settings-input-format-tsv-empty-as-default}

Cuando esté habilitado, reemplace los campos de entrada vacíos en TSV con valores predeterminados. Para expresiones predeterminadas complejas `input_format_defaults_for_omitted_fields` debe estar habilitado también.

Deshabilitado de forma predeterminada.

## input\_format\_null\_as\_default {#settings-input-format-null-as-default}

Habilita o deshabilita el uso de valores predeterminados si los datos de entrada `NULL`, pero el tipo de datos de la columna correspondiente en no `Nullable(T)` (para formatos de entrada de texto).

## input\_format\_skip\_unknown\_fields {#settings-input-format-skip-unknown-fields}

Habilita o deshabilita omitir la inserción de datos adicionales.

Al escribir datos, ClickHouse produce una excepción si los datos de entrada contienen columnas que no existen en la tabla de destino. Si la omisión está habilitada, ClickHouse no inserta datos adicionales y no lanza una excepción.

Formatos soportados:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)
-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)
-   [TSKV](../../interfaces/formats.md#tskv)

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 0.

## Entrada\_format\_import\_nested\_json {#settings-input_format_import_nested_json}

Habilita o deshabilita la inserción de datos JSON con objetos anidados.

Formatos soportados:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 0.

Ver también:

-   [Uso de estructuras anidadas](../../interfaces/formats.md#jsoneachrow-nested) con el `JSONEachRow` formato.

## Entrada\_format\_with\_names\_use\_header {#settings-input-format-with-names-use-header}

Habilita o deshabilita la comprobación del orden de las columnas al insertar datos.

Para mejorar el rendimiento de la inserción, se recomienda deshabilitar esta comprobación si está seguro de que el orden de columna de los datos de entrada es el mismo que en la tabla de destino.

Formatos soportados:

-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 1.

## Date\_time\_input\_format {#settings-date_time_input_format}

Permite elegir un analizador de la representación de texto de fecha y hora.

La configuración no se aplica a [Funciones de fecha y hora](../../sql-reference/functions/date-time-functions.md).

Valores posibles:

-   `'best_effort'` — Enables extended parsing.

    ClickHouse puede analizar el básico `YYYY-MM-DD HH:MM:SS` formato y todo [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) formatos de fecha y hora. Por ejemplo, `'2018-06-08T01:02:03.000Z'`.

-   `'basic'` — Use basic parser.

    ClickHouse puede analizar solo lo básico `YYYY-MM-DD HH:MM:SS` formato. Por ejemplo, `'2019-08-20 10:18:56'`.

Valor predeterminado: `'basic'`.

Ver también:

-   [Tipo de datos DateTime.](../../sql-reference/data-types/datetime.md)
-   [Funciones para trabajar con fechas y horas.](../../sql-reference/functions/date-time-functions.md)

## Por favor, introduzca su dirección de correo electrónico {#settings-join_default_strictness}

Establece el rigor predeterminado para [Cláusulas JOIN](../../sql-reference/statements/select/join.md#select-join).

Valores posibles:

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Producto cartesiano](https://en.wikipedia.org/wiki/Cartesian_product) de filas coincidentes. Esta es la normal `JOIN` comportamiento de SQL estándar.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` y `ALL` son los mismos.
-   `ASOF` — For joining sequences with an uncertain match.
-   `Empty string` — If `ALL` o `ANY` no se especifica en la consulta, ClickHouse produce una excepción.

Valor predeterminado: `ALL`.

## join\_any\_take\_last\_row {#settings-join_any_take_last_row}

Cambia el comportamiento de las operaciones de unión con `ANY` rigor.

!!! warning "Atención"
    Esta configuración sólo se aplica a `JOIN` operaciones con [Unir](../../engines/table-engines/special/join.md) mesas de motores.

Valores posibles:

-   0 — If the right table has more than one matching row, only the first one found is joined.
-   1 — If the right table has more than one matching row, only the last one found is joined.

Valor predeterminado: 0.

Ver también:

-   [Cláusula JOIN](../../sql-reference/statements/select/join.md#select-join)
-   [Unirse al motor de tabla](../../engines/table-engines/special/join.md)
-   [Por favor, introduzca su dirección de correo electrónico](#settings-join_default_strictness)

## Sistema abierto {#join_use_nulls}

Establece el tipo de [JOIN](../../sql-reference/statements/select/join.md) comportamiento. Al fusionar tablas, pueden aparecer celdas vacías. ClickHouse los rellena de manera diferente según esta configuración.

Valores posibles:

-   0 — The empty cells are filled with the default value of the corresponding field type.
-   1 — `JOIN` se comporta de la misma manera que en SQL estándar. El tipo del campo correspondiente se convierte en [NULL](../../sql-reference/data-types/nullable.md#data_type-nullable), y las celdas vacías se llenan con [NULL](../../sql-reference/syntax.md).

Valor predeterminado: 0.

## max\_block\_size {#setting-max_block_size}

En ClickHouse, los datos se procesan mediante bloques (conjuntos de partes de columna). Los ciclos de procesamiento interno para un solo bloque son lo suficientemente eficientes, pero hay gastos notables en cada bloque. El `max_block_size` set es una recomendación para el tamaño del bloque (en un recuento de filas) para cargar desde las tablas. El tamaño del bloque no debe ser demasiado pequeño, por lo que los gastos en cada bloque aún se notan, pero no demasiado grande para que la consulta con LIMIT que se complete después del primer bloque se procese rápidamente. El objetivo es evitar consumir demasiada memoria al extraer un gran número de columnas en múltiples subprocesos y preservar al menos alguna localidad de caché.

Valor predeterminado: 65,536.

Bloquea el tamaño de `max_block_size` no siempre se cargan desde la tabla. Si es obvio que se deben recuperar menos datos, se procesa un bloque más pequeño.

## preferred\_block\_size\_bytes {#preferred-block-size-bytes}

Utilizado para el mismo propósito que `max_block_size`, pero establece el tamaño de bloque recomendado en bytes adaptándolo al número de filas en el bloque.
Sin embargo, el tamaño del bloque no puede ser más que `max_block_size` filas.
Por defecto: 1,000,000. Solo funciona cuando se lee desde los motores MergeTree.

## merge\_tree\_min\_rows\_for\_concurrent\_read {#setting-merge-tree-min-rows-for-concurrent-read}

Si el número de filas que se leerán de un fichero [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md) mesa excede `merge_tree_min_rows_for_concurrent_read` luego ClickHouse intenta realizar una lectura simultánea de este archivo en varios hilos.

Valores posibles:

-   Cualquier entero positivo.

Valor predeterminado: 163840.

## merge\_tree\_min\_bytes\_for\_concurrent\_read {#setting-merge-tree-min-bytes-for-concurrent-read}

Si el número de bytes a leer de un archivo de un [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md)-La tabla del motor excede `merge_tree_min_bytes_for_concurrent_read`, entonces ClickHouse intenta leer simultáneamente este archivo en varios subprocesos.

Valor posible:

-   Cualquier entero positivo.

Valor predeterminado: 251658240.

## Método de codificación de datos: {#setting-merge-tree-min-rows-for-seek}

Si la distancia entre dos bloques de datos que se leen en un archivo es menor que `merge_tree_min_rows_for_seek` filas, luego ClickHouse no busca a través del archivo, sino que lee los datos secuencialmente.

Valores posibles:

-   Cualquier entero positivo.

Valor predeterminado: 0.

## merge\_tree\_min\_bytes\_for\_seek {#setting-merge-tree-min-bytes-for-seek}

Si la distancia entre dos bloques de datos que se leen en un archivo es menor que `merge_tree_min_bytes_for_seek` bytes, luego ClickHouse lee secuencialmente un rango de archivos que contiene ambos bloques, evitando así la búsqueda adicional.

Valores posibles:

-   Cualquier entero positivo.

Valor predeterminado: 0.

## merge\_tree\_coarse\_index\_granularity {#setting-merge-tree-coarse-index-granularity}

Al buscar datos, ClickHouse comprueba las marcas de datos en el archivo de índice. Si ClickHouse encuentra que las claves requeridas están en algún rango, divide este rango en `merge_tree_coarse_index_granularity` subintervalos y busca las claves necesarias allí de forma recursiva.

Valores posibles:

-   Cualquier entero incluso positivo.

Valor predeterminado: 8.

## merge\_tree\_max\_rows\_to\_use\_cache {#setting-merge-tree-max-rows-to-use-cache}

Si ClickHouse debería leer más de `merge_tree_max_rows_to_use_cache` en una consulta, no usa la memoria caché de bloques sin comprimir.

La memoria caché de bloques sin comprimir almacena datos extraídos para consultas. ClickHouse utiliza esta memoria caché para acelerar las respuestas a pequeñas consultas repetidas. Esta configuración protege la memoria caché del deterioro de las consultas que leen una gran cantidad de datos. El [Uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuración del servidor define el tamaño de la memoria caché de bloques sin comprimir.

Valores posibles:

-   Cualquier entero positivo.

Default value: 128 ✕ 8192.

## merge\_tree\_max\_bytes\_to\_use\_cache {#setting-merge-tree-max-bytes-to-use-cache}

Si ClickHouse debería leer más de `merge_tree_max_bytes_to_use_cache` bytes en una consulta, no usa el caché de bloques sin comprimir.

La memoria caché de bloques sin comprimir almacena datos extraídos para consultas. ClickHouse utiliza esta memoria caché para acelerar las respuestas a pequeñas consultas repetidas. Esta configuración protege la memoria caché del deterioro de las consultas que leen una gran cantidad de datos. El [Uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuración del servidor define el tamaño de la memoria caché de bloques sin comprimir.

Valor posible:

-   Cualquier entero positivo.

Valor predeterminado: 2013265920.

## Todos los derechos reservados {#settings-min-bytes-to-use-direct-io}

El volumen de datos mínimo necesario para utilizar el acceso directo de E/S al disco de almacenamiento.

ClickHouse usa esta configuración al leer datos de tablas. Si el volumen total de almacenamiento de todos los datos a leer excede `min_bytes_to_use_direct_io` luego ClickHouse lee los datos del disco de almacenamiento con el `O_DIRECT` opcion.

Valores posibles:

-   0 — Direct I/O is disabled.
-   Entero positivo.

Valor predeterminado: 0.

## Log\_queries {#settings-log-queries}

Configuración del registro de consultas.

Las consultas enviadas a ClickHouse con esta configuración se registran de acuerdo con las reglas [query\_log](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log) parámetro de configuración del servidor.

Ejemplo:

``` text
log_queries=1
```

## Nombre de la red inalámbrica (SSID): {#settings-log-queries-min-type}

`query_log` tipo mínimo para iniciar sesión.

Valores posibles:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

Valor predeterminado: `QUERY_START`.

Se puede usar para limitar a qué entiries va `query_log`, digamos que eres interesante solo en errores, entonces puedes usar `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```

## Log\_query\_threads {#settings-log-query-threads}

Configuración del registro de subprocesos de consulta.

Los subprocesos de consultas ejecutados por ClickHouse con esta configuración se registran de acuerdo con las reglas en el [Sistema abierto.](../server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) parámetro de configuración del servidor.

Ejemplo:

``` text
log_query_threads=1
```

## Max\_insert\_block\_size {#settings-max_insert_block_size}

El tamaño de los bloques a formar para su inserción en una tabla.
Esta configuración solo se aplica en los casos en que el servidor forma los bloques.
Por ejemplo, para un INSERT a través de la interfaz HTTP, el servidor analiza el formato de datos y forma bloques del tamaño especificado.
Pero al usar clickhouse-client, el cliente analiza los datos en sí, y el ‘max\_insert\_block\_size’ configuración en el servidor no afecta el tamaño de los bloques insertados.
La configuración tampoco tiene un propósito cuando se usa INSERT SELECT , ya que los datos se insertan usando los mismos bloques que se forman después de SELECT .

Valor predeterminado: 1.048.576.

El valor predeterminado es ligeramente más que `max_block_size`. La razón de esto se debe a que ciertos motores de mesa (`*MergeTree`) formar una parte de datos en el disco para cada bloque insertado, que es una entidad bastante grande. Similar, `*MergeTree` las tablas ordenan los datos durante la inserción y un tamaño de bloque lo suficientemente grande permiten clasificar más datos en la RAM.

## Nombre de la red inalámbrica (SSID): {#min-insert-block-size-rows}

Establece el número mínimo de filas en el bloque que se pueden insertar en una tabla `INSERT` consulta. Los bloques de menor tamaño se aplastan en otros más grandes.

Valores posibles:

-   Entero positivo.
-   0 — Squashing disabled.

Valor predeterminado: 1048576.

## Todos los derechos reservados {#min-insert-block-size-bytes}

Establece el número mínimo de bytes en el bloque que se pueden insertar en una tabla `INSERT` consulta. Los bloques de menor tamaño se aplastan en otros más grandes.

Valores posibles:

-   Entero positivo.
-   0 — Squashing disabled.

Valor predeterminado: 268435456.

## max\_replica\_delay\_for\_distributed\_queries {#settings-max_replica_delay_for_distributed_queries}

Deshabilita las réplicas rezagadas para consultas distribuidas. Ver [Replicación](../../engines/table-engines/mergetree-family/replication.md).

Establece el tiempo en segundos. Si una réplica tiene un retraso superior al valor establecido, no se utiliza esta réplica.

Valor predeterminado: 300.

Se utiliza al realizar `SELECT` desde una tabla distribuida que apunta a tablas replicadas.

## max\_threads {#settings-max_threads}

El número máximo de subprocesos de procesamiento de consultas, excluyendo subprocesos para recuperar datos de servidores ‘max\_distributed\_connections’ parámetro).

Este parámetro se aplica a los subprocesos que realizan las mismas etapas de la canalización de procesamiento de consultas en paralelo.
Por ejemplo, al leer desde una tabla, si es posible evaluar expresiones con funciones, filtre con WHERE y preagregue para GROUP BY en paralelo usando al menos ‘max\_threads’ número de hilos, entonces ‘max\_threads’ se utilizan.

Valor predeterminado: el número de núcleos de CPU físicos.

Si normalmente se ejecuta menos de una consulta SELECT en un servidor a la vez, establezca este parámetro en un valor ligeramente inferior al número real de núcleos de procesador.

Para las consultas que se completan rápidamente debido a un LIMIT, puede establecer un ‘max\_threads’. Por ejemplo, si el número necesario de entradas se encuentra en cada bloque y max\_threads = 8, entonces se recuperan 8 bloques, aunque hubiera sido suficiente leer solo uno.

Cuanto menor sea el `max_threads` valor, menos memoria se consume.

## Método de codificación de datos: {#settings-max-insert-threads}

El número máximo de subprocesos para ejecutar el `INSERT SELECT` consulta.

Valores posibles:

-   0 (or 1) — `INSERT SELECT` sin ejecución paralela.
-   Entero positivo. Más grande que 1.

Valor predeterminado: 0.

Paralelo `INSERT SELECT` sólo tiene efecto si el `SELECT` parte se ejecuta en paralelo, ver [max\_threads](#settings-max_threads) configuración.
Los valores más altos conducirán a un mayor uso de memoria.

## max\_compress\_block\_size {#max-compress-block-size}

El tamaño máximo de bloques de datos sin comprimir antes de comprimir para escribir en una tabla. De forma predeterminada, 1.048.576 (1 MiB). Si se reduce el tamaño, la tasa de compresión se reduce significativamente, la velocidad de compresión y descompresión aumenta ligeramente debido a la localidad de la memoria caché, y se reduce el consumo de memoria. Por lo general, no hay ninguna razón para cambiar esta configuración.

No confunda bloques para la compresión (un fragmento de memoria que consta de bytes) con bloques para el procesamiento de consultas (un conjunto de filas de una tabla).

## Descripción del producto {#min-compress-block-size}

Para [Método de codificación de datos:](../../engines/table-engines/mergetree-family/mergetree.md)" tabla. Para reducir la latencia al procesar consultas, un bloque se comprime al escribir la siguiente marca si su tamaño es al menos ‘min\_compress\_block\_size’. De forma predeterminada, 65.536.

El tamaño real del bloque, si los datos sin comprimir son menores que ‘max\_compress\_block\_size’, no es menor que este valor y no menor que el volumen de datos para una marca.

Veamos un ejemplo. Supongamos que ‘index\_granularity’ se estableció en 8192 durante la creación de la tabla.

Estamos escribiendo una columna de tipo UInt32 (4 bytes por valor). Al escribir 8192 filas, el total será de 32 KB de datos. Como min\_compress\_block\_size = 65,536, se formará un bloque comprimido por cada dos marcas.

Estamos escribiendo una columna URL con el tipo String (tamaño promedio de 60 bytes por valor). Al escribir 8192 filas, el promedio será ligeramente inferior a 500 KB de datos. Como esto es más de 65,536, se formará un bloque comprimido para cada marca. En este caso, al leer datos del disco en el rango de una sola marca, los datos adicionales no se descomprimirán.

Por lo general, no hay ninguna razón para cambiar esta configuración.

## max\_query\_size {#settings-max_query_size}

La parte máxima de una consulta que se puede llevar a la RAM para analizar con el analizador SQL.
La consulta INSERT también contiene datos para INSERT que es procesado por un analizador de secuencias independiente (que consume O(1) RAM), que no está incluido en esta restricción.

Valor predeterminado: 256 KiB.

## interactive\_delay {#interactive-delay}

El intervalo en microsegundos para comprobar si la ejecución de la solicitud se ha cancelado y enviar el progreso.

Valor predeterminado: 100.000 (comprueba la cancelación y envía el progreso diez veces por segundo).

## ¿Cómo puedo hacerlo? {#connect-timeout-receive-timeout-send-timeout}

Tiempos de espera en segundos en el socket utilizado para comunicarse con el cliente.

Valor predeterminado: 10, 300, 300.

## Cancel\_http\_readonly\_queries\_on\_client\_close {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

Valor predeterminado: 0

## poll\_interval {#poll-interval}

Bloquear en un bucle de espera durante el número especificado de segundos.

Valor predeterminado: 10.

## max\_distributed\_connections {#max-distributed-connections}

El número máximo de conexiones simultáneas con servidores remotos para el procesamiento distribuido de una única consulta a una única tabla distribuida. Se recomienda establecer un valor no menor que el número de servidores en el clúster.

Valor predeterminado: 1024.

Los siguientes parámetros solo se usan al crear tablas distribuidas (y al iniciar un servidor), por lo que no hay ninguna razón para cambiarlas en tiempo de ejecución.

## Distributed\_connections\_pool\_size {#distributed-connections-pool-size}

El número máximo de conexiones simultáneas con servidores remotos para el procesamiento distribuido de todas las consultas a una única tabla distribuida. Se recomienda establecer un valor no menor que el número de servidores en el clúster.

Valor predeterminado: 1024.

## Conecte\_timeout\_with\_failover\_ms {#connect-timeout-with-failover-ms}

El tiempo de espera en milisegundos para conectarse a un servidor remoto para un motor de tablas distribuidas ‘shard’ y ‘replica’ secciones se utilizan en la definición de clúster.
Si no tiene éxito, se realizan varios intentos para conectarse a varias réplicas.

Valor predeterminado: 50.

## connections\_with\_failover\_max\_tries {#connections-with-failover-max-tries}

El número máximo de intentos de conexión con cada réplica para el motor de tablas distribuidas.

Valor predeterminado: 3.

## extremo {#extremes}

Ya sea para contar valores extremos (los mínimos y máximos en columnas de un resultado de consulta). Acepta 0 o 1. De forma predeterminada, 0 (deshabilitado).
Para obtener más información, consulte la sección “Extreme values”.

## Use\_uncompressed\_cache {#setting-use_uncompressed_cache}

Si se debe usar una memoria caché de bloques sin comprimir. Acepta 0 o 1. De forma predeterminada, 0 (deshabilitado).
El uso de la memoria caché sin comprimir (solo para tablas de la familia MergeTree) puede reducir significativamente la latencia y aumentar el rendimiento cuando se trabaja con un gran número de consultas cortas. Habilite esta configuración para los usuarios que envían solicitudes cortas frecuentes. También preste atención al [Uncompressed\_cache\_size](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

Para consultas que leen al menos un volumen algo grande de datos (un millón de filas o más), la memoria caché sin comprimir se desactiva automáticamente para ahorrar espacio para consultas realmente pequeñas. Esto significa que puede mantener el ‘use\_uncompressed\_cache’ ajuste siempre establecido en 1.

## Reemplazar\_running\_query {#replace-running-query}

Cuando se utiliza la interfaz HTTP, el ‘query\_id’ parámetro puede ser pasado. Se trata de cualquier cadena que sirva como identificador de consulta.
Si una consulta del mismo usuario ‘query\_id’ que ya existe en este momento, el comportamiento depende de la ‘replace\_running\_query’ parámetro.

`0` (default) – Throw an exception (don't allow the query to run if a query with the same ‘query\_id’ ya se está ejecutando).

`1` – Cancel the old query and start running the new one.

El Yandex.Metrica utiliza este parámetro establecido en 1 para implementar sugerencias para las condiciones de segmentación. Después de ingresar el siguiente carácter, si la consulta anterior aún no ha finalizado, debe cancelarse.

## Nombre de la red inalámbrica (SSID): {#stream-flush-interval-ms}

Funciona para tablas con streaming en el caso de un tiempo de espera, o cuando un subproceso genera [Max\_insert\_block\_size](#settings-max_insert_block_size) filas.

El valor predeterminado es 7500.

Cuanto menor sea el valor, más a menudo los datos se vacían en la tabla. Establecer el valor demasiado bajo conduce a un rendimiento deficiente.

## load\_balancing {#settings-load_balancing}

Especifica el algoritmo de selección de réplicas que se utiliza para el procesamiento de consultas distribuidas.

ClickHouse admite los siguientes algoritmos para elegir réplicas:

-   [Aleatorio](#load_balancing-random) (predeterminada)
-   [Nombre de host más cercano](#load_balancing-nearest_hostname)
-   [En orden](#load_balancing-in_order)
-   [Primero o aleatorio](#load_balancing-first_or_random)

### Aleatorio (por defecto) {#load_balancing-random}

``` sql
load_balancing = random
```

El número de errores se cuenta para cada réplica. La consulta se envía a la réplica con el menor número de errores, y si hay varios de estos, a cualquiera de ellos.
Desventajas: La proximidad del servidor no se tiene en cuenta; si las réplicas tienen datos diferentes, también obtendrá datos diferentes.

### Nombre de host más cercano {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server's hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

Por ejemplo, example01-01-1 y example01-01-2.yandex.ru son diferentes en una posición, mientras que example01-01-1 y example01-02-2 difieren en dos lugares.
Este método puede parecer primitivo, pero no requiere datos externos sobre la topología de red, y no compara las direcciones IP, lo que sería complicado para nuestras direcciones IPv6.

Por lo tanto, si hay réplicas equivalentes, se prefiere la más cercana por nombre.
También podemos suponer que al enviar una consulta al mismo servidor, en ausencia de fallas, una consulta distribuida también irá a los mismos servidores. Por lo tanto, incluso si se colocan datos diferentes en las réplicas, la consulta devolverá principalmente los mismos resultados.

### En orden {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

Se accede a las réplicas con el mismo número de errores en el mismo orden en que se especifican en la configuración.
Este método es apropiado cuando se sabe exactamente qué réplica es preferible.

### Primero o aleatorio {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

Este algoritmo elige la primera réplica del conjunto o una réplica aleatoria si la primera no está disponible. Es efectivo en configuraciones de topología de replicación cruzada, pero inútil en otras configuraciones.

El `first_or_random` resuelve el problema del algoritmo `in_order` algoritmo. Con `in_order`, si una réplica se cae, la siguiente obtiene una carga doble mientras que las réplicas restantes manejan la cantidad habitual de tráfico. Cuando se utiliza el `first_or_random` algoritmo, la carga se distribuye uniformemente entre las réplicas que todavía están disponibles.

## prefer\_localhost\_replica {#settings-prefer-localhost-replica}

Habilita/deshabilita el uso preferible de la réplica localhost al procesar consultas distribuidas.

Valores posibles:

-   1 — ClickHouse always sends a query to the localhost replica if it exists.
-   0 — ClickHouse uses the balancing strategy specified by the [load\_balancing](#settings-load_balancing) configuración.

Valor predeterminado: 1.

!!! warning "Advertencia"
    Deshabilite esta configuración si usa [max\_parallel\_replicas](#settings-max_parallel_replicas).

## totals\_mode {#totals-mode}

Cómo calcular TOTALS cuando HAVING está presente, así como cuando max\_rows\_to\_group\_by y group\_by\_overflow\_mode = ‘any’ están presentes.
Vea la sección “WITH TOTALS modifier”.

## totals\_auto\_threshold {#totals-auto-threshold}

El umbral para `totals_mode = 'auto'`.
Vea la sección “WITH TOTALS modifier”.

## max\_parallel\_replicas {#settings-max_parallel_replicas}

El número máximo de réplicas para cada fragmento al ejecutar una consulta.
Para obtener coherencia (para obtener diferentes partes de la misma división de datos), esta opción solo funciona cuando se establece la clave de muestreo.
El retraso de réplica no está controlado.

## compilar {#compile}

Habilitar la compilación de consultas. De forma predeterminada, 0 (deshabilitado).

La compilación solo se usa para parte de la canalización de procesamiento de consultas: para la primera etapa de agregación (GROUP BY).
Si se compiló esta parte de la canalización, la consulta puede ejecutarse más rápido debido a la implementación de ciclos cortos y a las llamadas de función agregadas en línea. La mejora del rendimiento máximo (hasta cuatro veces más rápido en casos excepcionales) se ve para consultas con múltiples funciones agregadas simples. Por lo general, la ganancia de rendimiento es insignificante. En casos muy raros, puede ralentizar la ejecución de la consulta.

## min\_count\_to\_compile {#min-count-to-compile}

¿Cuántas veces usar potencialmente un fragmento de código compilado antes de ejecutar la compilación? Por defecto, 3.
For testing, the value can be set to 0: compilation runs synchronously and the query waits for the end of the compilation process before continuing execution. For all other cases, use values ​​starting with 1. Compilation normally takes about 5-10 seconds.
Si el valor es 1 o más, la compilación se produce de forma asíncrona en un subproceso independiente. El resultado se utilizará tan pronto como esté listo, incluidas las consultas que se están ejecutando actualmente.

Se requiere código compilado para cada combinación diferente de funciones agregadas utilizadas en la consulta y el tipo de claves en la cláusula GROUP BY.
The results of the compilation are saved in the build directory in the form of .so files. There is no restriction on the number of compilation results since they don't use very much space. Old results will be used after server restarts, except in the case of a server upgrade – in this case, the old results are deleted.

## output\_format\_json\_quote\_64bit\_integers {#session_settings-output_format_json_quote_64bit_integers}

Si el valor es true, los enteros aparecen entre comillas cuando se usan los formatos JSON\* Int64 y UInt64 (por compatibilidad con la mayoría de las implementaciones de JavaScript); de lo contrario, los enteros se generan sin las comillas.

## Formato\_csv\_delimiter {#settings-format_csv_delimiter}

El carácter interpretado como un delimitador en los datos CSV. De forma predeterminada, el delimitador es `,`.

## input\_format\_csv\_unquoted\_null\_literal\_as\_null {#settings-input_format_csv_unquoted_null_literal_as_null}

Para el formato de entrada CSV, habilita o deshabilita el análisis de `NULL` como literal (sinónimo de `\N`).

## output\_format\_csv\_crlf\_end\_of\_line {#settings-output-format-csv-crlf-end-of-line}

Utilice el separador de línea de estilo DOS / Windows (CRLF) en CSV en lugar de estilo Unix (LF).

## output\_format\_tsv\_crlf\_end\_of\_line {#settings-output-format-tsv-crlf-end-of-line}

Utilice el separador de línea de estilo DOC / Windows (CRLF) en TSV en lugar del estilo Unix (LF).

## insert\_quorum {#settings-insert_quorum}

Habilita las escrituras de quórum.

-   Si `insert_quorum < 2`, las escrituras de quórum están deshabilitadas.
-   Si `insert_quorum >= 2`, las escrituras de quórum están habilitadas.

Valor predeterminado: 0.

Quorum escribe

`INSERT` solo tiene éxito cuando ClickHouse logra escribir correctamente datos en el `insert_quorum` de réplicas durante el `insert_quorum_timeout`. Si por alguna razón el número de réplicas con escrituras exitosas no alcanza el `insert_quorum`, la escritura se considera fallida y ClickHouse eliminará el bloque insertado de todas las réplicas donde los datos ya se han escrito.

Todas las réplicas del quórum son consistentes, es decir, contienen datos de todas las réplicas anteriores `INSERT` consulta. El `INSERT` la secuencia está linealizada.

Al leer los datos escritos desde el `insert_quorum` usted puede utilizar el [select\_sequential\_consistency](#settings-select_sequential_consistency) opcion.

ClickHouse genera una excepción

-   Si el número de réplicas disponibles en el momento de la consulta es `insert_quorum`.
-   En un intento de escribir datos cuando el bloque anterior aún no se ha insertado en el `insert_quorum` de réplicas. Esta situación puede ocurrir si el usuario intenta realizar una `INSERT` antes de la anterior con el `insert_quorum` se ha completado.

Ver también:

-   [insert\_quorum\_timeout](#settings-insert_quorum_timeout)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## insert\_quorum\_timeout {#settings-insert_quorum_timeout}

Escribir en tiempo de espera de quórum en segundos. Si el tiempo de espera ha pasado y aún no se ha realizado ninguna escritura, ClickHouse generará una excepción y el cliente debe repetir la consulta para escribir el mismo bloque en la misma réplica o en cualquier otra réplica.

Valor predeterminado: 60 segundos.

Ver también:

-   [insert\_quorum](#settings-insert_quorum)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## select\_sequential\_consistency {#settings-select_sequential_consistency}

Habilita o deshabilita la coherencia secuencial para `SELECT` consulta:

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 0.

Uso

Cuando se habilita la coherencia secuencial, ClickHouse permite al cliente ejecutar el `SELECT` consulta sólo para aquellas réplicas que contienen datos de todas las `INSERT` consultas ejecutadas con `insert_quorum`. Si el cliente hace referencia a una réplica parcial, ClickHouse generará una excepción. La consulta SELECT no incluirá datos que aún no se hayan escrito en el quórum de réplicas.

Ver también:

-   [insert\_quorum](#settings-insert_quorum)
-   [insert\_quorum\_timeout](#settings-insert_quorum_timeout)

## insert\_deduplicate {#settings-insert-deduplicate}

Habilita o deshabilita la desduplicación de bloques `INSERT` (para tablas replicadas\*

Valores posibles:

-   0 — Disabled.
-   1 — Enabled.

Valor predeterminado: 1.

De forma predeterminada, los bloques insertados en tablas replicadas `INSERT` declaración se deduplican (ver [Replicación de datos](../../engines/table-engines/mergetree-family/replication.md)).

## deduplicate\_blocks\_in\_dependent\_materialized\_views {#settings-deduplicate-blocks-in-dependent-materialized-views}

Habilita o deshabilita la comprobación de desduplicación para las vistas materializadas que reciben datos de tablas replicadas\*.

Valores posibles:

      0 — Disabled.
      1 — Enabled.

Valor predeterminado: 0.

Uso

De forma predeterminada, la desduplicación no se realiza para las vistas materializadas, sino que se realiza en sentido ascendente, en la tabla de origen.
Si se omite un bloque INSERTed debido a la desduplicación en la tabla de origen, no habrá inserción en las vistas materializadas adjuntas. Este comportamiento existe para permitir la inserción de datos altamente agregados en vistas materializadas, para los casos en que los bloques insertados son los mismos después de la agregación de vistas materializadas pero derivados de diferentes INSERT en la tabla de origen.
Al mismo tiempo, este comportamiento “breaks” `INSERT` idempotencia. Si una `INSERT` en la mesa principal fue exitoso y `INSERT` into a materialized view failed (e.g. because of communication failure with Zookeeper) a client will get an error and can retry the operation. However, the materialized view won't receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` permite cambiar este comportamiento. Al reintentar, una vista materializada recibirá la inserción de repetición y realizará la comprobación de desduplicación por sí misma,
ignorando el resultado de la comprobación para la tabla de origen, e insertará filas perdidas debido a la primera falla.

## Método de codificación de datos: {#settings-max-network-bytes}

Limita el volumen de datos (en bytes) que se recibe o se transmite a través de la red al ejecutar una consulta. Esta configuración se aplica a cada consulta individual.

Valores posibles:

-   Entero positivo.
-   0 — Data volume control is disabled.

Valor predeterminado: 0.

## Método de codificación de datos: {#settings-max-network-bandwidth}

Limita la velocidad del intercambio de datos a través de la red en bytes por segundo. Esta configuración se aplica a todas las consultas.

Valores posibles:

-   Entero positivo.
-   0 — Bandwidth control is disabled.

Valor predeterminado: 0.

## Todos los derechos reservados {#settings-max-network-bandwidth-for-user}

Limita la velocidad del intercambio de datos a través de la red en bytes por segundo. Esta configuración se aplica a todas las consultas que se ejecutan simultáneamente realizadas por un único usuario.

Valores posibles:

-   Entero positivo.
-   0 — Control of the data speed is disabled.

Valor predeterminado: 0.

## Todos los derechos reservados {#settings-max-network-bandwidth-for-all-users}

Limita la velocidad a la que se intercambian datos a través de la red en bytes por segundo. Esta configuración se aplica a todas las consultas que se ejecutan simultáneamente en el servidor.

Valores posibles:

-   Entero positivo.
-   0 — Control of the data speed is disabled.

Valor predeterminado: 0.

## count\_distinct\_implementation {#settings-count_distinct_implementation}

Especifica cuál de las `uniq*` se deben utilizar para realizar el [COUNT(DISTINCT …)](../../sql-reference/aggregate-functions/reference.md#agg_function-count) construcción.

Valores posibles:

-   [uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq)
-   [uniqCombined](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined)
-   [UniqCombined64](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqhll12)
-   [uniqExact](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqexact)

Valor predeterminado: `uniqExact`.

## skip\_unavailable\_shards {#settings-skip_unavailable_shards}

Habilita o deshabilita la omisión silenciosa de fragmentos no disponibles.

El fragmento se considera no disponible si todas sus réplicas no están disponibles. Una réplica no está disponible en los siguientes casos:

-   ClickHouse no puede conectarse a la réplica por ningún motivo.

    Al conectarse a una réplica, ClickHouse realiza varios intentos. Si todos estos intentos fallan, la réplica se considera que no está disponible.

-   La réplica no se puede resolver a través de DNS.

    Si el nombre de host de la réplica no se puede resolver a través de DNS, puede indicar las siguientes situaciones:

    -   El host de Replica no tiene registro DNS. Puede ocurrir en sistemas con DNS dinámico, por ejemplo, [Kubernetes](https://kubernetes.io), donde los nodos pueden ser irresolubles durante el tiempo de inactividad, y esto no es un error.

    -   Error de configuración. El archivo de configuración de ClickHouse contiene un nombre de host incorrecto.

Valores posibles:

-   1 — skipping enabled.

    Si un fragmento no está disponible, ClickHouse devuelve un resultado basado en datos parciales y no informa de problemas de disponibilidad de nodos.

-   0 — skipping disabled.

    Si un fragmento no está disponible, ClickHouse produce una excepción.

Valor predeterminado: 0.

## Optize\_skip\_unused\_shards {#settings-optimize_skip_unused_shards}

Habilita o deshabilita la omisión de fragmentos no utilizados para las consultas SELECT que tienen la condición de clave de fragmentación en PREWHERE / WHERE (supone que los datos se distribuyen mediante clave de fragmentación, de lo contrario no hacer nada).

Valor predeterminado: 0

## Fuerza\_optimize\_skip\_unused\_shards {#settings-force_optimize_skip_unused_shards}

Habilita o deshabilita la ejecución de consultas si [`optimize_skip_unused_shards`](#settings-optimize_skip_unused_shards) no es posible omitir fragmentos no utilizados. Si la omisión no es posible y la configuración está habilitada, se lanzará una excepción.

Valores posibles:

-   0 - Discapacitados (no lanza)
-   1: deshabilite la ejecución de consultas solo si la tabla tiene una clave de fragmentación
-   2: deshabilita la ejecución de consultas independientemente de que se haya definido la clave de fragmentación para la tabla

Valor predeterminado: 0

## Optize\_throw\_if\_noop {#setting-optimize_throw_if_noop}

Habilita o deshabilita el lanzamiento de una excepción [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) la consulta no realizó una fusión.

Predeterminada, `OPTIMIZE` devuelve con éxito incluso si no hizo nada. Esta configuración le permite diferenciar estas situaciones y obtener el motivo en un mensaje de excepción.

Valores posibles:

-   1 — Throwing an exception is enabled.
-   0 — Throwing an exception is disabled.

Valor predeterminado: 0.

## distributed\_replica\_error\_half\_life {#settings-distributed_replica_error_half_life}

-   Tipo: segundos
-   Valor predeterminado: 60 segundos

Controla la rapidez con la que se ponen a cero los errores en las tablas distribuidas. Si una réplica no está disponible durante algún tiempo, acumula 5 errores y distribut\_replica\_error\_half\_life se establece en 1 segundo, la réplica se considera normal 3 segundos después del último error.

Ver también:

-   [Motor de tabla distribuido](../../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_cap](#settings-distributed_replica_error_cap)

## distributed\_replica\_error\_cap {#settings-distributed_replica_error_cap}

-   Tipo: unsigned int
-   Valor predeterminado: 1000

El recuento de errores de cada réplica está limitado a este valor, lo que impide que una sola réplica acumule demasiados errores.

Ver también:

-   [Motor de tabla distribuido](../../engines/table-engines/special/distributed.md)
-   [distributed\_replica\_error\_half\_life](#settings-distributed_replica_error_half_life)

## Distributed\_directory\_monitor\_sleep\_time\_ms {#distributed_directory_monitor_sleep_time_ms}

Intervalo base para el [Distribuido](../../engines/table-engines/special/distributed.md) motor de tabla para enviar datos. El intervalo real crece exponencialmente en caso de errores.

Valores posibles:

-   Un número entero positivo de milisegundos.

Valor predeterminado: 100 milisegundos.

## Distributed\_directory\_monitor\_max\_sleep\_time\_ms {#distributed_directory_monitor_max_sleep_time_ms}

Intervalo máximo para el [Distribuido](../../engines/table-engines/special/distributed.md) motor de tabla para enviar datos. Limita el crecimiento exponencial del intervalo establecido en el [Distributed\_directory\_monitor\_sleep\_time\_ms](#distributed_directory_monitor_sleep_time_ms) configuración.

Valores posibles:

-   Un número entero positivo de milisegundos.

Valor predeterminado: 30000 milisegundos (30 segundos).

## distributed\_directory\_monitor\_batch\_inserts {#distributed_directory_monitor_batch_inserts}

Habilita/deshabilita el envío de datos insertados en lotes.

Cuando el envío por lotes está habilitado, el [Distribuido](../../engines/table-engines/special/distributed.md) El motor de tabla intenta enviar varios archivos de datos insertados en una operación en lugar de enviarlos por separado. El envío por lotes mejora el rendimiento del clúster al utilizar mejor los recursos del servidor y de la red.

Valores posibles:

-   1 — Enabled.
-   0 — Disabled.

Valor predeterminado: 0.

## os\_thread\_priority {#setting-os-thread-priority}

Establece la prioridad ([agradable](https://en.wikipedia.org/wiki/Nice_(Unix))) para subprocesos que ejecutan consultas. El programador del sistema operativo considera esta prioridad al elegir el siguiente hilo para ejecutar en cada núcleo de CPU disponible.

!!! warning "Advertencia"
    Para utilizar esta configuración, debe establecer el `CAP_SYS_NICE` capacidad. El `clickhouse-server` paquete lo configura durante la instalación. Algunos entornos virtuales no le permiten establecer `CAP_SYS_NICE` capacidad. En este caso, `clickhouse-server` muestra un mensaje al respecto al principio.

Valores posibles:

-   Puede establecer valores en el rango `[-20, 19]`.

Los valores más bajos significan mayor prioridad. Hilos con bajo `nice` Los valores de prioridad se ejecutan con más frecuencia que los subprocesos con valores altos. Los valores altos son preferibles para consultas no interactivas de larga ejecución porque les permite renunciar rápidamente a recursos en favor de consultas interactivas cortas cuando llegan.

Valor predeterminado: 0.

## query\_profiler\_real\_time\_period\_ns {#query_profiler_real_time_period_ns}

Establece el período para un temporizador de reloj real del [perfilador de consultas](../../operations/optimizing-performance/sampling-query-profiler.md). El temporizador de reloj real cuenta el tiempo del reloj de pared.

Valores posibles:

-   Número entero positivo, en nanosegundos.

    Valores recomendados:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 para apagar el temporizador.

Tipo: [UInt64](../../sql-reference/data-types/int-uint.md).

Valor predeterminado: 1000000000 nanosegundos (una vez por segundo).

Ver también:

-   Tabla del sistema [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## Los resultados de la prueba {#query_profiler_cpu_time_period_ns}

Establece el período para un temporizador de reloj de CPU [perfilador de consultas](../../operations/optimizing-performance/sampling-query-profiler.md). Este temporizador solo cuenta el tiempo de CPU.

Valores posibles:

-   Un número entero positivo de nanosegundos.

    Valores recomendados:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 para apagar el temporizador.

Tipo: [UInt64](../../sql-reference/data-types/int-uint.md).

Valor predeterminado: 1000000000 nanosegundos.

Ver también:

-   Tabla del sistema [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## allow\_introspection\_functions {#settings-allow_introspection_functions}

Habilita deshabilita [funciones de introspecciones](../../sql-reference/functions/introspection.md) para la creación de perfiles de consultas.

Valores posibles:

-   1 — Introspection functions enabled.
-   0 — Introspection functions disabled.

Valor predeterminado: 0.

**Ver también**

-   [Analizador de consultas de muestreo](../optimizing-performance/sampling-query-profiler.md)
-   Tabla del sistema [trace\_log](../../operations/system-tables.md#system_tables-trace_log)

## input\_format\_parallel\_parsing {#input-format-parallel-parsing}

-   Tipo: bool
-   Valor predeterminado: True

Habilitar el análisis paralelo de los formatos de datos para preservar el orden. Solo se admite para los formatos TSV, TKSV, CSV y JSONEachRow.

## También puede utilizar los siguientes métodos de envío: {#min-chunk-bytes-for-parallel-parsing}

-   Tipo: unsigned int
-   Valor predeterminado: 1 MiB

El tamaño mínimo de fragmento en bytes, que cada subproceso analizará en paralelo.

## Sistema abierto {#settings-output_format_avro_codec}

Establece el códec de compresión utilizado para el archivo Avro de salida.

Tipo: cadena

Valores posibles:

-   `null` — No compression
-   `deflate` — Compress with Deflate (zlib)
-   `snappy` — Compress with [Rápido](https://google.github.io/snappy/)

Valor predeterminado: `snappy` (si está disponible) o `deflate`.

## Sistema abierto {#settings-output_format_avro_sync_interval}

Establece el tamaño mínimo de datos (en bytes) entre los marcadores de sincronización para el archivo Avro de salida.

Tipo: unsigned int

Valores posibles: 32 (32 bytes) - 1073741824 (1 GiB)

Valor predeterminado: 32768 (32 KiB)

## Todos los derechos reservados {#settings-format_avro_schema_registry_url}

Establece la URL del Registro de esquemas confluentes para usar con [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent) formato

Tipo: URL

Valor predeterminado: Vacío

## background\_pool\_size {#background_pool_size}

Establece el número de subprocesos que realizan operaciones en segundo plano en motores de tabla (por ejemplo, fusiona [Motor MergeTree](../../engines/table-engines/mergetree-family/index.md) tabla). Esta configuración se aplica al inicio del servidor ClickHouse y no se puede cambiar en una sesión de usuario. Al ajustar esta configuración, puede administrar la carga de la CPU y el disco. Un tamaño de grupo más pequeño utiliza menos recursos de CPU y disco, pero los procesos en segundo plano avanzan más lentamente, lo que eventualmente podría afectar el rendimiento de la consulta.

Valores posibles:

-   Cualquier entero positivo.

Valor predeterminado: 16.

[Artículo Original](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->
