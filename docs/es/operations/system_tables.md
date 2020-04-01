---
machine_translated: true
---

# Tablas del sistema {#system-tables}

Las tablas del sistema se utilizan para implementar parte de la funcionalidad del sistema y para proporcionar acceso a información sobre cómo funciona el sistema.
No puede eliminar una tabla del sistema (pero puede realizar DETACH).
Las tablas del sistema no tienen archivos con datos en el disco o archivos con metadatos. El servidor crea todas las tablas del sistema cuando se inicia.
Las tablas del sistema son de solo lectura.
Están ubicados en el ‘system’ basar.

## sistema.asynchronous\_metrics {#system_tables-asynchronous_metrics}

Contiene métricas que se calculan periódicamente en segundo plano. Por ejemplo, la cantidad de RAM en uso.

Columna:

-   `metric` ([Cadena](../data_types/string.md)) — Nombre métrico.
-   `value` ([Float64](../data_types/float.md)) — Valor métrico.

**Ejemplo**

``` sql
SELECT * FROM system.asynchronous_metrics LIMIT 10
```

``` text
┌─metric──────────────────────────────────┬──────value─┐
│ jemalloc.background_thread.run_interval │          0 │
│ jemalloc.background_thread.num_runs     │          0 │
│ jemalloc.background_thread.num_threads  │          0 │
│ jemalloc.retained                       │  422551552 │
│ jemalloc.mapped                         │ 1682989056 │
│ jemalloc.resident                       │ 1656446976 │
│ jemalloc.metadata_thp                   │          0 │
│ jemalloc.metadata                       │   10226856 │
│ UncompressedCacheCells                  │          0 │
│ MarkCacheFiles                          │          0 │
└─────────────────────────────────────────┴────────────┘
```

**Ver también**

-   [Monitoreo](monitoring.md) — Conceptos básicos de monitoreo ClickHouse.
-   [sistema.métricas](#system_tables-metrics) - Contiene métricas calculadas al instante.
-   [sistema.evento](#system_tables-events) — Contiene una serie de eventos que han ocurrido.
-   [sistema.metric\_log](#system_tables-metric_log) — Contiene un historial de valores de métricas de tablas `system.metrics` , . `system.events`.

## sistema.Cluster {#system-clusters}

Contiene información sobre los clústeres disponibles en el archivo de configuración y los servidores que contienen.

Columna:

-   `cluster` (String) — El nombre del clúster.
-   `shard_num` (UInt32) — El número de fragmento en el clúster, a partir de 1.
-   `shard_weight` (UInt32) — El peso relativo del fragmento al escribir datos.
-   `replica_num` (UInt32) — El número de réplica en el fragmento, a partir de 1.
-   `host_name` (String) — El nombre de host, como se especifica en la configuración.
-   `host_address` (String) — La dirección IP del host obtenida de DNS.
-   `port` (UInt16): el puerto que se utiliza para conectarse al servidor.
-   `user` (String) — El nombre del usuario para conectarse al servidor.
-   `errors_count` (UInt32): número de veces que este host no pudo alcanzar la réplica.
-   `estimated_recovery_time` (UInt32): quedan segundos hasta que el recuento de errores de réplica se ponga a cero y se considere que vuelve a la normalidad.

Tenga en cuenta que `errors_count` se actualiza una vez por consulta al clúster, pero `estimated_recovery_time` se vuelve a calcular bajo demanda. Entonces podría haber un caso distinto de cero `errors_count` y cero `estimated_recovery_time`, esa próxima consulta será cero `errors_count` e intente usar la réplica como si no tuviera errores.

**Ver también**

-   [Motor de tabla distribuido](table_engines/distributed.md)
-   [distributed\_replica\_error\_cap configuración](settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed\_replica\_error\_half\_life configuración](settings/settings.md#settings-distributed_replica_error_half_life)

## sistema.columna {#system-columns}

Contiene información sobre las columnas de todas las tablas.

Puede utilizar esta tabla para obtener información similar a la [TABLA DE DESCRIBE](../query_language/misc.md#misc-describe-table) Consulta, pero para varias tablas a la vez.

El `system.columns` tabla contiene las siguientes columnas (el tipo de columna se muestra entre corchetes):

-   `database` (String) — Nombre de la base de datos.
-   `table` (Cadena) — Nombre de tabla.
-   `name` (Cadena) — Nombre de columna.
-   `type` (Cadena) — Tipo de columna.
-   `default_kind` (String) — Tipo de expresión (`DEFAULT`, `MATERIALIZED`, `ALIAS`) para el valor predeterminado, o una cadena vacía si no está definida.
-   `default_expression` (String) — Expresión para el valor predeterminado, o una cadena vacía si no está definida.
-   `data_compressed_bytes` (UInt64): el tamaño de los datos comprimidos, en bytes.
-   `data_uncompressed_bytes` (UInt64): el tamaño de los datos descomprimidos, en bytes.
-   `marks_bytes` (UInt64) — El tamaño de las marcas, en bytes.
-   `comment` (Cadena): comenta la columna o una cadena vacía si no está definida.
-   `is_in_partition_key` (UInt8): marca que indica si la columna está en la expresión de partición.
-   `is_in_sorting_key` (UInt8): marca que indica si la columna está en la expresión de clave de ordenación.
-   `is_in_primary_key` (UInt8): marca que indica si la columna está en la expresión de clave principal.
-   `is_in_sampling_key` (UInt8): marca que indica si la columna está en la expresión de clave de muestreo.

## sistema.colaborador {#system-contributors}

Contiene información sobre los colaboradores. Todos los constributores en orden aleatorio. El orden es aleatorio en el momento de la ejecución de la consulta.

Columna:

-   `name` (Cadena) - Nombre del colaborador (autor) del git log.

**Ejemplo**

``` sql
SELECT * FROM system.contributors LIMIT 10
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
│ Max Vetrov       │
│ LiuYangkuan      │
│ svladykin        │
│ zamulla          │
│ Šimon Podlipský  │
│ BayoNet          │
│ Ilya Khomutov    │
│ Amy Krishnevsky  │
│ Loud_Scream      │
└──────────────────┘
```

Para descubrirlo en la tabla, use una consulta:

``` sql
SELECT * FROM system.contributors WHERE name='Olga Khvostikova'
```

``` text
┌─name─────────────┐
│ Olga Khvostikova │
└──────────────────┘
```

## sistema.basar {#system-databases}

Esta tabla contiene una sola columna String llamada ‘name’ – el nombre de una base de datos.
Cada base de datos que el servidor conoce tiene una entrada correspondiente en la tabla.
Esta tabla del sistema se utiliza para implementar el `SHOW DATABASES` consulta.

## sistema.detached\_parts {#system_tables-detached_parts}

Contiene información sobre piezas separadas de [Método de codificación de datos:](table_engines/mergetree.md) tabla. El `reason` columna especifica por qué se separó la pieza. Para las piezas separadas por el usuario, el motivo está vacío. Tales partes se pueden unir con [ALTER TABLE ATTACH PARTITION\|PARTE](../query_language/query_language/alter/#alter_attach-partition) comando. Para obtener la descripción de otras columnas, consulte [sistema.parte](#system_tables-parts). Si el nombre de la pieza no es válido, los valores de algunas columnas pueden ser `NULL`. Tales partes se pueden eliminar con [ALTER MESA GOTA PARTE DESMONTADA](../query_language/query_language/alter/#alter_drop-detached).

## sistema.Diccionario {#system-dictionaries}

Contiene información sobre diccionarios externos.

Columna:

-   `name` (Cadena) — Nombre del diccionario.
-   `type` (Cadena) - Tipo de diccionario: plano, hash, caché.
-   `origin` (String) — Ruta de acceso al archivo de configuración que describe el diccionario.
-   `attribute.names` (Array(String)) — Matriz de nombres de atributos proporcionados por el diccionario.
-   `attribute.types` (Array(String)) — Matriz correspondiente de tipos de atributos que proporciona el diccionario.
-   `has_hierarchy` (UInt8) - Si el diccionario es jerárquico.
-   `bytes_allocated` (UInt64) - La cantidad de RAM que usa el diccionario.
-   `hit_rate` (Float64): para los diccionarios de caché, el porcentaje de usos para los que el valor estaba en la caché.
-   `element_count` (UInt64) — El número de elementos almacenados en el diccionario.
-   `load_factor` (Float64): el porcentaje rellenado en el diccionario (para un diccionario hash, el porcentaje rellenado en la tabla hash).
-   `creation_time` (DateTime): la hora en que se creó el diccionario o se recargó correctamente por última vez.
-   `last_exception` (Cadena) — Texto del error que se produce al crear o volver a cargar el diccionario si no se pudo crear el diccionario.
-   `source` (String) — Texto que describe el origen de datos para el diccionario.

Tenga en cuenta que la cantidad de memoria utilizada por el diccionario no es proporcional a la cantidad de elementos almacenados en él. Por lo tanto, para los diccionarios planos y en caché, todas las celdas de memoria se asignan previamente, independientemente de qué tan lleno esté realmente el diccionario.

## sistema.evento {#system_tables-events}

Contiene información sobre el número de eventos que se han producido en el sistema. Por ejemplo, en la tabla, puede encontrar cuántos `SELECT` las consultas se procesaron desde que se inició el servidor ClickHouse.

Columna:

-   `event` ([Cadena](../data_types/string.md)) — Nombre del evento.
-   `value` ([UInt64](../data_types/int_uint.md)) — Número de eventos ocurridos.
-   `description` ([Cadena](../data_types/string.md)) — Descripción del evento.

**Ejemplo**

``` sql
SELECT * FROM system.events LIMIT 5
```

``` text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ Number of queries to be interpreted and potentially executed. Does not include queries that failed to parse or were rejected due to AST size limits, quota limits or limits on the number of simultaneously running queries. May include internal queries initiated by ClickHouse itself. Does not count subqueries.                  │
│ SelectQuery                           │     8 │ Same as Query, but only for SELECT queries.                                                                                                                                                                                                                │
│ FileOpen                              │    73 │ Number of files opened.                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ Number of reads (read/pread) from a file descriptor. Does not include sockets.                                                                                                                                                                             │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ Number of bytes read from file descriptors. If the file is compressed, this will show the compressed data size.                                                                                                                                              │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Ver también**

-   [sistema.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contiene métricas calculadas periódicamente.
-   [sistema.métricas](#system_tables-metrics) - Contiene métricas calculadas al instante.
-   [sistema.metric\_log](#system_tables-metric_log) — Contiene un historial de valores de métricas de tablas `system.metrics` , . `system.events`.
-   [Monitoreo](monitoring.md) — Conceptos básicos de monitoreo ClickHouse.

## sistema.función {#system-functions}

Contiene información sobre funciones normales y agregadas.

Columna:

-   `name`(`String`) – El nombre de la función.
-   `is_aggregate`(`UInt8`) — Si la función es agregada.

## sistema.graphite\_retentions {#system-graphite-retentions}

Contiene información sobre los parámetros [graphite\_rollup](server_settings/settings.md#server_settings-graphite_rollup) que se utilizan en tablas con [\*GraphiteMergeTree](table_engines/graphitemergetree.md) motor.

Columna:

-   `config_name` (Cadena) - `graphite_rollup` nombre del parámetro.
-   `regexp` (Cadena) - Un patrón para el nombre de la métrica.
-   `function` (String) - El nombre de la función de agregación.
-   `age` (UInt64) - La edad mínima de los datos en segundos.
-   `precision` (UInt64) - Cómo definir con precisión la edad de los datos en segundos.
-   `priority` (UInt16) - Prioridad de patrón.
-   `is_default` (UInt8) - Si el patrón es el predeterminado.
-   `Tables.database` (Array(String)) - Matriz de nombres de tablas de base de datos que utilizan `config_name` parámetro.
-   `Tables.table` (Array(String)) - Matriz de nombres de tablas que utilizan `config_name` parámetro.

## sistema.Fusionar {#system-merges}

Contiene información sobre fusiones y mutaciones de piezas actualmente en proceso para tablas de la familia MergeTree.

Columna:

-   `database` (String) — El nombre de la base de datos en la que se encuentra la tabla.
-   `table` (Cadena) — Nombre de tabla.
-   `elapsed` (Float64) — El tiempo transcurrido (en segundos) desde que se inició la fusión.
-   `progress` (Float64) — El porcentaje de trabajo completado de 0 a 1.
-   `num_parts` (UInt64) — El número de piezas que se fusionarán.
-   `result_part_name` (Cadena) — El nombre de la parte que se formará como resultado de la fusión.
-   `is_mutation` (UInt8) - 1 si este proceso es una mutación parte.
-   `total_size_bytes_compressed` (UInt64): el tamaño total de los datos comprimidos en los fragmentos combinados.
-   `total_size_marks` (UInt64) — Número total de marcas en las partes fusionadas.
-   `bytes_read_uncompressed` (UInt64) — Número de bytes leídos, sin comprimir.
-   `rows_read` (UInt64) — Número de filas leídas.
-   `bytes_written_uncompressed` (UInt64) — Número de bytes escritos, sin comprimir.
-   `rows_written` (UInt64) — Número de filas escritas.

## sistema.métricas {#system_tables-metrics}

Contiene métricas que pueden calcularse instantáneamente o tener un valor actual. Por ejemplo, el número de consultas procesadas simultáneamente o el retraso de réplica actual. Esta tabla está siempre actualizada.

Columna:

-   `metric` ([Cadena](../data_types/string.md)) — Nombre métrico.
-   `value` ([Int64](../data_types/int_uint.md)) — Valor métrico.
-   `description` ([Cadena](../data_types/string.md)) — Descripción métrica.

La lista de métricas admitidas que puede encontrar en el [dbms/src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/dbms/src/Common/CurrentMetrics.cpp) archivo fuente de ClickHouse.

**Ejemplo**

``` sql
SELECT * FROM system.metrics LIMIT 10
```

``` text
┌─metric─────────────────────┬─value─┬─description──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                      │     1 │ Number of executing queries                                                                                                                                                                      │
│ Merge                      │     0 │ Number of executing background merges                                                                                                                                                            │
│ PartMutation               │     0 │ Number of mutations (ALTER DELETE/UPDATE)                                                                                                                                                        │
│ ReplicatedFetch            │     0 │ Number of data parts being fetched from replicas                                                                                                                                                │
│ ReplicatedSend             │     0 │ Number of data parts being sent to replicas                                                                                                                                                      │
│ ReplicatedChecks           │     0 │ Number of data parts checking for consistency                                                                                                                                                    │
│ BackgroundPoolTask         │     0 │ Number of active tasks in BackgroundProcessingPool (merges, mutations, fetches, or replication queue bookkeeping)                                                                                │
│ BackgroundSchedulePoolTask │     0 │ Number of active tasks in BackgroundSchedulePool. This pool is used for periodic ReplicatedMergeTree tasks, like cleaning old data parts, altering data parts, replica re-initialization, etc.   │
│ DiskSpaceReservedForMerge  │     0 │ Disk space reserved for currently running background merges. It is slightly more than the total size of currently merging parts.                                                                     │
│ DistributedSend            │     0 │ Number of connections to remote servers sending data that was INSERTed into Distributed tables. Both synchronous and asynchronous mode.                                                          │
└────────────────────────────┴───────┴──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**Ver también**

-   [sistema.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contiene métricas calculadas periódicamente.
-   [sistema.evento](#system_tables-events) — Contiene una serie de eventos que ocurrieron.
-   [sistema.metric\_log](#system_tables-metric_log) — Contiene un historial de valores de métricas de tablas `system.metrics` , . `system.events`.
-   [Monitoreo](monitoring.md) — Conceptos básicos de monitoreo ClickHouse.

## sistema.metric\_log {#system_tables-metric_log}

Contiene el historial de valores de métricas de tablas `system.metrics` y `system.events`, periódicamente enjuagado al disco.
Para activar la recopilación de historial de métricas en `system.metric_log`, crear `/etc/clickhouse-server/config.d/metric_log.xml` con el siguiente contenido:

``` xml
<yandex>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
    </metric_log>
</yandex>
```

**Ejemplo**

``` sql
SELECT * FROM system.metric_log LIMIT 1 FORMAT Vertical;
```

``` text
Row 1:
──────
event_date:                                                 2020-02-18
event_time:                                                 2020-02-18 07:15:33
milliseconds:                                               554
ProfileEvent_Query:                                         0
ProfileEvent_SelectQuery:                                   0
ProfileEvent_InsertQuery:                                   0
ProfileEvent_FileOpen:                                      0
ProfileEvent_Seek:                                          0
ProfileEvent_ReadBufferFromFileDescriptorRead:              1
ProfileEvent_ReadBufferFromFileDescriptorReadFailed:        0
ProfileEvent_ReadBufferFromFileDescriptorReadBytes:         0
ProfileEvent_WriteBufferFromFileDescriptorWrite:            1
ProfileEvent_WriteBufferFromFileDescriptorWriteFailed:      0
ProfileEvent_WriteBufferFromFileDescriptorWriteBytes:       56
...
CurrentMetric_Query:                                        0
CurrentMetric_Merge:                                        0
CurrentMetric_PartMutation:                                 0
CurrentMetric_ReplicatedFetch:                              0
CurrentMetric_ReplicatedSend:                               0
CurrentMetric_ReplicatedChecks:                             0
...
```

**Ver también**

-   [sistema.asynchronous\_metrics](#system_tables-asynchronous_metrics) — Contiene métricas calculadas periódicamente.
-   [sistema.evento](#system_tables-events) — Contiene una serie de eventos que ocurrieron.
-   [sistema.métricas](#system_tables-metrics) - Contiene métricas calculadas al instante.
-   [Monitoreo](monitoring.md) — Conceptos básicos de monitoreo ClickHouse.

## sistema.número {#system-numbers}

Esta tabla contiene una única columna UInt64 llamada ‘number’ que contiene casi todos los números naturales a partir de cero.
Puede usar esta tabla para pruebas, o si necesita hacer una búsqueda de fuerza bruta.
Las lecturas de esta tabla no están paralelizadas.

## sistema.Números\_mt {#system-numbers-mt}

Lo mismo que ‘system.numbers’ pero las lecturas están paralelizadas. Los números se pueden devolver en cualquier orden.
Se utiliza para pruebas.

## sistema.una {#system-one}

Esta tabla contiene una sola fila con una ‘dummy’ Columna UInt8 que contiene el valor 0.
Esta tabla se utiliza si una consulta SELECT no especifica la cláusula FROM.
Esto es similar a la tabla DUAL que se encuentra en otros DBMS.

## sistema.parte {#system_tables-parts}

Contiene información sobre partes de [Método de codificación de datos:](table_engines/mergetree.md) tabla.

Cada fila describe una parte de datos.

Columna:

-   `partition` (Cadena) – el nombre de La partición. Para saber qué es una partición, consulte la descripción del [ALTERAR](../query_language/alter.md#query_language_queries_alter) consulta.

    Formato:

    -   `YYYYMM` para la partición automática por mes.
    -   `any_string` al particionar manualmente.

-   `name` (`String`) – Nombre de la parte de datos.

-   `active` (`UInt8`) – Indicador que indica si la parte de datos está activa. Si un elemento de datos está activo, se utiliza en una tabla. De lo contrario, se elimina. Las partes de datos inactivas permanecen después de la fusión.

-   `marks` (`UInt64`) – El número de puntos. Para obtener el número aproximado de filas en una parte de datos, multiplique `marks` por la granularidad del índice (generalmente 8192) (esta sugerencia no funciona para la granularidad adaptativa).

-   `rows` (`UInt64`) – El número de filas.

-   `bytes_on_disk` (`UInt64`) – Tamaño total de todos los archivos de parte de datos en bytes.

-   `data_compressed_bytes` (`UInt64`) – Tamaño total de los datos comprimidos en la parte de datos. Todos los archivos auxiliares (por ejemplo, archivos con marcas) no están incluidos.

-   `data_uncompressed_bytes` (`UInt64`) – Tamaño total de los datos sin comprimir en la parte de datos. Todos los archivos auxiliares (por ejemplo, archivos con marcas) no están incluidos.

-   `marks_bytes` (`UInt64`) – El tamaño del archivo con marcas.

-   `modification_time` (`DateTime`) – La hora en que se modificó el directorio con la parte de datos. Esto normalmente corresponde a la hora de creación del elemento de datos.\|

-   `remove_time` (`DateTime`) – El momento en que la parte de datos quedó inactiva.

-   `refcount` (`UInt32`) – El número de lugares donde se utiliza la parte de datos. Un valor mayor que 2 indica que el elemento de datos se utiliza en consultas o fusiones.

-   `min_date` (`Date`) – El valor mínimo de la clave de fecha en la parte de datos.

-   `max_date` (`Date`) – El valor máximo de la clave de fecha en la parte de datos.

-   `min_time` (`DateTime`) – El valor mínimo de la clave de fecha y hora en la parte de datos.

-   `max_time`(`DateTime`) – El valor máximo de la clave de fecha y hora en la parte de datos.

-   `partition_id` (`String`) – ID de la partición.

-   `min_block_number` (`UInt64`) – El número mínimo de partes de datos que componen la parte actual después de la fusión.

-   `max_block_number` (`UInt64`) – El número máximo de partes de datos que componen la parte actual después de la fusión.

-   `level` (`UInt32`) – Profundidad del árbol de fusión. Cero significa que la parte actual se creó mediante inserción en lugar de fusionar otras partes.

-   `data_version` (`UInt64`) – Número que se utiliza para determinar qué mutaciones se deben aplicar a la parte de datos (mutaciones con una versión superior a `data_version`).

-   `primary_key_bytes_in_memory` (`UInt64`) – La cantidad de memoria (en bytes) utilizada por los valores de clave primaria.

-   `primary_key_bytes_in_memory_allocated` (`UInt64`) – La cantidad de memoria (en bytes) reservada para los valores de clave primaria.

-   `is_frozen` (`UInt8`) – Indicador que muestra que existe una copia de seguridad de datos de partición. 1, la copia de seguridad existe. 0, la copia de seguridad no existe. Para obtener más información, consulte [CONGELAR PARTICIÓN](../query_language/alter.md#alter_freeze-partition)

-   `database` (`String`) – Nombre de la base de datos.

-   `table` (`String`) – Nombre de la tabla.

-   `engine` (`String`) – Nombre del motor de tabla sin parámetros.

-   `path` (`String`) – Ruta absoluta a la carpeta con archivos de parte de datos.

-   `disk` (`String`) – Nombre de un disco que almacena la parte de datos.

-   `hash_of_all_files` (`String`) – [sipHash128](../query_language/functions/hash_functions.md#hash_functions-siphash128) de archivos comprimidos.

-   `hash_of_uncompressed_files` (`String`) – [sipHash128](../query_language/functions/hash_functions.md#hash_functions-siphash128) de archivos sin comprimir (archivos con marcas, archivo de índice, etc.).

-   `uncompressed_hash_of_compressed_files` (`String`) – [sipHash128](../query_language/functions/hash_functions.md#hash_functions-siphash128) de datos en los archivos comprimidos como si estuvieran descomprimidos.

-   `bytes` (`UInt64`) – Alias para `bytes_on_disk`.

-   `marks_size` (`UInt64`) – Alias para `marks_bytes`.

## sistema.part\_log {#system_tables-part-log}

El `system.part_log` se crea sólo si el [part\_log](server_settings/settings.md#server_settings-part-log) se especifica la configuración del servidor.

Esta tabla contiene información sobre eventos que ocurrieron con [partes de datos](table_engines/custom_partitioning_key.md) es el [Método de codificación de datos:](table_engines/mergetree.md) tablas familiares, como agregar o fusionar datos.

El `system.part_log` contiene las siguientes columnas:

-   `event_type` (Enum) — Tipo del evento que ocurrió con la parte de datos. Puede tener uno de los siguientes valores:
    -   `NEW_PART` — Inserción de una nueva parte de datos.
    -   `MERGE_PARTS` — Fusión de partes de datos.
    -   `DOWNLOAD_PART` — Descarga de una parte de datos.
    -   `REMOVE_PART` — Extracción o separación de una parte de datos mediante [DETACH PARTITION](../query_language/alter.md#alter_detach-partition).
    -   `MUTATE_PART` — Mutación de una parte de datos.
    -   `MOVE_PART` — Mover la parte de datos de un disco a otro.
-   `event_date` (Fecha) — fecha del Evento.
-   `event_time` (DateTime) — Hora del evento.
-   `duration_ms` (UInt64) — Duración.
-   `database` (String) — Nombre de la base de datos en la que se encuentra la parte de datos.
-   `table` (String) — Nombre de la tabla en la que se encuentra la parte de datos.
-   `part_name` (String) — Nombre de la parte de datos.
-   `partition_id` (String) — ID de la partición en la que se insertó la parte de datos. La columna toma el ‘all’ valor si la partición es por `tuple()`.
-   `rows` (UInt64): el número de filas en la parte de datos.
-   `size_in_bytes` (UInt64) — Tamaño de la parte de datos en bytes.
-   `merged_from` (Array(String)) - Una matriz de nombres de las partes de las que se componía la parte actual (después de la fusión).
-   `bytes_uncompressed` (UInt64): tamaño de bytes sin comprimir.
-   `read_rows` (UInt64): el número de filas que se leyó durante la fusión.
-   `read_bytes` (UInt64): el número de bytes que se leyeron durante la fusión.
-   `error` (UInt16) — El número de código del error ocurrido.
-   `exception` (Cadena) — Mensaje de texto del error ocurrido.

El `system.part_log` se crea después de la primera inserción de datos `MergeTree` tabla.

## sistema.proceso {#system_tables-processes}

Esta tabla del sistema se utiliza para implementar el `SHOW PROCESSLIST` consulta.

Columna:

-   `user` (Cadena): el usuario que realizó la consulta. Tenga en cuenta que para el procesamiento distribuido, las consultas se envían a servidores remotos `default` usuario. El campo contiene el nombre de usuario para una consulta específica, no para una consulta que esta consulta inició.
-   `address` (Cadena): la dirección IP desde la que se realizó la solicitud. Lo mismo para el procesamiento distribuido. Para realizar un seguimiento de dónde se hizo originalmente una consulta distribuida, mire `system.processes` en el servidor de solicitud de consulta.
-   `elapsed` (Float64): el tiempo en segundos desde que se inició la ejecución de la solicitud.
-   `rows_read` (UInt64): el número de filas leídas de la tabla. Para el procesamiento distribuido, en el solicitante servidor, este es el total para todos los servidores remotos.
-   `bytes_read` (UInt64): el número de bytes sin comprimir leídos de la tabla. Para el procesamiento distribuido, en el solicitante servidor, este es el total para todos los servidores remotos.
-   `total_rows_approx` (UInt64): la aproximación del número total de filas que se deben leer. Para el procesamiento distribuido, en el solicitante servidor, este es el total para todos los servidores remotos. Se puede actualizar durante el procesamiento de solicitudes, cuando se conozcan nuevas fuentes para procesar.
-   `memory_usage` (UInt64): cantidad de RAM que usa la solicitud. Puede que no incluya algunos tipos de memoria dedicada. Ver el [Método de codificación de datos:](../operations/settings/query_complexity.md#settings_max_memory_usage) configuración.
-   `query` (Cadena) – el texto de La consulta. Para `INSERT`, no incluye los datos para insertar.
-   `query_id` (Cadena): ID de consulta, si se define.

## sistema.text\_log {#system-tables-text-log}

Contiene entradas de registro. El nivel de registro que va a esta tabla se puede limitar con `text_log.level` configuración del servidor.

Columna:

-   `event_date` (`Date`) - Fecha de la entrada.
-   `event_time` (`DateTime`) - Hora de la entrada.
-   `microseconds` (`UInt32`) - Microsegundos de la entrada.
-   `thread_name` (Cadena) — Nombre del subproceso desde el que se realizó el registro.
-   `thread_id` (UInt64) - ID de subproceso del sistema operativo.
-   `level` (`Enum8`) - Nivel de entrada.
    -   `'Fatal' = 1`
    -   `'Critical' = 2`
    -   `'Error' = 3`
    -   `'Warning' = 4`
    -   `'Notice' = 5`
    -   `'Information' = 6`
    -   `'Debug' = 7`
    -   `'Trace' = 8`
-   `query_id` (`String`) - ID de la consulta.
-   `logger_name` (`LowCardinality(String)`) - Nombre del registrador (es decir, `DDLWorker`)
-   `message` (`String`) - El mensaje en sí.
-   `revision` (`UInt32`) - Revisión de ClickHouse.
-   `source_file` (`LowCardinality(String)`) - Archivo de origen desde el que se realizó el registro.
-   `source_line` (`UInt64`) - Línea de origen desde la que se realizó el registro.

## sistema.query\_log {#system_tables-query_log}

Contiene información sobre la ejecución de consultas. Para cada consulta, puede ver la hora de inicio del procesamiento, la duración del procesamiento, los mensajes de error y otra información.

!!! note "Nota"
    La tabla no contiene datos de entrada para `INSERT` consulta.

ClickHouse crea esta tabla sólo si el [query\_log](server_settings/settings.md#server_settings-query-log) se especifica el parámetro server. Este parámetro establece las reglas de registro, como el intervalo de registro o el nombre de la tabla en la que se registrarán las consultas.

Para habilitar el registro de consultas, [Log\_queries](settings/settings.md#settings-log-queries) parámetro a 1. Para obtener más información, consulte el [Configuración](settings/settings.md) apartado.

El `system.query_log` tabla registra dos tipos de consultas:

1.  Consultas iniciales ejecutadas directamente por el cliente.
2.  Consultas secundarias iniciadas por otras consultas (para la ejecución de consultas distribuidas). Para estos tipos de consultas, la información sobre las consultas principales se muestra en el `initial_*` columna.

Columna:

-   `type` (`Enum8`) — Tipo de evento que se produjo al ejecutar la consulta. Valor:
    -   `'QueryStart' = 1` — Inicio exitoso de la ejecución de la consulta.
    -   `'QueryFinish' = 2` — Final exitoso de la ejecución de la consulta.
    -   `'ExceptionBeforeStart' = 3` — Excepción antes del inicio de la ejecución de la consulta.
    -   `'ExceptionWhileProcessing' = 4` — Excepción durante la ejecución de la consulta.
-   `event_date` (Fecha) — Fecha de inicio de la consulta.
-   `event_time` (DateTime) — Hora de inicio de la consulta.
-   `query_start_time` (DateTime) — Hora de inicio de la ejecución de la consulta.
-   `query_duration_ms` (UInt64) — Duración de la ejecución de la consulta.
-   `read_rows` (UInt64) — Número de filas leídas.
-   `read_bytes` (UInt64) — Número de bytes leídos.
-   `written_rows` (UInt64) — Para `INSERT` consultas, el número de filas escritas. Para otras consultas, el valor de la columna es 0.
-   `written_bytes` (UInt64) — Para `INSERT` consultas, el número de bytes escritos. Para otras consultas, el valor de la columna es 0.
-   `result_rows` (UInt64) — Número de filas en el resultado.
-   `result_bytes` (UInt64) — Número de bytes en el resultado.
-   `memory_usage` (UInt64) — Consumo de memoria por la consulta.
-   `query` (Cadena) — Cadena de consulta.
-   `exception` (String) — Mensaje de excepción.
-   `stack_trace` (String) - Rastreo de pila (una lista de métodos llamados antes de que ocurriera el error). Una cadena vacía, si la consulta se completa correctamente.
-   `is_initial_query` (UInt8) — Tipo de consulta. Valores posibles:
    -   1 — La consulta fue iniciada por el cliente.
    -   0 — La consulta fue iniciada por otra consulta para la ejecución de consultas distribuidas.
-   `user` (String) — Nombre del usuario que inició la consulta actual.
-   `query_id` (String) — ID de la consulta.
-   `address` (IPv6): dirección IP que se utilizó para realizar la consulta.
-   `port` (UInt16): el puerto de cliente que se utilizó para realizar la consulta.
-   `initial_user` (String) — Nombre del usuario que ejecutó la consulta inicial (para la ejecución de consultas distribuidas).
-   `initial_query_id` (String) — ID de la consulta inicial (para la ejecución de consultas distribuidas).
-   `initial_address` (IPv6): dirección IP desde la que se inició la consulta principal.
-   `initial_port` (UInt16): el puerto de cliente que se utilizó para realizar la consulta principal.
-   `interface` (UInt8): interfaz desde la que se inició la consulta. Valores posibles:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (Cadena) — Nombre de usuario del sistema operativo que ejecuta [Casa de clics-cliente](../interfaces/cli.md).
-   `client_hostname` (String) — Nombre de host de la máquina cliente donde [Casa de clics-cliente](../interfaces/cli.md) o se ejecuta otro cliente TCP.
-   `client_name` (Cadena) — El [Casa de clics-cliente](../interfaces/cli.md) o otro nombre de cliente TCP.
-   `client_revision` (UInt32) — Revisión del [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_major` (UInt32) — Versión principal del [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_minor` (UInt32) — Versión menor de la [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_patch` (UInt32) — Componente de parche del [Casa de clics-cliente](../interfaces/cli.md) o otra versión de cliente TCP.
-   `http_method` (UInt8): método HTTP que inició la consulta. Valores posibles:
    -   0 — La consulta se inició desde la interfaz TCP.
    -   Uno — `GET` se utilizó el método.
    -   Cómo hacer — `POST` se utilizó el método.
-   `http_user_agent` (Cadena) — El `UserAgent` encabezado pasado en la solicitud HTTP.
-   `quota_key` (Cadena) — El “quota key” especificado en el [cuota](quotas.md) ajuste (ver `keyed`).
-   `revision` (UInt32) - Revisión de ClickHouse.
-   `thread_numbers` (Array(UInt32)) — Número de subprocesos que participan en la ejecución de la consulta.
-   `ProfileEvents.Names` (Array(String)) — Contadores que miden diferentes métricas. La descripción de ellos se puede encontrar en la tabla [sistema.evento](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Valores de las métricas que se enumeran en `ProfileEvents.Names` columna.
-   `Settings.Names` (Array(String)) — Nombres de la configuración que se cambiaron cuando el cliente ejecutó la consulta. Para habilitar los cambios de registro en la configuración, `log_query_settings` parámetro a 1.
-   `Settings.Values` (Array(String)) — Valores de configuración que se enumeran en el `Settings.Names` columna.

Cada consulta crea una o dos filas en el `query_log` tabla, dependiendo del estado de la consulta:

1.  Si la ejecución de la consulta se realiza correctamente, se crean dos eventos con los tipos 1 y 2 (consulte `type` columna).
2.  Si se produjo un error durante el procesamiento de la consulta, se crean dos eventos con los tipos 1 y 4.
3.  Si se produjo un error antes de iniciar la consulta, se crea un solo evento con el tipo 3.

De forma predeterminada, los registros se agregan a la tabla a intervalos de 7,5 segundos. Puede establecer este intervalo en el [query\_log](server_settings/settings.md#server_settings-query-log) configuración del servidor (consulte el `flush_interval_milliseconds` parámetro). Para vaciar los registros a la fuerza desde el búfer de memoria a la tabla, utilice `SYSTEM FLUSH LOGS` consulta.

Cuando la tabla se elimina manualmente, se creará automáticamente sobre la marcha. Tenga en cuenta que se eliminarán todos los registros anteriores.

!!! note "Nota"
    El período de almacenamiento para los registros es ilimitado. Los registros no se eliminan automáticamente de la tabla. Debe organizar la eliminación de registros obsoletos usted mismo.

Puede especificar una clave de partición arbitraria `system.query_log` mesa en el [query\_log](server_settings/settings.md#server_settings-query-log) configuración del servidor (consulte el `partition_by` parámetro).

## sistema.Sistema abierto. {#system_tables-query-thread-log}

La tabla contiene información sobre cada subproceso de ejecución de consultas.

ClickHouse crea esta tabla sólo si el [Sistema abierto.](server_settings/settings.md#server_settings-query-thread-log) se especifica el parámetro server. Este parámetro establece las reglas de registro, como el intervalo de registro o el nombre de la tabla en la que se registrarán las consultas.

Para habilitar el registro de consultas, [Log\_query\_threads](settings/settings.md#settings-log-query-threads) parámetro a 1. Para obtener más información, consulte el [Configuración](settings/settings.md) apartado.

Columna:

-   `event_date` (Fecha) — la fecha en que el subproceso ha finalizado la ejecución de la consulta.
-   `event_time` (DateTime) — la fecha y hora en que el subproceso ha finalizado la ejecución de la consulta.
-   `query_start_time` (DateTime) — Hora de inicio de la ejecución de la consulta.
-   `query_duration_ms` (UInt64) — Duración de la ejecución de la consulta.
-   `read_rows` (UInt64) — Número de filas leídas.
-   `read_bytes` (UInt64) — Número de bytes leídos.
-   `written_rows` (UInt64) — Para `INSERT` consultas, el número de filas escritas. Para otras consultas, el valor de la columna es 0.
-   `written_bytes` (UInt64) — Para `INSERT` consultas, el número de bytes escritos. Para otras consultas, el valor de la columna es 0.
-   `memory_usage` (Int64) - La diferencia entre la cantidad de memoria asignada y liberada en el contexto de este hilo.
-   `peak_memory_usage` (Int64) - La diferencia máxima entre la cantidad de memoria asignada y liberada en el contexto de este hilo.
-   `thread_name` (String) — Nombre del hilo.
-   `thread_number` (UInt32) - ID de rosca interna.
-   `os_thread_id` (Int32) - ID de subproceso del sistema operativo.
-   `master_thread_id` (UInt64) - ID inicial del sistema operativo del hilo inicial.
-   `query` (Cadena) — Cadena de consulta.
-   `is_initial_query` (UInt8) — Tipo de consulta. Valores posibles:
    -   1 — La consulta fue iniciada por el cliente.
    -   0 — La consulta fue iniciada por otra consulta para la ejecución de consultas distribuidas.
-   `user` (String) — Nombre del usuario que inició la consulta actual.
-   `query_id` (String) — ID de la consulta.
-   `address` (IPv6): dirección IP que se utilizó para realizar la consulta.
-   `port` (UInt16): el puerto de cliente que se utilizó para realizar la consulta.
-   `initial_user` (String) — Nombre del usuario que ejecutó la consulta inicial (para la ejecución de consultas distribuidas).
-   `initial_query_id` (String) — ID de la consulta inicial (para la ejecución de consultas distribuidas).
-   `initial_address` (IPv6): dirección IP desde la que se inició la consulta principal.
-   `initial_port` (UInt16): el puerto de cliente que se utilizó para realizar la consulta principal.
-   `interface` (UInt8): interfaz desde la que se inició la consulta. Valores posibles:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (Cadena) — Nombre de usuario del sistema operativo que ejecuta [Casa de clics-cliente](../interfaces/cli.md).
-   `client_hostname` (String) — Nombre de host de la máquina cliente donde [Casa de clics-cliente](../interfaces/cli.md) o se ejecuta otro cliente TCP.
-   `client_name` (Cadena) — El [Casa de clics-cliente](../interfaces/cli.md) o otro nombre de cliente TCP.
-   `client_revision` (UInt32) — Revisión del [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_major` (UInt32) — Versión principal del [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_minor` (UInt32) — Versión menor de la [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_patch` (UInt32) — Componente de parche del [Casa de clics-cliente](../interfaces/cli.md) o otra versión de cliente TCP.
-   `http_method` (UInt8): método HTTP que inició la consulta. Valores posibles:
    -   0 — La consulta se inició desde la interfaz TCP.
    -   Uno — `GET` se utilizó el método.
    -   Cómo hacer — `POST` se utilizó el método.
-   `http_user_agent` (Cadena) — El `UserAgent` encabezado pasado en la solicitud HTTP.
-   `quota_key` (Cadena) — El “quota key” especificado en el [cuota](quotas.md) ajuste (ver `keyed`).
-   `revision` (UInt32) - Revisión de ClickHouse.
-   `ProfileEvents.Names` (Array(String)) - Contadores que miden diferentes métricas para este hilo. La descripción de ellos se puede encontrar en la tabla [sistema.evento](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Valores de métricas para este subproceso que se enumeran en el `ProfileEvents.Names` columna.

De forma predeterminada, los registros se agregan a la tabla a intervalos de 7,5 segundos. Puede establecer este intervalo en el [Sistema abierto.](server_settings/settings.md#server_settings-query-thread-log) configuración del servidor (consulte el `flush_interval_milliseconds` parámetro). Para vaciar los registros a la fuerza desde el búfer de memoria a la tabla, utilice `SYSTEM FLUSH LOGS` consulta.

Cuando la tabla se elimina manualmente, se creará automáticamente sobre la marcha. Tenga en cuenta que se eliminarán todos los registros anteriores.

!!! note "Nota"
    El período de almacenamiento para los registros es ilimitado. Los registros no se eliminan automáticamente de la tabla. Debe organizar la eliminación de registros obsoletos usted mismo.

Puede especificar una clave de partición arbitraria `system.query_thread_log` mesa en el [Sistema abierto.](server_settings/settings.md#server_settings-query-thread-log) configuración del servidor (consulte el `partition_by` parámetro).

## sistema.trace\_log {#system_tables-trace_log}

Contiene seguimientos de pila recopilados por el generador de perfiles de consultas de muestreo.

ClickHouse crea esta tabla cuando el [trace\_log](server_settings/settings.md#server_settings-trace_log) se establece la sección de configuración del servidor. También el [query\_profiler\_real\_time\_period\_ns](settings/settings.md#query_profiler_real_time_period_ns) y [Los resultados de la prueba](settings/settings.md#query_profiler_cpu_time_period_ns) los ajustes deben establecerse.

Para analizar los registros, utilice el `addressToLine`, `addressToSymbol` y `demangle` funciones de inspección.

Columna:

-   `event_date`([Fecha](../data_types/date.md)) — Fecha del momento del muestreo.

-   `event_time`([FechaHora](../data_types/datetime.md)) — Marca de tiempo del momento de muestreo.

-   `revision`([UInt32](../data_types/int_uint.md)) — Revisión de compilación del servidor ClickHouse.

    Cuando se conecta al servidor por `clickhouse-client`, ves la cadena similar a `Connected to ClickHouse server version 19.18.1 revision 54429.`. Este campo contiene el `revision`, pero no el `version` de un servidor.

-   `timer_type`([Enum8](../data_types/enum.md)) — Tipo de temporizador:

    -   `Real` representa el tiempo del reloj de pared.
    -   `CPU` representa el tiempo de CPU.

-   `thread_number`([UInt32](../data_types/int_uint.md)) — Identificador del subproceso.

-   `query_id`([Cadena](../data_types/string.md)) — Identificador de consulta que se puede utilizar para obtener detalles sobre una consulta que se estaba ejecutando desde el [query\_log](#system_tables-query_log) tabla del sistema.

-   `trace`([Matriz (UInt64)](../data_types/array.md)) — Rastro de apilamiento en el momento del muestreo. Cada elemento es una dirección de memoria virtual dentro del proceso del servidor ClickHouse.

**Ejemplo**

``` sql
SELECT * FROM system.trace_log LIMIT 1 \G
```

``` text
Row 1:
──────
event_date:    2019-11-15
event_time:    2019-11-15 15:09:38
revision:      54428
timer_type:    Real
thread_number: 48
query_id:      acc4d61f-5bd1-4a3e-bc91-2180be37c915
trace:         [94222141367858,94222152240175,94222152325351,94222152329944,94222152330796,94222151449980,94222144088167,94222151682763,94222144088167,94222151682763,94222144088167,94222144058283,94222144059248,94222091840750,94222091842302,94222091831228,94222189631488,140509950166747,140509942945935]
```

## sistema.Replica {#system_tables-replicas}

Contiene información y estado de las tablas replicadas que residen en el servidor local.
Esta tabla se puede utilizar para el monitoreo. La tabla contiene una fila para cada tabla Replicated\*.

Ejemplo:

``` sql
SELECT *
FROM system.replicas
WHERE table = 'visits'
FORMAT Vertical
```

``` text
Row 1:
──────
database:                   merge
table:                      visits
engine:                     ReplicatedCollapsingMergeTree
is_leader:                  1
can_become_leader:          1
is_readonly:                0
is_session_expired:         0
future_parts:               1
parts_to_check:             0
zookeeper_path:             /clickhouse/tables/01-06/visits
replica_name:               example01-06-1.yandex.ru
replica_path:               /clickhouse/tables/01-06/visits/replicas/example01-06-1.yandex.ru
columns_version:            9
queue_size:                 1
inserts_in_queue:           0
merges_in_queue:            1
part_mutations_in_queue:    0
queue_oldest_time:          2020-02-20 08:34:30
inserts_oldest_time:        0000-00-00 00:00:00
merges_oldest_time:         2020-02-20 08:34:30
part_mutations_oldest_time: 0000-00-00 00:00:00
oldest_part_to_get:
oldest_part_to_merge_to:    20200220_20284_20840_7
oldest_part_to_mutate_to:
log_max_index:              596273
log_pointer:                596274
last_queue_update:          2020-02-20 08:34:32
absolute_delay:             0
total_replicas:             2
active_replicas:            2
```

Columna:

-   `database` (`String`) - Nombre de la base de datos
-   `table` (`String`) - Nombre de la tabla
-   `engine` (`String`) - Nombre del motor de tabla
-   `is_leader` (`UInt8`) - Si la réplica es la líder.
    Sólo una réplica a la vez puede ser el líder. El líder es responsable de seleccionar las fusiones de fondo para realizar.
    Tenga en cuenta que las escrituras se pueden realizar en cualquier réplica que esté disponible y tenga una sesión en ZK, independientemente de si es un líder.
-   `can_become_leader` (`UInt8`) - Si la réplica puede ser elegida como líder.
-   `is_readonly` (`UInt8`) - Si la réplica está en modo de sólo lectura.
    Este modo se activa si la configuración no tiene secciones con ZooKeeper, si se produce un error desconocido al reinicializar sesiones en ZooKeeper y durante la reinicialización de sesiones en ZooKeeper.
-   `is_session_expired` (`UInt8`) - la sesión con ZooKeeper ha expirado. Básicamente lo mismo que `is_readonly`.
-   `future_parts` (`UInt32`) - El número de partes de datos que aparecerán como resultado de INSERTs o fusiones que aún no se han realizado.
-   `parts_to_check` (`UInt32`) - El número de partes de datos en la cola para la verificación. Una pieza se coloca en la cola de verificación si existe la sospecha de que podría estar dañada.
-   `zookeeper_path` (`String`) - Ruta de acceso a los datos de la tabla en ZooKeeper.
-   `replica_name` (`String`) - Nombre de réplica en ZooKeeper. Diferentes réplicas de la misma tabla tienen diferentes nombres.
-   `replica_path` (`String`) - Ruta de acceso a los datos de réplica en ZooKeeper. Lo mismo que concatenar ‘zookeeper\_path/replicas/replica\_path’.
-   `columns_version` (`Int32`) - Número de versión de la estructura de la tabla. Indica cuántas veces se realizó ALTER. Si las réplicas tienen versiones diferentes, significa que algunas réplicas aún no han realizado todas las ALTER.
-   `queue_size` (`UInt32`) - Tamaño de la cola para las operaciones en espera de ser realizadas. Las operaciones incluyen insertar bloques de datos, fusiones y otras acciones. Por lo general, coincide con `future_parts`.
-   `inserts_in_queue` (`UInt32`) - Número de inserciones de bloques de datos que deben realizarse. Las inserciones generalmente se replican con bastante rapidez. Si este número es grande, significa que algo anda mal.
-   `merges_in_queue` (`UInt32`) - El número de fusiones en espera de hacerse. A veces las fusiones son largas, por lo que este valor puede ser mayor que cero durante mucho tiempo.
-   `part_mutations_in_queue` (`UInt32`) - El número de mutaciones a la espera de hacerse.
-   `queue_oldest_time` (`DateTime`) - Si `queue_size` mayor que 0, muestra cuándo se agregó la operación más antigua a la cola.
-   `inserts_oldest_time` (`DateTime`) - Ver `queue_oldest_time`
-   `merges_oldest_time` (`DateTime`) - Ver `queue_oldest_time`
-   `part_mutations_oldest_time` (`DateTime`) - Ver `queue_oldest_time`

Las siguientes 4 columnas tienen un valor distinto de cero solo cuando hay una sesión activa con ZK.

-   `log_max_index` (`UInt64`) - Número máximo de inscripción en el registro de actividad general.
-   `log_pointer` (`UInt64`) - Número máximo de entrada en el registro de actividad general que la réplica copió en su cola de ejecución, más uno. Si `log_pointer` es mucho más pequeño que `log_max_index`, algo está mal.
-   `last_queue_update` (`DateTime`) - Cuando la cola se actualizó la última vez.
-   `absolute_delay` (`UInt64`) - ¿Qué tan grande retraso en segundos tiene la réplica actual.
-   `total_replicas` (`UInt8`) - El número total de réplicas conocidas de esta tabla.
-   `active_replicas` (`UInt8`) - El número de réplicas de esta tabla que tienen una sesión en ZooKeeper (es decir, el número de réplicas en funcionamiento).

Si solicita todas las columnas, la tabla puede funcionar un poco lentamente, ya que se realizan varias lecturas de ZooKeeper para cada fila.
Si no solicita las últimas 4 columnas (log\_max\_index, log\_pointer, total\_replicas, active\_replicas), la tabla funciona rápidamente.

Por ejemplo, puede verificar que todo funcione correctamente de esta manera:

``` sql
SELECT
    database,
    table,
    is_leader,
    is_readonly,
    is_session_expired,
    future_parts,
    parts_to_check,
    columns_version,
    queue_size,
    inserts_in_queue,
    merges_in_queue,
    log_max_index,
    log_pointer,
    total_replicas,
    active_replicas
FROM system.replicas
WHERE
       is_readonly
    OR is_session_expired
    OR future_parts > 20
    OR parts_to_check > 10
    OR queue_size > 20
    OR inserts_in_queue > 10
    OR log_max_index - log_pointer > 10
    OR total_replicas < 2
    OR active_replicas < total_replicas
```

Si esta consulta no devuelve nada, significa que todo está bien.

## sistema.configuración {#system-settings}

Contiene información sobre la configuración actualmente en uso.
Es decir, se usa para ejecutar la consulta que está utilizando para leer del sistema.tabla de configuración.

Columna:

-   `name` (Cadena) — Nombre de configuración.
-   `value` (Cadena) — Valor de ajuste.
-   `changed` (UInt8): si la configuración se definió explícitamente en la configuración o si se cambió explícitamente.

Ejemplo:

``` sql
SELECT *
FROM system.settings
WHERE changed
```

``` text
┌─name───────────────────┬─value───────┬─changed─┐
│ max_threads            │ 8           │       1 │
│ use_uncompressed_cache │ 0           │       1 │
│ load_balancing         │ random      │       1 │
│ max_memory_usage       │ 10000000000 │       1 │
└────────────────────────┴─────────────┴─────────┘
```

## sistema.table\_engines {#system-table-engines}

Contiene la descripción de los motores de tablas admitidos por el servidor y su información de soporte de características.

Esta tabla contiene las siguientes columnas (el tipo de columna se muestra entre corchetes):

-   `name` (Cadena) — El nombre del motor de tabla.
-   `supports_settings` (UInt8): marca que indica si el motor de tabla admite `SETTINGS` clausula.
-   `supports_skipping_indices` (UInt8): marca que indica si el motor de tabla admite [Índices de saltos](table_engines/mergetree/#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8): marca que indica si el motor de tabla admite [TTL](table_engines/mergetree/#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8): marca que indica si el motor de tablas admite cláusulas `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` y `SAMPLE_BY`.
-   `supports_replication` (UInt8): marca que indica si el motor de tabla admite [Replicación de datos](table_engines/replication/).
-   `supports_duduplication` (UInt8): marca que indica si el motor de tablas admite la desduplicación de datos.

Ejemplo:

``` sql
SELECT *
FROM system.table_engines
WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

``` text
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┐
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┘
```

**Ver también**

-   Familia MergeTree [cláusulas de consulta](table_engines/mergetree.md#mergetree-query-clauses)
-   Kafka [configuración](table_engines/kafka.md#table_engine-kafka-creating-a-table)
-   Unir [configuración](table_engines/join.md#join-limitations-and-settings)

## sistema.tabla {#system-tables}

Contiene metadatos de cada tabla que el servidor conoce. Las tablas separadas no se muestran en `system.tables`.

Esta tabla contiene las siguientes columnas (el tipo de columna se muestra entre corchetes):

-   `database` (String) — El nombre de la base de datos en la que se encuentra la tabla.
-   `name` (Cadena) — Nombre de tabla.
-   `engine` (Cadena) — Nombre del motor de tabla (sin parámetros).
-   `is_temporary` (UInt8): marca que indica si la tabla es temporal.
-   `data_path` (String) - Ruta de acceso a los datos de la tabla en el sistema de archivos.
-   `metadata_path` (String) - Ruta de acceso a los metadatos de la tabla en el sistema de archivos.
-   `metadata_modification_time` (DateTime) - Hora de la última modificación de los metadatos de la tabla.
-   `dependencies_database` (Array(String)) - Dependencias de base de datos.
-   `dependencies_table` (Array(String)) - Dependencias de tabla ([Método de codificación de datos:](table_engines/materializedview.md) tablas basadas en la tabla real).
-   `create_table_query` (String) - La consulta que se utilizó para crear la tabla.
-   `engine_full` (String) - Parámetros del motor de tabla.
-   `partition_key` (String) - La expresión de clave de partición especificada en la tabla.
-   `sorting_key` (String) - La expresión de clave de ordenación especificada en la tabla.
-   `primary_key` (String) - La expresión de clave principal especificada en la tabla.
-   `sampling_key` (String) - La expresión de clave de muestreo especificada en la tabla.

El `system.tables` se utiliza en `SHOW TABLES` implementación de consultas.

## sistema.Zookeeper {#system-zookeeper}

La tabla no existe si ZooKeeper no está configurado. Permite leer datos del clúster ZooKeeper definido en la configuración.
La consulta debe tener un ‘path’ condición de igualdad en la cláusula WHERE. Este es el camino en ZooKeeper para los niños para los que desea obtener datos.

Consulta `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` salidas de datos para todos los niños en el `/clickhouse` Nodo.
Para generar datos para todos los nodos raíz, escriba path = ‘/’.
Si la ruta especificada en ‘path’ no existe, se lanzará una excepción.

Columna:

-   `name` (String) — El nombre del nodo.
-   `path` (String) — La ruta al nodo.
-   `value` (Cadena) - el Valor de nodo.
-   `dataLength` (Int32) — Tamaño del valor.
-   `numChildren` (Int32) — Número de descendientes.
-   `czxid` (Int64) — ID de la transacción que creó el nodo.
-   `mzxid` (Int64) — ID de la transacción que cambió el nodo por última vez.
-   `pzxid` (Int64) — ID de la transacción que eliminó o agregó descendientes por última vez.
-   `ctime` (DateTime) — Hora de creación del nodo.
-   `mtime` (DateTime) — Hora de la última modificación del nodo.
-   `version` (Int32) — Versión del nodo: el número de veces que se cambió el nodo.
-   `cversion` (Int32) — Número de descendientes añadidos o eliminados.
-   `aversion` (Int32) — Número de cambios en la ACL.
-   `ephemeralOwner` (Int64): para nodos efímeros, el ID de la sesión que posee este nodo.

Ejemplo:

``` sql
SELECT *
FROM system.zookeeper
WHERE path = '/clickhouse/tables/01-08/visits/replicas'
FORMAT Vertical
```

``` text
Row 1:
──────
name:           example01-08-1.yandex.ru
value:
czxid:          932998691229
mzxid:          932998691229
ctime:          2015-03-27 16:49:51
mtime:          2015-03-27 16:49:51
version:        0
cversion:       47
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021031383
path:           /clickhouse/tables/01-08/visits/replicas

Row 2:
──────
name:           example01-08-2.yandex.ru
value:
czxid:          933002738135
mzxid:          933002738135
ctime:          2015-03-27 16:57:01
mtime:          2015-03-27 16:57:01
version:        0
cversion:       37
aversion:       0
ephemeralOwner: 0
dataLength:     0
numChildren:    7
pzxid:          987021252247
path:           /clickhouse/tables/01-08/visits/replicas
```

## sistema.mutación {#system_tables-mutations}

La tabla contiene información sobre [mutación](../query_language/alter.md#alter-mutations) de las tablas MergeTree y su progreso. Cada comando de mutación está representado por una sola fila. La tabla tiene las siguientes columnas:

**basar**, **tabla** - El nombre de la base de datos y la tabla a la que se aplicó la mutación.

**mutation\_id** - La identificación de la mutación. Para las tablas replicadas, estos identificadores corresponden a los nombres de znode `<table_path_in_zookeeper>/mutations/` Directorio en ZooKeeper. Para las tablas no duplicadas, los ID corresponden a los nombres de archivo en el directorio de datos de la tabla.

**comando** - La cadena de comandos de mutación (la parte de la consulta después de `ALTER TABLE [db.]table`).

**create\_time** - Cuando este comando de mutación fue enviado para su ejecución.

**block\_numbers.partition\_id**, **block\_numbers.número** - Una columna anidada. Para las mutaciones de tablas replicadas, contiene un registro para cada partición: el ID de partición y el número de bloque que fue adquirido por la mutación (en cada partición, solo se mutarán las partes que contienen bloques con números menores que el número de bloque adquirido por la mutación en esa partición). En tablas no replicadas, los números de bloque en todas las particiones forman una sola secuencia. Esto significa que para las mutaciones de tablas no replicadas, la columna contendrá un registro con un solo número de bloque adquirido por la mutación.

**partes\_a\_do** - El número de partes de datos que deben mutarse para que finalice la mutación.

**is\_done** - Es la mutación hecho? Tenga en cuenta que incluso si `parts_to_do = 0` es posible que aún no se haya realizado una mutación de una tabla replicada debido a un INSERT de larga ejecución que creará una nueva parte de datos que deberá mutarse.

Si hubo problemas con la mutación de algunas partes, las siguientes columnas contienen información adicional:

**Método de codificación de datos:** - El nombre de la parte más reciente que no se pudo mutar.

**Método de codificación de datos:** - El momento del fracaso de la mutación de la parte más reciente.

**Método de codificación de datos:** - El mensaje de excepción que causó el error de mutación de parte más reciente.

## sistema.Discoteca {#system_tables-disks}

Contiene información sobre los discos definidos en el [configuración del servidor](table_engines/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columna:

-   `name` ([Cadena](../data_types/string.md)) — Nombre de un disco en la configuración del servidor.
-   `path` ([Cadena](../data_types/string.md)) — Ruta de acceso al punto de montaje en el sistema de archivos.
-   `free_space` ([UInt64](../data_types/int_uint.md)) — Espacio libre en el disco en bytes.
-   `total_space` ([UInt64](../data_types/int_uint.md)) — Volumen del disco en bytes.
-   `keep_free_space` ([UInt64](../data_types/int_uint.md)) — Cantidad de espacio en disco que debe permanecer libre en el disco en bytes. Definido en el `keep_free_space_bytes` parámetro de configuración del disco.

## sistema.almacenamiento\_policies {#system_tables-storage_policies}

Contiene información sobre las directivas de almacenamiento y los volúmenes [configuración del servidor](table_engines/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columna:

-   `policy_name` ([Cadena](../data_types/string.md)) — Nombre de la política de almacenamiento.
-   `volume_name` ([Cadena](../data_types/string.md)) — Nombre de volumen definido en la política de almacenamiento.
-   `volume_priority` ([UInt64](../data_types/int_uint.md)) — Número de orden de volumen en la configuración.
-   `disks` ([Matriz (Cadena)](../data_types/array.md)) — Nombres de disco, definidos en la directiva de almacenamiento.
-   `max_data_part_size` ([UInt64](../data_types/int_uint.md)) — Tamaño máximo de una parte de datos que se puede almacenar en discos de volumen (0 — sin límite).
-   `move_factor` ([Float64](../data_types/float.md)) — Relación de espacio libre en disco. Cuando la relación excede el valor del parámetro de configuración, ClickHouse comienza a mover los datos al siguiente volumen en orden.

Si la directiva de almacenamiento contiene más de un volumen, la información de cada volumen se almacena en la fila individual de la tabla.

[Artículo Original](https://clickhouse.tech/docs/es/operations/system_tables/) <!--hide-->
