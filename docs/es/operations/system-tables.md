---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: Tablas del sistema
---

# Tablas del sistema {#system-tables}

Las tablas del sistema se utilizan para implementar parte de la funcionalidad del sistema y para proporcionar acceso a información sobre cómo funciona el sistema.
No puede eliminar una tabla del sistema (pero puede realizar DETACH).
Las tablas del sistema no tienen archivos con datos en el disco o archivos con metadatos. El servidor crea todas las tablas del sistema cuando se inicia.
Las tablas del sistema son de solo lectura.
Están ubicados en el ‘system’ base.

## sistema.asynchronous_metrics {#system_tables-asynchronous_metrics}

Contiene métricas que se calculan periódicamente en segundo plano. Por ejemplo, la cantidad de RAM en uso.

Columna:

-   `metric` ([Cadena](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Float64](../sql-reference/data-types/float.md)) — Metric value.

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

-   [Monitoreo](monitoring.md) — Base concepts of ClickHouse monitoring.
-   [sistema.métricas](#system_tables-metrics) — Contains instantly calculated metrics.
-   [sistema.evento](#system_tables-events) — Contains a number of events that have occurred.
-   [sistema.metric_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.

## sistema.Cluster {#system-clusters}

Contiene información sobre los clústeres disponibles en el archivo de configuración y los servidores que contienen.

Columna:

-   `cluster` (String) — The cluster name.
-   `shard_num` (UInt32) — The shard number in the cluster, starting from 1.
-   `shard_weight` (UInt32) — The relative weight of the shard when writing data.
-   `replica_num` (UInt32) — The replica number in the shard, starting from 1.
-   `host_name` (String) — The host name, as specified in the config.
-   `host_address` (String) — The host IP address obtained from DNS.
-   `port` (UInt16) — The port to use for connecting to the server.
-   `user` (String) — The name of the user for connecting to the server.
-   `errors_count` (UInt32): número de veces que este host no pudo alcanzar la réplica.
-   `estimated_recovery_time` (UInt32): quedan segundos hasta que el recuento de errores de réplica se ponga a cero y se considere que vuelve a la normalidad.

Tenga en cuenta que `errors_count` se actualiza una vez por consulta al clúster, pero `estimated_recovery_time` se vuelve a calcular bajo demanda. Entonces podría haber un caso distinto de cero `errors_count` y cero `estimated_recovery_time`, esa próxima consulta será cero `errors_count` e intente usar la réplica como si no tuviera errores.

**Ver también**

-   [Motor de tabla distribuido](../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap configuración](settings/settings.md#settings-distributed_replica_error_cap)
-   [distributed_replica_error_half_life configuración](settings/settings.md#settings-distributed_replica_error_half_life)

## sistema.columna {#system-columns}

Contiene información sobre las columnas de todas las tablas.

Puede utilizar esta tabla para obtener información similar a la [DESCRIBE TABLE](../sql-reference/statements/misc.md#misc-describe-table) consulta, pero para varias tablas a la vez.

El `system.columns` tabla contiene las siguientes columnas (el tipo de columna se muestra entre corchetes):

-   `database` (String) — Database name.
-   `table` (String) — Table name.
-   `name` (String) — Column name.
-   `type` (String) — Column type.
-   `default_kind` (String) — Expression type (`DEFAULT`, `MATERIALIZED`, `ALIAS`) para el valor predeterminado, o una cadena vacía si no está definida.
-   `default_expression` (String) — Expression for the default value, or an empty string if it is not defined.
-   `data_compressed_bytes` (UInt64) — The size of compressed data, in bytes.
-   `data_uncompressed_bytes` (UInt64) — The size of decompressed data, in bytes.
-   `marks_bytes` (UInt64) — The size of marks, in bytes.
-   `comment` (String) — Comment on the column, or an empty string if it is not defined.
-   `is_in_partition_key` (UInt8) — Flag that indicates whether the column is in the partition expression.
-   `is_in_sorting_key` (UInt8) — Flag that indicates whether the column is in the sorting key expression.
-   `is_in_primary_key` (UInt8) — Flag that indicates whether the column is in the primary key expression.
-   `is_in_sampling_key` (UInt8) — Flag that indicates whether the column is in the sampling key expression.

## sistema.colaborador {#system-contributors}

Contiene información sobre los colaboradores. Todos los constributores en orden aleatorio. El orden es aleatorio en el momento de la ejecución de la consulta.

Columna:

-   `name` (String) — Contributor (author) name from git log.

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

## sistema.base {#system-databases}

Esta tabla contiene una sola columna String llamada ‘name’ – the name of a database.
Cada base de datos que el servidor conoce tiene una entrada correspondiente en la tabla.
Esta tabla del sistema se utiliza para implementar el `SHOW DATABASES` consulta.

## sistema.detached_parts {#system_tables-detached_parts}

Contiene información sobre piezas separadas de [Método de codificación de datos:](../engines/table-engines/mergetree-family/mergetree.md) tabla. El `reason` columna especifica por qué se separó la pieza. Para las piezas separadas por el usuario, el motivo está vacío. Tales partes se pueden unir con [ALTER TABLE ATTACH PARTITION\|PART](../sql-reference/statements/alter.md#alter_attach-partition) comando. Para obtener la descripción de otras columnas, consulte [sistema.parte](#system_tables-parts). Si el nombre de la pieza no es válido, los valores de algunas columnas pueden ser `NULL`. Tales partes se pueden eliminar con [ALTER TABLE DROP DETACHED PART](../sql-reference/statements/alter.md#alter_drop-detached).

## sistema.diccionario {#system_tables-dictionaries}

Contiene información sobre [diccionarios externos](../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

Columna:

-   `database` ([Cadena](../sql-reference/data-types/string.md)) — Name of the database containing the dictionary created by DDL query. Empty string for other dictionaries.
-   `name` ([Cadena](../sql-reference/data-types/string.md)) — [Nombre del diccionario](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict.md).
-   `status` ([Enum8](../sql-reference/data-types/enum.md)) — Dictionary status. Possible values:
    -   `NOT_LOADED` — Dictionary was not loaded because it was not used.
    -   `LOADED` — Dictionary loaded successfully.
    -   `FAILED` — Unable to load the dictionary as a result of an error.
    -   `LOADING` — Dictionary is loading now.
    -   `LOADED_AND_RELOADING` — Dictionary is loaded successfully, and is being reloaded right now (frequent reasons: [SYSTEM RELOAD DICTIONARY](../sql-reference/statements/system.md#query_language-system-reload-dictionary) consulta, tiempo de espera, configuración del diccionario ha cambiado).
    -   `FAILED_AND_RELOADING` — Could not load the dictionary as a result of an error and is loading now.
-   `origin` ([Cadena](../sql-reference/data-types/string.md)) — Path to the configuration file that describes the dictionary.
-   `type` ([Cadena](../sql-reference/data-types/string.md)) — Type of a dictionary allocation. [Almacenamiento de diccionarios en la memoria](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md).
-   `key` — [Tipo de llave](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-key): Clave numérica ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) or Сomposite key ([Cadena](../sql-reference/data-types/string.md)) — form “(type 1, type 2, …, type n)”.
-   `attribute.names` ([Matriz](../sql-reference/data-types/array.md)([Cadena](../sql-reference/data-types/string.md))) — Array of [nombres de atributos](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) proporcionada por el diccionario.
-   `attribute.types` ([Matriz](../sql-reference/data-types/array.md)([Cadena](../sql-reference/data-types/string.md))) — Corresponding array of [tipos de atributos](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md#ext_dict_structure-attributes) que son proporcionados por el diccionario.
-   `bytes_allocated` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Amount of RAM allocated for the dictionary.
-   `query_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of queries since the dictionary was loaded or since the last successful reboot.
-   `hit_rate` ([Float64](../sql-reference/data-types/float.md)) — For cache dictionaries, the percentage of uses for which the value was in the cache.
-   `element_count` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Number of items stored in the dictionary.
-   `load_factor` ([Float64](../sql-reference/data-types/float.md)) — Percentage filled in the dictionary (for a hashed dictionary, the percentage filled in the hash table).
-   `source` ([Cadena](../sql-reference/data-types/string.md)) — Text describing the [fuente de datos](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md) para el diccionario.
-   `lifetime_min` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Minimum [vida](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) del diccionario en la memoria, después de lo cual ClickHouse intenta volver a cargar el diccionario (si `invalidate_query` está configurado, entonces solo si ha cambiado). Establecer en segundos.
-   `lifetime_max` ([UInt64](../sql-reference/data-types/int-uint.md#uint-ranges)) — Maximum [vida](../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md) del diccionario en la memoria, después de lo cual ClickHouse intenta volver a cargar el diccionario (si `invalidate_query` está configurado, entonces solo si ha cambiado). Establecer en segundos.
-   `loading_start_time` ([FechaHora](../sql-reference/data-types/datetime.md)) — Start time for loading the dictionary.
-   `last_successful_update_time` ([FechaHora](../sql-reference/data-types/datetime.md)) — End time for loading or updating the dictionary. Helps to monitor some troubles with external sources and investigate causes.
-   `loading_duration` ([Float32](../sql-reference/data-types/float.md)) — Duration of a dictionary loading.
-   `last_exception` ([Cadena](../sql-reference/data-types/string.md)) — Text of the error that occurs when creating or reloading the dictionary if the dictionary couldn't be created.

**Ejemplo**

Configurar el diccionario.

``` sql
CREATE DICTIONARY dictdb.dict
(
    `key` Int64 DEFAULT -1,
    `value_default` String DEFAULT 'world',
    `value_expression` String DEFAULT 'xxx' EXPRESSION 'toString(127 * 172)'
)
PRIMARY KEY key
SOURCE(CLICKHOUSE(HOST 'localhost' PORT 9000 USER 'default' TABLE 'dicttbl' DB 'dictdb'))
LIFETIME(MIN 0 MAX 1)
LAYOUT(FLAT())
```

Asegúrese de que el diccionario esté cargado.

``` sql
SELECT * FROM system.dictionaries
```

``` text
┌─database─┬─name─┬─status─┬─origin──────┬─type─┬─key────┬─attribute.names──────────────────────┬─attribute.types─────┬─bytes_allocated─┬─query_count─┬─hit_rate─┬─element_count─┬───────────load_factor─┬─source─────────────────────┬─lifetime_min─┬─lifetime_max─┬──loading_start_time─┌──last_successful_update_time─┬──────loading_duration─┬─last_exception─┐
│ dictdb   │ dict │ LOADED │ dictdb.dict │ Flat │ UInt64 │ ['value_default','value_expression'] │ ['String','String'] │           74032 │           0 │        1 │             1 │ 0.0004887585532746823 │ ClickHouse: dictdb.dicttbl │            0 │            1 │ 2020-03-04 04:17:34 │   2020-03-04 04:30:34        │                 0.002 │                │
└──────────┴──────┴────────┴─────────────┴──────┴────────┴──────────────────────────────────────┴─────────────────────┴─────────────────┴─────────────┴──────────┴───────────────┴───────────────────────┴────────────────────────────┴──────────────┴──────────────┴─────────────────────┴──────────────────────────────┘───────────────────────┴────────────────┘
```

## sistema.evento {#system_tables-events}

Contiene información sobre el número de eventos que se han producido en el sistema. Por ejemplo, en la tabla, puede encontrar cuántos `SELECT` las consultas se procesaron desde que se inició el servidor ClickHouse.

Columna:

-   `event` ([Cadena](../sql-reference/data-types/string.md)) — Event name.
-   `value` ([UInt64](../sql-reference/data-types/int-uint.md)) — Number of events occurred.
-   `description` ([Cadena](../sql-reference/data-types/string.md)) — Event description.

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

-   [sistema.asynchronous_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [sistema.métricas](#system_tables-metrics) — Contains instantly calculated metrics.
-   [sistema.metric_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [Monitoreo](monitoring.md) — Base concepts of ClickHouse monitoring.

## sistema.función {#system-functions}

Contiene información sobre funciones normales y agregadas.

Columna:

-   `name`(`String`) – The name of the function.
-   `is_aggregate`(`UInt8`) — Whether the function is aggregate.

## sistema.graphite_retentions {#system-graphite-retentions}

Contiene información sobre los parámetros [graphite_rollup](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) que se utilizan en tablas con [\*GraphiteMergeTree](../engines/table-engines/mergetree-family/graphitemergetree.md) motor.

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

## sistema.fusionar {#system-merges}

Contiene información sobre fusiones y mutaciones de piezas actualmente en proceso para tablas de la familia MergeTree.

Columna:

-   `database` (String) — The name of the database the table is in.
-   `table` (String) — Table name.
-   `elapsed` (Float64) — The time elapsed (in seconds) since the merge started.
-   `progress` (Float64) — The percentage of completed work from 0 to 1.
-   `num_parts` (UInt64) — The number of pieces to be merged.
-   `result_part_name` (String) — The name of the part that will be formed as the result of merging.
-   `is_mutation` (UInt8) - 1 si este proceso es una mutación parte.
-   `total_size_bytes_compressed` (UInt64) — The total size of the compressed data in the merged chunks.
-   `total_size_marks` (UInt64) — The total number of marks in the merged parts.
-   `bytes_read_uncompressed` (UInt64) — Number of bytes read, uncompressed.
-   `rows_read` (UInt64) — Number of rows read.
-   `bytes_written_uncompressed` (UInt64) — Number of bytes written, uncompressed.
-   `rows_written` (UInt64) — Number of rows written.

## sistema.métricas {#system_tables-metrics}

Contiene métricas que pueden calcularse instantáneamente o tener un valor actual. Por ejemplo, el número de consultas procesadas simultáneamente o el retraso de réplica actual. Esta tabla está siempre actualizada.

Columna:

-   `metric` ([Cadena](../sql-reference/data-types/string.md)) — Metric name.
-   `value` ([Int64](../sql-reference/data-types/int-uint.md)) — Metric value.
-   `description` ([Cadena](../sql-reference/data-types/string.md)) — Metric description.

La lista de métricas admitidas que puede encontrar en el [src/Common/CurrentMetrics.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/CurrentMetrics.cpp) archivo fuente de ClickHouse.

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

-   [sistema.asynchronous_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [sistema.evento](#system_tables-events) — Contains a number of events that occurred.
-   [sistema.metric_log](#system_tables-metric_log) — Contains a history of metrics values from tables `system.metrics` и `system.events`.
-   [Monitoreo](monitoring.md) — Base concepts of ClickHouse monitoring.

## sistema.metric_log {#system_tables-metric_log}

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

-   [sistema.asynchronous_metrics](#system_tables-asynchronous_metrics) — Contains periodically calculated metrics.
-   [sistema.evento](#system_tables-events) — Contains a number of events that occurred.
-   [sistema.métricas](#system_tables-metrics) — Contains instantly calculated metrics.
-   [Monitoreo](monitoring.md) — Base concepts of ClickHouse monitoring.

## sistema.numero {#system-numbers}

Esta tabla contiene una única columna UInt64 llamada ‘number’ que contiene casi todos los números naturales a partir de cero.
Puede usar esta tabla para pruebas, o si necesita hacer una búsqueda de fuerza bruta.
Las lecturas de esta tabla no están paralelizadas.

## sistema.Números_mt {#system-numbers-mt}

Lo mismo que ‘system.numbers’ pero las lecturas están paralelizadas. Los números se pueden devolver en cualquier orden.
Se utiliza para pruebas.

## sistema.una {#system-one}

Esta tabla contiene una sola fila con una ‘dummy’ Columna UInt8 que contiene el valor 0.
Esta tabla se utiliza si una consulta SELECT no especifica la cláusula FROM.
Esto es similar a la tabla DUAL que se encuentra en otros DBMS.

## sistema.parte {#system_tables-parts}

Contiene información sobre partes de [Método de codificación de datos:](../engines/table-engines/mergetree-family/mergetree.md) tabla.

Cada fila describe una parte de datos.

Columna:

-   `partition` (String) – The partition name. To learn what a partition is, see the description of the [ALTER](../sql-reference/statements/alter.md#query_language_queries_alter) consulta.

    Formato:

    -   `YYYYMM` para la partición automática por mes.
    -   `any_string` al particionar manualmente.

-   `name` (`String`) – Name of the data part.

-   `active` (`UInt8`) – Flag that indicates whether the data part is active. If a data part is active, it's used in a table. Otherwise, it's deleted. Inactive data parts remain after merging.

-   `marks` (`UInt64`) – The number of marks. To get the approximate number of rows in a data part, multiply `marks` por la granularidad del índice (generalmente 8192) (esta sugerencia no funciona para la granularidad adaptativa).

-   `rows` (`UInt64`) – The number of rows.

-   `bytes_on_disk` (`UInt64`) – Total size of all the data part files in bytes.

-   `data_compressed_bytes` (`UInt64`) – Total size of compressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `data_uncompressed_bytes` (`UInt64`) – Total size of uncompressed data in the data part. All the auxiliary files (for example, files with marks) are not included.

-   `marks_bytes` (`UInt64`) – The size of the file with marks.

-   `modification_time` (`DateTime`) – The time the directory with the data part was modified. This usually corresponds to the time of data part creation.\|

-   `remove_time` (`DateTime`) – The time when the data part became inactive.

-   `refcount` (`UInt32`) – The number of places where the data part is used. A value greater than 2 indicates that the data part is used in queries or merges.

-   `min_date` (`Date`) – The minimum value of the date key in the data part.

-   `max_date` (`Date`) – The maximum value of the date key in the data part.

-   `min_time` (`DateTime`) – The minimum value of the date and time key in the data part.

-   `max_time`(`DateTime`) – The maximum value of the date and time key in the data part.

-   `partition_id` (`String`) – ID of the partition.

-   `min_block_number` (`UInt64`) – The minimum number of data parts that make up the current part after merging.

-   `max_block_number` (`UInt64`) – The maximum number of data parts that make up the current part after merging.

-   `level` (`UInt32`) – Depth of the merge tree. Zero means that the current part was created by insert rather than by merging other parts.

-   `data_version` (`UInt64`) – Number that is used to determine which mutations should be applied to the data part (mutations with a version higher than `data_version`).

-   `primary_key_bytes_in_memory` (`UInt64`) – The amount of memory (in bytes) used by primary key values.

-   `primary_key_bytes_in_memory_allocated` (`UInt64`) – The amount of memory (in bytes) reserved for primary key values.

-   `is_frozen` (`UInt8`) – Flag that shows that a partition data backup exists. 1, the backup exists. 0, the backup doesn't exist. For more details, see [FREEZE PARTITION](../sql-reference/statements/alter.md#alter_freeze-partition)

-   `database` (`String`) – Name of the database.

-   `table` (`String`) – Name of the table.

-   `engine` (`String`) – Name of the table engine without parameters.

-   `path` (`String`) – Absolute path to the folder with data part files.

-   `disk` (`String`) – Name of a disk that stores the data part.

-   `hash_of_all_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) de archivos comprimidos.

-   `hash_of_uncompressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) de archivos sin comprimir (archivos con marcas, archivo de índice, etc.).

-   `uncompressed_hash_of_compressed_files` (`String`) – [sipHash128](../sql-reference/functions/hash-functions.md#hash_functions-siphash128) de datos en los archivos comprimidos como si estuvieran descomprimidos.

-   `bytes` (`UInt64`) – Alias for `bytes_on_disk`.

-   `marks_size` (`UInt64`) – Alias for `marks_bytes`.

## sistema.part_log {#system_tables-part-log}

El `system.part_log` se crea sólo si el [part_log](server-configuration-parameters/settings.md#server_configuration_parameters-part-log) se especifica la configuración del servidor.

Esta tabla contiene información sobre eventos que ocurrieron con [partes de datos](../engines/table-engines/mergetree-family/custom-partitioning-key.md) en el [Método de codificación de datos:](../engines/table-engines/mergetree-family/mergetree.md) tablas familiares, como agregar o fusionar datos.

El `system.part_log` contiene las siguientes columnas:

-   `event_type` (Enum) — Type of the event that occurred with the data part. Can have one of the following values:
    -   `NEW_PART` — Inserting of a new data part.
    -   `MERGE_PARTS` — Merging of data parts.
    -   `DOWNLOAD_PART` — Downloading a data part.
    -   `REMOVE_PART` — Removing or detaching a data part using [DETACH PARTITION](../sql-reference/statements/alter.md#alter_detach-partition).
    -   `MUTATE_PART` — Mutating of a data part.
    -   `MOVE_PART` — Moving the data part from the one disk to another one.
-   `event_date` (Date) — Event date.
-   `event_time` (DateTime) — Event time.
-   `duration_ms` (UInt64) — Duration.
-   `database` (String) — Name of the database the data part is in.
-   `table` (String) — Name of the table the data part is in.
-   `part_name` (String) — Name of the data part.
-   `partition_id` (String) — ID of the partition that the data part was inserted to. The column takes the ‘all’ valor si la partición es por `tuple()`.
-   `rows` (UInt64) — The number of rows in the data part.
-   `size_in_bytes` (UInt64) — Size of the data part in bytes.
-   `merged_from` (Array(String)) — An array of names of the parts which the current part was made up from (after the merge).
-   `bytes_uncompressed` (UInt64) — Size of uncompressed bytes.
-   `read_rows` (UInt64) — The number of rows was read during the merge.
-   `read_bytes` (UInt64) — The number of bytes was read during the merge.
-   `error` (UInt16) — The code number of the occurred error.
-   `exception` (String) — Text message of the occurred error.

El `system.part_log` se crea después de la primera inserción de datos `MergeTree` tabla.

## sistema.procesa {#system_tables-processes}

Esta tabla del sistema se utiliza para implementar el `SHOW PROCESSLIST` consulta.

Columna:

-   `user` (String) – The user who made the query. Keep in mind that for distributed processing, queries are sent to remote servers under the `default` usuario. El campo contiene el nombre de usuario para una consulta específica, no para una consulta que esta consulta inició.
-   `address` (String) – The IP address the request was made from. The same for distributed processing. To track where a distributed query was originally made from, look at `system.processes` en el servidor de solicitud de consulta.
-   `elapsed` (Float64) – The time in seconds since request execution started.
-   `rows_read` (UInt64) – The number of rows read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `bytes_read` (UInt64) – The number of uncompressed bytes read from the table. For distributed processing, on the requestor server, this is the total for all remote servers.
-   `total_rows_approx` (UInt64) – The approximation of the total number of rows that should be read. For distributed processing, on the requestor server, this is the total for all remote servers. It can be updated during request processing, when new sources to process become known.
-   `memory_usage` (UInt64) – Amount of RAM the request uses. It might not include some types of dedicated memory. See the [Método de codificación de datos:](../operations/settings/query-complexity.md#settings_max_memory_usage) configuración.
-   `query` (String) – The query text. For `INSERT`, no incluye los datos para insertar.
-   `query_id` (String) – Query ID, if defined.

## sistema.text_log {#system_tables-text_log}

Contiene entradas de registro. El nivel de registro que va a esta tabla se puede limitar con `text_log.level` configuración del servidor.

Columna:

-   `event_date` (`Date`) - Fecha de la entrada.
-   `event_time` (`DateTime`) - Hora de la entrada.
-   `microseconds` (`UInt32`) - Microsegundos de la entrada.
-   `thread_name` (String) — Name of the thread from which the logging was done.
-   `thread_id` (UInt64) — OS thread ID.
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
-   `logger_name` (`LowCardinality(String)`) - Name of the logger (i.e. `DDLWorker`)
-   `message` (`String`) - El mensaje en sí.
-   `revision` (`UInt32`) - Revisión de ClickHouse.
-   `source_file` (`LowCardinality(String)`) - Archivo de origen desde el que se realizó el registro.
-   `source_line` (`UInt64`) - Línea de origen desde la que se realizó el registro.

## sistema.query_log {#system_tables-query_log}

Contiene información sobre la ejecución de consultas. Para cada consulta, puede ver la hora de inicio del procesamiento, la duración del procesamiento, los mensajes de error y otra información.

!!! note "Nota"
    La tabla no contiene datos de entrada para `INSERT` consulta.

ClickHouse crea esta tabla sólo si el [query_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) se especifica el parámetro server. Este parámetro establece las reglas de registro, como el intervalo de registro o el nombre de la tabla en la que se registrarán las consultas.

Para habilitar el registro de consultas, [Log_queries](settings/settings.md#settings-log-queries) parámetro a 1. Para obtener más información, consulte el [Configuración](settings/settings.md) apartado.

El `system.query_log` tabla registra dos tipos de consultas:

1.  Consultas iniciales ejecutadas directamente por el cliente.
2.  Consultas secundarias iniciadas por otras consultas (para la ejecución de consultas distribuidas). Para estos tipos de consultas, la información sobre las consultas principales se muestra en el `initial_*` columna.

Columna:

-   `type` (`Enum8`) — Type of event that occurred when executing the query. Values:
    -   `'QueryStart' = 1` — Successful start of query execution.
    -   `'QueryFinish' = 2` — Successful end of query execution.
    -   `'ExceptionBeforeStart' = 3` — Exception before the start of query execution.
    -   `'ExceptionWhileProcessing' = 4` — Exception during the query execution.
-   `event_date` (Date) — Query starting date.
-   `event_time` (DateTime) — Query starting time.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` consultas, el número de filas escritas. Para otras consultas, el valor de la columna es 0.
-   `written_bytes` (UInt64) — For `INSERT` consultas, el número de bytes escritos. Para otras consultas, el valor de la columna es 0.
-   `result_rows` (UInt64) — Number of rows in the result.
-   `result_bytes` (UInt64) — Number of bytes in the result.
-   `memory_usage` (UInt64) — Memory consumption by the query.
-   `query` (String) — Query string.
-   `exception` (String) — Exception message.
-   `stack_trace` (String) — Stack trace (a list of methods called before the error occurred). An empty string, if the query is completed successfully.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [Casa de clics-cliente](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [Casa de clics-cliente](../interfaces/cli.md) o se ejecuta otro cliente TCP.
-   `client_name` (String) — The [Casa de clics-cliente](../interfaces/cli.md) o otro nombre de cliente TCP.
-   `client_revision` (UInt32) — Revision of the [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_major` (UInt32) — Major version of the [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_minor` (UInt32) — Minor version of the [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_patch` (UInt32) — Patch component of the [Casa de clics-cliente](../interfaces/cli.md) o otra versión de cliente TCP.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` se utilizó el método.
    -   2 — `POST` se utilizó el método.
-   `http_user_agent` (String) — The `UserAgent` encabezado pasado en la solicitud HTTP.
-   `quota_key` (String) — The “quota key” especificado en el [cuota](quotas.md) ajuste (ver `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `thread_numbers` (Array(UInt32)) — Number of threads that are participating in query execution.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics. The description of them could be found in the table [sistema.evento](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics that are listed in the `ProfileEvents.Names` columna.
-   `Settings.Names` (Array(String)) — Names of settings that were changed when the client ran the query. To enable logging changes to settings, set the `log_query_settings` parámetro a 1.
-   `Settings.Values` (Array(String)) — Values of settings that are listed in the `Settings.Names` columna.

Cada consulta crea una o dos filas en el `query_log` tabla, dependiendo del estado de la consulta:

1.  Si la ejecución de la consulta se realiza correctamente, se crean dos eventos con los tipos 1 y 2 (consulte `type` columna).
2.  Si se produjo un error durante el procesamiento de la consulta, se crean dos eventos con los tipos 1 y 4.
3.  Si se produjo un error antes de iniciar la consulta, se crea un solo evento con el tipo 3.

De forma predeterminada, los registros se agregan a la tabla a intervalos de 7,5 segundos. Puede establecer este intervalo en el [query_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) configuración del servidor (consulte el `flush_interval_milliseconds` parámetro). Para vaciar los registros a la fuerza desde el búfer de memoria a la tabla, utilice `SYSTEM FLUSH LOGS` consulta.

Cuando la tabla se elimina manualmente, se creará automáticamente sobre la marcha. Tenga en cuenta que se eliminarán todos los registros anteriores.

!!! note "Nota"
    El período de almacenamiento para los registros es ilimitado. Los registros no se eliminan automáticamente de la tabla. Debe organizar la eliminación de registros obsoletos usted mismo.

Puede especificar una clave de partición arbitraria `system.query_log` mesa en el [query_log](server-configuration-parameters/settings.md#server_configuration_parameters-query-log) configuración del servidor (consulte el `partition_by` parámetro).

## sistema.Sistema abierto {#system_tables-query-thread-log}

La tabla contiene información sobre cada subproceso de ejecución de consultas.

ClickHouse crea esta tabla sólo si el [Sistema abierto.](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) se especifica el parámetro server. Este parámetro establece las reglas de registro, como el intervalo de registro o el nombre de la tabla en la que se registrarán las consultas.

Para habilitar el registro de consultas, [Log_query_threads](settings/settings.md#settings-log-query-threads) parámetro a 1. Para obtener más información, consulte el [Configuración](settings/settings.md) apartado.

Columna:

-   `event_date` (Date) — the date when the thread has finished execution of the query.
-   `event_time` (DateTime) — the date and time when the thread has finished execution of the query.
-   `query_start_time` (DateTime) — Start time of query execution.
-   `query_duration_ms` (UInt64) — Duration of query execution.
-   `read_rows` (UInt64) — Number of read rows.
-   `read_bytes` (UInt64) — Number of read bytes.
-   `written_rows` (UInt64) — For `INSERT` consultas, el número de filas escritas. Para otras consultas, el valor de la columna es 0.
-   `written_bytes` (UInt64) — For `INSERT` consultas, el número de bytes escritos. Para otras consultas, el valor de la columna es 0.
-   `memory_usage` (Int64) — The difference between the amount of allocated and freed memory in context of this thread.
-   `peak_memory_usage` (Int64) — The maximum difference between the amount of allocated and freed memory in context of this thread.
-   `thread_name` (String) — Name of the thread.
-   `thread_number` (UInt32) — Internal thread ID.
-   `os_thread_id` (Int32) — OS thread ID.
-   `master_thread_id` (UInt64) — OS initial ID of initial thread.
-   `query` (String) — Query string.
-   `is_initial_query` (UInt8) — Query type. Possible values:
    -   1 — Query was initiated by the client.
    -   0 — Query was initiated by another query for distributed query execution.
-   `user` (String) — Name of the user who initiated the current query.
-   `query_id` (String) — ID of the query.
-   `address` (IPv6) — IP address that was used to make the query.
-   `port` (UInt16) — The client port that was used to make the query.
-   `initial_user` (String) — Name of the user who ran the initial query (for distributed query execution).
-   `initial_query_id` (String) — ID of the initial query (for distributed query execution).
-   `initial_address` (IPv6) — IP address that the parent query was launched from.
-   `initial_port` (UInt16) — The client port that was used to make the parent query.
-   `interface` (UInt8) — Interface that the query was initiated from. Possible values:
    -   1 — TCP.
    -   2 — HTTP.
-   `os_user` (String) — OS's username who runs [Casa de clics-cliente](../interfaces/cli.md).
-   `client_hostname` (String) — Hostname of the client machine where the [Casa de clics-cliente](../interfaces/cli.md) o se ejecuta otro cliente TCP.
-   `client_name` (String) — The [Casa de clics-cliente](../interfaces/cli.md) o otro nombre de cliente TCP.
-   `client_revision` (UInt32) — Revision of the [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_major` (UInt32) — Major version of the [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_minor` (UInt32) — Minor version of the [Casa de clics-cliente](../interfaces/cli.md) o otro cliente TCP.
-   `client_version_patch` (UInt32) — Patch component of the [Casa de clics-cliente](../interfaces/cli.md) o otra versión de cliente TCP.
-   `http_method` (UInt8) — HTTP method that initiated the query. Possible values:
    -   0 — The query was launched from the TCP interface.
    -   1 — `GET` se utilizó el método.
    -   2 — `POST` se utilizó el método.
-   `http_user_agent` (String) — The `UserAgent` encabezado pasado en la solicitud HTTP.
-   `quota_key` (String) — The “quota key” especificado en el [cuota](quotas.md) ajuste (ver `keyed`).
-   `revision` (UInt32) — ClickHouse revision.
-   `ProfileEvents.Names` (Array(String)) — Counters that measure different metrics for this thread. The description of them could be found in the table [sistema.evento](#system_tables-events)
-   `ProfileEvents.Values` (Array(UInt64)) — Values of metrics for this thread that are listed in the `ProfileEvents.Names` columna.

De forma predeterminada, los registros se agregan a la tabla a intervalos de 7,5 segundos. Puede establecer este intervalo en el [Sistema abierto.](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) configuración del servidor (consulte el `flush_interval_milliseconds` parámetro). Para vaciar los registros a la fuerza desde el búfer de memoria a la tabla, utilice `SYSTEM FLUSH LOGS` consulta.

Cuando la tabla se elimina manualmente, se creará automáticamente sobre la marcha. Tenga en cuenta que se eliminarán todos los registros anteriores.

!!! note "Nota"
    El período de almacenamiento para los registros es ilimitado. Los registros no se eliminan automáticamente de la tabla. Debe organizar la eliminación de registros obsoletos usted mismo.

Puede especificar una clave de partición arbitraria `system.query_thread_log` mesa en el [Sistema abierto.](server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) configuración del servidor (consulte el `partition_by` parámetro).

## sistema.trace_log {#system_tables-trace_log}

Contiene seguimientos de pila recopilados por el generador de perfiles de consultas de muestreo.

ClickHouse crea esta tabla cuando el [trace_log](server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) se establece la sección de configuración del servidor. También el [query_profiler_real_time_period_ns](settings/settings.md#query_profiler_real_time_period_ns) y [Los resultados de la prueba](settings/settings.md#query_profiler_cpu_time_period_ns) los ajustes deben establecerse.

Para analizar los registros, utilice el `addressToLine`, `addressToSymbol` y `demangle` funciones de inspección.

Columna:

-   `event_date` ([Fecha](../sql-reference/data-types/date.md)) — Date of sampling moment.

-   `event_time` ([FechaHora](../sql-reference/data-types/datetime.md)) — Timestamp of the sampling moment.

-   `timestamp_ns` ([UInt64](../sql-reference/data-types/int-uint.md)) — Timestamp of the sampling moment in nanoseconds.

-   `revision` ([UInt32](../sql-reference/data-types/int-uint.md)) — ClickHouse server build revision.

    Cuando se conecta al servidor por `clickhouse-client`, ves la cadena similar a `Connected to ClickHouse server version 19.18.1 revision 54429.`. Este campo contiene el `revision`, pero no el `version` de un servidor.

-   `timer_type` ([Enum8](../sql-reference/data-types/enum.md)) — Timer type:

    -   `Real` representa el tiempo del reloj de pared.
    -   `CPU` representa el tiempo de CPU.

-   `thread_number` ([UInt32](../sql-reference/data-types/int-uint.md)) — Thread identifier.

-   `query_id` ([Cadena](../sql-reference/data-types/string.md)) — Query identifier that can be used to get details about a query that was running from the [query_log](#system_tables-query_log) tabla del sistema.

-   `trace` ([Matriz (UInt64)](../sql-reference/data-types/array.md)) — Stack trace at the moment of sampling. Each element is a virtual memory address inside ClickHouse server process.

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
inserts_oldest_time:        1970-01-01 00:00:00
merges_oldest_time:         2020-02-20 08:34:30
part_mutations_oldest_time: 1970-01-01 00:00:00
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
-   `replica_path` (`String`) - Ruta de acceso a los datos de réplica en ZooKeeper. Lo mismo que concatenar ‘zookeeper_path/replicas/replica_path’.
-   `columns_version` (`Int32`) - Número de versión de la estructura de la tabla. Indica cuántas veces se realizó ALTER. Si las réplicas tienen versiones diferentes, significa que algunas réplicas aún no han hecho todas las ALTER.
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
Si no solicita las últimas 4 columnas (log_max_index, log_pointer, total_replicas, active_replicas), la tabla funciona rápidamente.

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

## sistema.configuración {#system-tables-system-settings}

Contiene información sobre la configuración de sesión para el usuario actual.

Columna:

-   `name` ([Cadena](../sql-reference/data-types/string.md)) — Setting name.
-   `value` ([Cadena](../sql-reference/data-types/string.md)) — Setting value.
-   `changed` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
-   `description` ([Cadena](../sql-reference/data-types/string.md)) — Short setting description.
-   `min` ([NULL](../sql-reference/data-types/nullable.md)([Cadena](../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [limitación](settings/constraints-on-settings.md#constraints-on-settings). Si la configuración no tiene ningún valor mínimo, contiene [NULL](../sql-reference/syntax.md#null-literal).
-   `max` ([NULL](../sql-reference/data-types/nullable.md)([Cadena](../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [limitación](settings/constraints-on-settings.md#constraints-on-settings). Si la configuración no tiene ningún valor máximo, contiene [NULL](../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    -   `0` — Current user can change the setting.
    -   `1` — Current user can't change the setting.

**Ejemplo**

En el ejemplo siguiente se muestra cómo obtener información sobre la configuración cuyo nombre contiene `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_i%'
```

``` text
┌─name────────────────────────────────────────┬─value─────┬─changed─┬─description───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─min──┬─max──┬─readonly─┐
│ min_insert_block_size_rows                  │ 1048576   │       0 │ Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.                                                                         │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ min_insert_block_size_bytes                 │ 268435456 │       0 │ Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.                                                                        │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ read_backoff_min_interval_between_events_ms │ 1000      │       0 │ Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time. │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
└─────────────────────────────────────────────┴───────────┴─────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────┴──────┴──────────┘
```

Uso de `WHERE changed` puede ser útil, por ejemplo, cuando se desea comprobar:

-   Si los ajustes de los archivos de configuración se cargan correctamente y están en uso.
-   Configuración que cambió en la sesión actual.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**Ver también**

-   [Configuración](settings/index.md#session-settings-intro)
-   [Permisos para consultas](settings/permissions-for-queries.md#settings_readonly)
-   [Restricciones en la configuración](settings/constraints-on-settings.md)

## sistema.table_engines {#system.table_engines}

``` text
┌─name───────────────────┬─value───────┐
│ max_threads            │ 8           │
│ use_uncompressed_cache │ 0           │
│ load_balancing         │ random      │
│ max_memory_usage       │ 10000000000 │
└────────────────────────┴─────────────┘
```

## sistema.merge_tree_settings {#system-merge_tree_settings}

Contiene información sobre la configuración `MergeTree` tabla.

Columna:

-   `name` (String) — Setting name.
-   `value` (String) — Setting value.
-   `description` (String) — Setting description.
-   `type` (String) — Setting type (implementation specific string value).
-   `changed` (UInt8) — Whether the setting was explicitly defined in the config or explicitly changed.

## sistema.table_engines {#system-table-engines}

Contiene la descripción de los motores de tablas admitidos por el servidor y su información de soporte de características.

Esta tabla contiene las siguientes columnas (el tipo de columna se muestra entre corchetes):

-   `name` (String) — The name of table engine.
-   `supports_settings` (UInt8) — Flag that indicates if table engine supports `SETTINGS` clausula.
-   `supports_skipping_indices` (UInt8) — Flag that indicates if table engine supports [Índices de saltos](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — Flag that indicates if table engine supports [TTL](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — Flag that indicates if table engine supports clauses `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` y `SAMPLE_BY`.
-   `supports_replication` (UInt8) — Flag that indicates if table engine supports [Replicación de datos](../engines/table-engines/mergetree-family/replication.md).
-   `supports_duduplication` (UInt8) — Flag that indicates if table engine supports data deduplication.

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

-   Familia MergeTree [cláusulas de consulta](../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
-   Kafka [configuración](../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table)
-   Unir [configuración](../engines/table-engines/special/join.md#join-limitations-and-settings)

## sistema.tabla {#system-tables}

Contiene metadatos de cada tabla que el servidor conoce. Las tablas separadas no se muestran en `system.tables`.

Esta tabla contiene las siguientes columnas (el tipo de columna se muestra entre corchetes):

-   `database` (String) — The name of the database the table is in.

-   `name` (String) — Table name.

-   `engine` (String) — Table engine name (without parameters).

-   `is_temporary` (UInt8): marca que indica si la tabla es temporal.

-   `data_path` (String) - Ruta de acceso a los datos de la tabla en el sistema de archivos.

-   `metadata_path` (String) - Ruta de acceso a los metadatos de la tabla en el sistema de archivos.

-   `metadata_modification_time` (DateTime) - Hora de la última modificación de los metadatos de la tabla.

-   `dependencies_database` (Array(String)) - Dependencias de base de datos.

-   `dependencies_table` (Array(String)) - Dependencias de tabla ([Método de codificación de datos:](../engines/table-engines/special/materializedview.md) tablas basadas en la tabla actual).

-   `create_table_query` (String) - La consulta que se utilizó para crear la tabla.

-   `engine_full` (String) - Parámetros del motor de tabla.

-   `partition_key` (String) - La expresión de clave de partición especificada en la tabla.

-   `sorting_key` (String) - La expresión de clave de ordenación especificada en la tabla.

-   `primary_key` (String) - La expresión de clave principal especificada en la tabla.

-   `sampling_key` (String) - La expresión de clave de muestreo especificada en la tabla.

-   `storage_policy` (String) - La política de almacenamiento:

    -   [Método de codificación de datos:](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)
    -   [Distribuido](../engines/table-engines/special/distributed.md#distributed)

-   `total_rows` (Nullable(UInt64)) - Número total de filas, si es posible determinar rápidamente el número exacto de filas en la tabla, de lo contrario `Null` (incluyendo underying `Buffer` tabla).

-   `total_bytes` (Nullable(UInt64)) - Número total de bytes, si es posible determinar rápidamente el número exacto de bytes para la tabla en el almacenamiento, de lo contrario `Null` (**no** incluye cualquier almacenamiento subyacente).

    -   If the table stores data on disk, returns used space on disk (i.e. compressed).
    -   Si la tabla almacena datos en la memoria, devuelve el número aproximado de bytes utilizados en la memoria.

El `system.tables` se utiliza en `SHOW TABLES` implementación de consultas.

## sistema.Zookeeper {#system-zookeeper}

La tabla no existe si ZooKeeper no está configurado. Permite leer datos del clúster ZooKeeper definido en la configuración.
La consulta debe tener un ‘path’ condición de igualdad en la cláusula WHERE. Este es el camino en ZooKeeper para los niños para los que desea obtener datos.

Consulta `SELECT * FROM system.zookeeper WHERE path = '/clickhouse'` salidas de datos para todos los niños en el `/clickhouse` nodo.
Para generar datos para todos los nodos raíz, escriba path = ‘/’.
Si la ruta especificada en ‘path’ no existe, se lanzará una excepción.

Columna:

-   `name` (String) — The name of the node.
-   `path` (String) — The path to the node.
-   `value` (String) — Node value.
-   `dataLength` (Int32) — Size of the value.
-   `numChildren` (Int32) — Number of descendants.
-   `czxid` (Int64) — ID of the transaction that created the node.
-   `mzxid` (Int64) — ID of the transaction that last changed the node.
-   `pzxid` (Int64) — ID of the transaction that last deleted or added descendants.
-   `ctime` (DateTime) — Time of node creation.
-   `mtime` (DateTime) — Time of the last modification of the node.
-   `version` (Int32) — Node version: the number of times the node was changed.
-   `cversion` (Int32) — Number of added or removed descendants.
-   `aversion` (Int32) — Number of changes to the ACL.
-   `ephemeralOwner` (Int64) — For ephemeral nodes, the ID of the session that owns this node.

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

La tabla contiene información sobre [mutación](../sql-reference/statements/alter.md#alter-mutations) de las tablas MergeTree y su progreso. Cada comando de mutación está representado por una sola fila. La tabla tiene las siguientes columnas:

**base**, **tabla** - El nombre de la base de datos y la tabla a la que se aplicó la mutación.

**mutation_id** - La identificación de la mutación. Para las tablas replicadas, estos identificadores corresponden a los nombres de znode `<table_path_in_zookeeper>/mutations/` directorio en ZooKeeper. Para las tablas no duplicadas, los ID corresponden a los nombres de archivo en el directorio de datos de la tabla.

**comando** - La cadena de comandos de mutación (la parte de la consulta después de `ALTER TABLE [db.]table`).

**create_time** - Cuando este comando de mutación fue enviado para su ejecución.

**block_numbers.partition_id**, **block_numbers.numero** - Una columna anidada. Para las mutaciones de tablas replicadas, contiene un registro para cada partición: el ID de partición y el número de bloque que fue adquirido por la mutación (en cada partición, solo se mutarán las partes que contienen bloques con números menores que el número de bloque adquirido por la mutación en esa partición). En tablas no replicadas, los números de bloque en todas las particiones forman una sola secuencia. Esto significa que para las mutaciones de tablas no replicadas, la columna contendrá un registro con un solo número de bloque adquirido por la mutación.

**partes_a_do** - El número de partes de datos que deben mutarse para que finalice la mutación.

**is_done** - Es la mutación hecho? Tenga en cuenta que incluso si `parts_to_do = 0` es posible que aún no se haya realizado una mutación de una tabla replicada debido a un INSERT de larga ejecución que creará una nueva parte de datos que deberá mutarse.

Si hubo problemas con la mutación de algunas partes, las siguientes columnas contienen información adicional:

**Método de codificación de datos:** - El nombre de la parte más reciente que no se pudo mutar.

**Método de codificación de datos:** - El momento del fracaso de la mutación de la parte más reciente.

**Método de codificación de datos:** - El mensaje de excepción que causó el error de mutación de parte más reciente.

## sistema.disco {#system_tables-disks}

Contiene información sobre los discos definidos en el [configuración del servidor](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columna:

-   `name` ([Cadena](../sql-reference/data-types/string.md)) — Name of a disk in the server configuration.
-   `path` ([Cadena](../sql-reference/data-types/string.md)) — Path to the mount point in the file system.
-   `free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Free space on disk in bytes.
-   `total_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Disk volume in bytes.
-   `keep_free_space` ([UInt64](../sql-reference/data-types/int-uint.md)) — Amount of disk space that should stay free on disk in bytes. Defined in the `keep_free_space_bytes` parámetro de configuración del disco.

## sistema.almacenamiento_policies {#system_tables-storage_policies}

Contiene información sobre las directivas de almacenamiento y los volúmenes [configuración del servidor](../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure).

Columna:

-   `policy_name` ([Cadena](../sql-reference/data-types/string.md)) — Name of the storage policy.
-   `volume_name` ([Cadena](../sql-reference/data-types/string.md)) — Volume name defined in the storage policy.
-   `volume_priority` ([UInt64](../sql-reference/data-types/int-uint.md)) — Volume order number in the configuration.
-   `disks` ([Array(Cadena)](../sql-reference/data-types/array.md)) — Disk names, defined in the storage policy.
-   `max_data_part_size` ([UInt64](../sql-reference/data-types/int-uint.md)) — Maximum size of a data part that can be stored on volume disks (0 — no limit).
-   `move_factor` ([Float64](../sql-reference/data-types/float.md)) — Ratio of free disk space. When the ratio exceeds the value of configuration parameter, ClickHouse start to move data to the next volume in order.

Si la directiva de almacenamiento contiene más de un volumen, la información de cada volumen se almacena en la fila individual de la tabla.

[Artículo Original](https://clickhouse.tech/docs/en/operations/system_tables/) <!--hide-->
