---
description: 'Documentación de las sentencias SYSTEM'
sidebar_label: 'SYSTEM'
sidebar_position: 36
slug: /sql-reference/statements/system
title: 'Sentencias SYSTEM'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="system-statements">
  # Sentencias de SYSTEM
</div>

<div id="reload-embedded-dictionaries">
  ## SYSTEM RELOAD EMBEDDED DICTIONARIES
</div>

Recarga todos los [diccionarios internos](./create/dictionary/overview.md).
De forma predeterminada, los diccionarios internos están deshabilitados.
Siempre devuelve `Ok.`, independientemente del resultado de la actualización de los diccionarios internos.

<div id="reload-dictionaries">
  ## SYSTEM RELOAD DICTIONARIES
</div>

La consulta `SYSTEM RELOAD DICTIONARIES` recarga los diccionarios con estado `LOADED` (consulte la columna `status` de [`system.dictionaries`](/es/operations/system-tables/dictionaries)); es decir, diccionarios que ya se habían cargado correctamente con anterioridad.
De forma predeterminada, los diccionarios se cargan de forma diferida (consulte [dictionaries&#95;lazy&#95;load](../../operations/server-configuration-parameters/settings.md#dictionaries_lazy_load)), por lo que, en lugar de cargarse automáticamente al iniciar, se inicializan en el primer acceso usando la función [`dictGet`](/es/sql-reference/functions/ext-dict-functions#dictGet) o `SELECT` en tablas con `ENGINE = Dictionary`.

**Sintaxis**

```sql
SYSTEM RELOAD DICTIONARIES [ON CLUSTER cluster_name]
```

<div id="reload-dictionary">
  ## SYSTEM RELOAD DICTIONARY
</div>

Recarga por completo un diccionario `dictionary_name`, independientemente de su estado (LOADED / NOT&#95;LOADED / FAILED).
Siempre devuelve `Ok.`, independientemente del resultado de la actualización del diccionario.

```sql
SYSTEM RELOAD DICTIONARY [ON CLUSTER cluster_name] dictionary_name
```

El estado del diccionario puede comprobarse consultando la tabla `system.dictionaries`.

```sql
SELECT name, status FROM system.dictionaries;
```

<div id="reload-models">
  ## SYSTEM RELOAD MODELS
</div>

:::note
Esta sentencia y `SYSTEM RELOAD MODEL` simplemente descargan los modelos de CatBoost de clickhouse-library-bridge. La función `catboostEvaluate()`
carga un modelo la primera vez que se accede a él si aún no está cargado.
:::

Descarga todos los modelos de CatBoost.

**Sintaxis**

```sql
SYSTEM RELOAD MODELS [ON CLUSTER cluster_name]
```

<div id="reload-model">
  ## SYSTEM RELOAD MODEL
</div>

Descarga de la memoria un modelo de CatBoost en `model_path`.

**Sintaxis**

```sql
SYSTEM RELOAD MODEL [ON CLUSTER cluster_name] <model_path>
```

<div id="reload-functions">
  ## SYSTEM RELOAD FUNCTIONS
</div>

Recarga todas las [funciones ejecutables definidas por el usuario](/es/sql-reference/functions/udf#executable-user-defined-functions) registradas, o una sola, desde un archivo de configuración.

**Sintaxis**

```sql
SYSTEM RELOAD FUNCTIONS [ON CLUSTER cluster_name]
SYSTEM RELOAD FUNCTION [ON CLUSTER cluster_name] function_name
```

<div id="reload-asynchronous-metrics">
  ## SYSTEM RELOAD ASYNCHRONOUS METRICS
</div>

Vuelve a calcular todas las [métricas asíncronas](../../operations/system-tables/asynchronous_metrics.md). Dado que las métricas asíncronas se actualizan periódicamente según la configuración [asynchronous&#95;metrics&#95;update&#95;period&#95;s](../../operations/server-configuration-parameters/settings.md), por lo general no es necesario actualizarlas manualmente mediante esta sentencia.

```sql
SYSTEM RELOAD ASYNCHRONOUS METRICS [ON CLUSTER cluster_name]
```

<div id="drop-dns-cache">
  ## SYSTEM CLEAR|DROP DNS CACHE
</div>

Limpia la caché DNS interna de ClickHouse. A veces (en versiones antiguas de ClickHouse) es necesario usar este comando al cambiar la infraestructura (por ejemplo, al cambiar la dirección IP de otro servidor de ClickHouse o del servidor utilizado por los diccionarios).

Para gestionar la caché de forma más cómoda (automática), consulte los parámetros `disable_internal_dns_cache`, `dns_cache_max_entries`, `dns_cache_update_period`.

<div id="drop-mark-cache">
  ## SYSTEM CLEAR|DROP MARK CACHE
</div>

Limpia la caché de marcas.

<div id="drop-primary-index-cache">
  ## SYSTEM CLEAR|DROP PRIMARY INDEX CACHE
</div>

Borra la caché del índice primario, que almacena en memoria las claves primarias de las tablas [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md).
Su tamaño se configura con el ajuste a nivel de servidor [`primary_index_cache_size`](../../operations/server-configuration-parameters/settings.md#primary_index_cache_size).

<div id="drop-iceberg-metadata-cache">
  ## SYSTEM CLEAR|DROP ICEBERG METADATA CACHE
</div>

Limpia la caché de metadatos de Iceberg.

<div id="drop-avro-schema-cache">
  ## SYSTEM CLEAR|DROP AVRO SCHEMA CACHE
</div>

Limpia las cachés por URL de Confluent Schema Registry que usa el formato `AvroConfluent`. Esto elimina tanto la caché de recuperación de esquemas (id → esquema) como la caché de registro de esquemas (subject + schema → id), por lo que las lecturas y escrituras posteriores vuelven a usar el servidor del registro. Resulta útil cuando se eliminó o reescribió un esquema del lado del registro, o para verificar la idempotencia del registro en las pruebas.

<div id="drop-parquet-metadata-cache">
  ## SYSTEM DROP PARQUET METADATA CACHE
</div>

Vacía la caché de metadatos de Parquet.

<div id="drop-point-in-polygon-cache">
  ## SYSTEM CLEAR|DROP POINT IN POLYGON CACHE
</div>

Borra la caché de polígonos constantes preprocesados que utiliza la función [`pointInPolygon`](../functions/geo/coordinates.md#pointinpolygon). El límite de tamaño configurado (la configuración del servidor `point_in_polygon_cache_size`) no se modifica, por lo que la caché seguirá aceptando entradas después. Para deshabilitar la caché, establezca `point_in_polygon_cache_size` en `0`.

<div id="drop-text-index-caches">
  ## SYSTEM CLEAR|DROP TEXT INDEX CACHES
</div>

Borra las cachés de tokens, cabecera y postings del índice de texto.

Si desea borrar una de estas cachés por separado, puede ejecutar

* `SYSTEM CLEAR TEXT INDEX TOKENS CACHE`,
* `SYSTEM CLEAR TEXT INDEX HEADER CACHE`, o
* `SYSTEM CLEAR TEXT INDEX POSTINGS CACHE`

<div id="drop-index-mark-cache">
  ## SYSTEM CLEAR|DROP INDEX MARK CACHE
</div>

Borra la caché de marcas de los índices secundarios (de omisión de datos).

<div id="drop-index-uncompressed-cache">
  ## SYSTEM CLEAR|DROP INDEX UNCOMPRESSED CACHE
</div>

Limpia la caché de bloques sin comprimir de los índices secundarios (de omisión de datos).

<div id="drop-mmap-cache">
  ## SYSTEM CLEAR|DROP MMAP CACHE
</div>

Limpia la caché de los archivos mapeados en memoria.

<div id="drop-page-cache">
  ## SYSTEM CLEAR|DROP PAGE CACHE
</div>

Borra la caché de páginas en espacio de usuario, la caché en memoria propia de ClickHouse para los datos leídos del almacenamiento subyacente.

<div id="drop-vector-similarity-index-cache">
  ## SYSTEM CLEAR|DROP VECTOR SIMILARITY INDEX CACHE
</div>

Vacía la caché del índice de similitud vectorial.

<div id="drop-connections-cache">
  ## SYSTEM CLEAR|DROP CONNECTIONS CACHE
</div>

Borra la caché de los grupos de conexiones HTTP utilizados para las conexiones salientes.

<div id="drop-s3-client-cache">
  ## SYSTEM CLEAR|DROP S3 CLIENT CACHE
</div>

Borra la caché de los clientes de S3.

<div id="prewarm-mark-cache">
  ## SYSTEM PREWARM MARK CACHE
</div>

Carga las marcas de una tabla en la [caché de marcas](#drop-mark-cache). También carga las marcas de índices secundarios en la [caché de marcas de índice](#drop-index-mark-cache).

```sql
SYSTEM PREWARM MARK CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="prewarm-primary-index-cache">
  ## SYSTEM PREWARM PRIMARY INDEX CACHE
</div>

Carga en la [caché de índices primarios](#drop-primary-index-cache) los índices primarios de una tabla `MergeTree`.

```sql
SYSTEM PREWARM PRIMARY INDEX CACHE [ON CLUSTER cluster_name] [db.]table
```

<div id="drop-disk-metadata-cache">
  ## SYSTEM CLEAR|DROP DISK METADATA CACHE
</div>

Limpia la caché de metadatos del disco especificado.

```sql
SYSTEM DROP DISK METADATA CACHE <disk_name>
```

<div id="sync-filesystem-cache">
  ## SYSTEM SYNC FILESYSTEM CACHE
</div>

Reconcilia el estado en memoria de la caché del sistema de archivos de ClickHouse con los archivos de caché realmente presentes en el disco, y devuelve `cache_name`, `path` y el `size` descargado de cada segmento de archivo almacenado en caché. Un nombre de caché opcional limita la operación a una sola caché.

```sql
SYSTEM SYNC FILESYSTEM CACHE ['<cache_name>']
```

<div id="drop-distributed-cache">
  ## SYSTEM CLEAR|DROP DISTRIBUTED CACHE
</div>

:::note
`SYSTEM CLEAR|DROP DISTRIBUTED CACHE` está disponible solo en ClickHouse Cloud.
:::

Elimina la caché distribuida. Use `CONNECTIONS` para eliminar solo las conexiones en caché a los servidores de caché distribuida, o pase un identificador de servidor para dirigirse a un solo servidor.

```sql
SYSTEM DROP DISTRIBUTED CACHE [CONNECTIONS | 'server_id']
```

<div id="drop-replica">
  ## SYSTEM DROP REPLICA
</div>

Las réplicas inactivas de las tablas `ReplicatedMergeTree` pueden eliminarse con la siguiente sintaxis:

```sql
SYSTEM DROP REPLICA 'replica_name' FROM TABLE database.table;
SYSTEM DROP REPLICA 'replica_name' FROM DATABASE database;
SYSTEM DROP REPLICA 'replica_name';
SYSTEM DROP REPLICA 'replica_name' FROM ZKPATH '/path/to/table/in/zk';
```

Las consultas eliminarán la ruta de la réplica de `ReplicatedMergeTree` en ZooKeeper. Resulta útil cuando la réplica está caída y sus metadatos no pueden eliminarse de ZooKeeper mediante `DROP TABLE` porque esa tabla ya no existe. Solo eliminará la réplica inactiva/obsoleta y no puede eliminar la réplica local; para eso, use `DROP TABLE`. `DROP REPLICA` no elimina ninguna tabla ni quita datos o metadatos del disco.

La primera elimina los metadatos de la réplica `'replica_name'` de la tabla `database.table`.
La segunda hace lo mismo para todas las tablas replicadas de la base de datos.
La tercera hace lo mismo para todas las tablas replicadas del servidor local.
La cuarta es útil para eliminar los metadatos de una réplica caída cuando ya se han eliminado todas las demás réplicas de una tabla. Requiere que la ruta de la tabla se especifique explícitamente. Debe ser la misma ruta que se pasó como primer argumento del motor `ReplicatedMergeTree` al crear la tabla.

<div id="drop-database-replica">
  ## SYSTEM DROP DATABASE REPLICA
</div>

Las réplicas inactivas de las bases de datos `Replicated` se pueden eliminar con la siguiente sintaxis:

```sql
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM DATABASE database;
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'];
SYSTEM DROP DATABASE REPLICA 'replica_name' [FROM SHARD 'shard_name'] FROM ZKPATH '/path/to/table/in/zk';
```

Similar a `SYSTEM DROP REPLICA`, pero elimina de ZooKeeper la ruta de la réplica de la base de datos `Replicated` cuando no hay ninguna base de datos sobre la que ejecutar `DROP DATABASE`. Tenga en cuenta que no elimina las réplicas de `ReplicatedMergeTree` (por lo que también podría necesitar `SYSTEM DROP REPLICA`). Los nombres del segmento y de la réplica son los que se especificaron en los argumentos del motor `Replicated` al crear la base de datos. Además, estos nombres pueden obtenerse de las columnas `database_shard_name` y `database_replica_name` de `system.clusters`. Si falta la cláusula `FROM SHARD`, `replica_name` debe ser un nombre de réplica completo con el formato `shard_name|replica_name`.

<div id="drop-uncompressed-cache">
  ## SYSTEM CLEAR|DROP UNCOMPRESSED CACHE
</div>

Borra la caché de datos sin comprimir.
La caché de datos sin comprimir se habilita o deshabilita con la configuración de nivel de consulta/usuario/perfil [`use_uncompressed_cache`](../../operations/settings/settings.md#use_uncompressed_cache).
Su tamaño puede configurarse mediante la configuración de nivel de servidor [`uncompressed_cache_size`](../../operations/server-configuration-parameters/settings.md#uncompressed_cache_size).

<div id="drop-compiled-expression-cache">
  ## SYSTEM CLEAR|DROP COMPILED EXPRESSION CACHE
</div>

Borra la caché de expresiones compiladas.
La caché de expresiones compiladas se activa o desactiva mediante la configuración de nivel de consulta/usuario/perfil [`compile_expressions`](../../operations/settings/settings.md#compile_expressions).

<div id="drop-query-condition-cache">
  ## SYSTEM CLEAR|DROP QUERY CONDITION CACHE
</div>

Limpia la caché de condiciones de consulta.

<div id="drop-query-cache">
  ## SYSTEM CLEAR|DROP QUERY CACHE
</div>

```sql
SYSTEM CLEAR QUERY CACHE;
SYSTEM CLEAR QUERY CACHE TAG '<tag>'
```

Borra la [caché de consultas](../../operations/query-cache.md).
Si se especifica una etiqueta, solo se eliminan las entradas de la caché de consultas con la etiqueta especificada.

<div id="system-drop-schema-format">
  ## SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE
</div>

Limpia la caché de los esquemas cargados desde [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path).

Destinos admitidos:

* Protobuf: Elimina de la memoria las definiciones importadas de mensajes Protobuf.
* Files: Elimina los archivos de esquema en caché almacenados localmente en [`format_schema_path`](../../operations/server-configuration-parameters/settings.md#format_schema_path), generados cuando `format_schema_source` se establece en `query`.
  Nota: Si no se especifica ningún destino, se limpian ambas cachés.

```sql
SYSTEM CLEAR|DROP FORMAT SCHEMA CACHE [FOR Protobuf/Files]
```

<div id="flush-logs">
  ## SYSTEM FLUSH LOGS
</div>

Vuelca los mensajes de log almacenados en búfer a las tablas del sistema, p. ej., system.query&#95;log. Resulta útil principalmente para la depuración, ya que la mayoría de las tablas del sistema tienen un intervalo de vaciado predeterminado de 7,5 segundos.
Esto también creará las tablas del sistema aunque la cola de mensajes esté vacía.

```sql
SYSTEM FLUSH LOGS [ON CLUSTER cluster_name] [log_name|[database.table]] [, ...]
```

Si no desea vaciarlo todo, puede vaciar uno o varios logs individuales indicando su nombre o su tabla de destino:

```sql
SYSTEM FLUSH LOGS query_log, system.query_views_log;
```

<div id="reload-config">
  ## SYSTEM RELOAD CONFIG
</div>

Recarga la configuración de ClickHouse. Se usa cuando la configuración está almacenada en ZooKeeper. Tenga en cuenta que `SYSTEM RELOAD CONFIG` no recarga la configuración de `USER` almacenada en ZooKeeper; solo recarga la configuración de `USER` almacenada en `users.xml`. Para recargar toda la configuración de `USER`, use `SYSTEM RELOAD USERS`

```sql
SYSTEM RELOAD CONFIG [ON CLUSTER cluster_name]
```

<div id="reload-users">
  ## SYSTEM RELOAD USERS
</div>

Recarga todos los almacenamientos de acceso, incluidos users.xml, el almacenamiento de acceso en disco local y el almacenamiento de acceso replicado (en ZooKeeper).

```sql
SYSTEM RELOAD USERS [ON CLUSTER cluster_name]
```

<div id="shutdown">
  ## SYSTEM SHUTDOWN
</div>

<CloudNotSupportedBadge />

Apaga ClickHouse de forma normal (como `service clickhouse-server stop` / `kill {$pid_clickhouse-server}`)

<div id="kill">
  ## SYSTEM KILL
</div>

Finaliza el proceso de ClickHouse (como `kill -9 {$ pid_clickhouse-server}`)

<div id="instrument">
  ## SYSTEM INSTRUMENT
</div>

Gestiona los puntos de instrumentación mediante la función XRay de LLVM, disponible cuando ClickHouse se compila con `ENABLE_XRAY=1`.
Esto permite depurar y perfilar en producción sin modificar el código fuente y con una sobrecarga mínima.
Cuando no se añade ningún punto de instrumentación, la penalización en el rendimiento es insignificante, ya que solo agrega un salto adicional a una dirección cercana
en el prólogo y el epílogo de aquellas funciones que tienen más de 200 instrucciones.

<div id="instrument-add">
  ### SYSTEM INSTRUMENT ADD
</div>

Añade un nuevo punto de instrumentación. Las funciones instrumentadas pueden inspeccionarse en la tabla del sistema [`system.instrumentation`](../../operations/system-tables/instrumentation.md). Se puede añadir más de un handler para la misma función, y se ejecutarán en el mismo orden en que se añadió la instrumentación.
Las funciones que se van a instrumentar pueden obtenerse de la tabla del sistema [`system.symbols`](../../operations/system-tables/symbols.md).

Hay tres tipos distintos de handler que se pueden añadir a las funciones:

**Sintaxis**

```sql
SYSTEM INSTRUMENT ADD FUNCTION HANDLER [ARGUMENTS]
```

donde `FUNCTION` es cualquier función o subcadena de una función, como `QueryMetricLog::startQuery`, y el manejador es uno de los siguientes

<div id="instrument-add-log">
  #### LOG
</div>

Imprime el texto proporcionado como argumento y la traza de pila, ya sea en `ENTRY` o en `EXIT` de la función.

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG ENTRY 'this is a log printed at entry'
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' LOG EXIT 'this is a log printed at exit'
```

<div id="instrument-add-sleep">
  #### SLEEP
</div>

Pausa la ejecución durante una cantidad fija de segundos, ya sea en `ENTRY` o en `EXIT`:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0.5
```

o para una cantidad aleatoria de segundos con distribución uniforme, indicando el mínimo y el máximo separados por un espacio en blanco:

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' SLEEP ENTRY 0 1
```

<div id="instrument-add-profile">
  #### PROFILE
</div>

Mide el tiempo transcurrido entre `ENTRY` y `EXIT` de una función.
El resultado del perfilado se almacena en [`system.trace_log`](../../operations/system-tables/trace_log.md) y puede convertirse
al [formato de rastreo de eventos de Chrome](../../operations/system-tables/trace_log.md#chrome-event-trace-format).

```sql
SYSTEM INSTRUMENT ADD 'QueryMetricLog::startQuery' PROFILE
```

<div id="instrument-remove">
  ### SYSTEM INSTRUMENT REMOVE
</div>

Elimina un solo punto de instrumentación con:

```sql
SYSTEM INSTRUMENT REMOVE ID
```

todos ellos usando la palabra clave `ALL`:

```sql
SYSTEM INSTRUMENT REMOVE ALL
```

un conjunto de IDs de una subconsulta:

```sql
SYSTEM INSTRUMENT REMOVE (SELECT id FROM system.instrumentation WHERE handler = 'log')
```

o todos los puntos de instrumentación que coincidan con un function&#95;name específico:

```sql
SYSTEM INSTRUMENT REMOVE 'QueryMetricLog::startQuery'
```

La información del punto de instrumentación puede obtenerse de la tabla del sistema [`system.instrumentation`](../../operations/system-tables/instrumentation.md).

<div id="managing-distributed-tables">
  ## Gestión de tablas distribuidas
</div>

ClickHouse puede gestionar tablas [distribuidas](../../engines/table-engines/special/distributed.md). Cuando un usuario inserta datos en estas tablas, ClickHouse primero crea una cola con los datos que deben enviarse a los nodos del clúster y luego los envía de forma asíncrona. Puede gestionar el procesamiento de esa cola con las consultas [`STOP DISTRIBUTED SENDS`](#stop-distributed-sends), [FLUSH DISTRIBUTED](#flush-distributed) y [`START DISTRIBUTED SENDS`](#start-distributed-sends). También puede insertar datos distribuidos de forma síncrona con el ajuste [`distributed_foreground_insert`](../../operations/settings/settings.md#distributed_foreground_insert).

<div id="stop-distributed-sends">
  ### SYSTEM STOP DISTRIBUTED SENDS
</div>

Desactiva la distribución de datos en segundo plano al insertar datos en tablas distribuidas.

```sql
SYSTEM STOP DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

:::note
Si [`prefer_localhost_replica`](../../operations/settings/settings.md#prefer_localhost_replica) está habilitado (de forma predeterminada), los datos del segmento local se insertarán de todas formas.
:::

<div id="flush-distributed">
  ### SYSTEM FLUSH DISTRIBUTED
</div>

Fuerza a ClickHouse a enviar datos a los nodos del clúster de forma síncrona. Si algún nodo no está disponible, ClickHouse lanza una excepción y detiene la ejecución de la consulta. Puede reintentar la consulta hasta que se ejecute correctamente, lo que ocurrirá cuando todos los nodos vuelvan a estar en línea.

También puede sobrescribir algunos ajustes mediante la cláusula `SETTINGS`; esto puede ser útil para evitar limitaciones temporales, como `max_concurrent_queries_for_all_users` o `max_memory_usage`.

```sql
SYSTEM FLUSH DISTRIBUTED [db.]<distributed_table_name> [ON CLUSTER cluster_name] [SETTINGS ...]
```

:::note
Cada bloque pendiente se almacena en disco con la configuración de la consulta INSERT inicial, por lo que a veces quizá quieras sobrescribirla.
:::

<div id="start-distributed-sends">
  ### SYSTEM START DISTRIBUTED SENDS
</div>

Activa la distribución de datos en segundo plano al insertar datos en tablas distribuidas.

```sql
SYSTEM START DISTRIBUTED SENDS [db.]<distributed_table_name> [ON CLUSTER cluster_name]
```

<div id="stop-listen">
  ### SYSTEM STOP LISTEN
</div>

Cierra el socket y finaliza ordenadamente las conexiones existentes con el servidor en el puerto y con el protocolo especificados.

Sin embargo, si la configuración correspondiente del protocolo no se especificó en la configuración de clickhouse-server, este comando no tendrá efecto.

```sql
SYSTEM STOP LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

* Si se especifica el modificador `CUSTOM 'protocol'`, se detendrá el protocolo personalizado con el nombre indicado en la sección de protocolos de la configuración del servidor.
* Si se especifica el modificador `QUERIES ALL [EXCEPT .. [,..]]`, se detendrán todos los protocolos, excepto los indicados en la cláusula `EXCEPT`.
* Si se especifica el modificador `QUERIES DEFAULT [EXCEPT .. [,..]]`, se detendrán todos los protocolos predeterminados, excepto los indicados en la cláusula `EXCEPT`.
* Si se especifica el modificador `QUERIES CUSTOM [EXCEPT .. [,..]]`, se detendrán todos los protocolos personalizados, excepto los indicados en la cláusula `EXCEPT`.

<div id="start-listen">
  ### SYSTEM START LISTEN
</div>

Permite establecer nuevas conexiones en los protocolos especificados.

Sin embargo, si el servidor en el puerto y protocolo especificados no se detuvo con el comando SYSTEM STOP LISTEN, este comando no tendrá efecto.

```sql
SYSTEM START LISTEN [ON CLUSTER cluster_name] [QUERIES ALL | QUERIES DEFAULT | QUERIES CUSTOM | TCP | TCP WITH PROXY | TCP SECURE | HTTP | HTTPS | MYSQL | GRPC | POSTGRESQL | PROMETHEUS | CUSTOM 'protocol']
```

<div id="managing-mergetree-tables">
  ## Gestión de tablas MergeTree
</div>

ClickHouse puede gestionar procesos en segundo plano en las tablas [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

<div id="stop-merges">
  ### SYSTEM STOP MERGES
</div>

<CloudNotSupportedBadge />

Permite detener las operaciones de fusión en segundo plano en las tablas de la familia MergeTree:

```sql
SYSTEM STOP MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

:::note
La operación `DETACH / ATTACH` de una tabla iniciará las fusiones en segundo plano de la tabla, incluso si antes se habían detenido para todas las tablas MergeTree.
:::

<div id="start-merges">
  ### SYSTEM START MERGES
</div>

<CloudNotSupportedBadge />

Permite iniciar las fusiones en segundo plano para las tablas de la familia MergeTree:

```sql
SYSTEM START MERGES [ON CLUSTER cluster_name] [ON VOLUME <volume_name> | [db.]merge_tree_family_table_name]
```

<div id="stop-ttl-merges">
  ### SYSTEM STOP TTL MERGES
</div>

<CloudNotSupportedBadge />

Permite detener la eliminación en segundo plano de datos antiguos según la [expresión TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) en tablas de la familia MergeTree:
Devuelve `Ok.` incluso si la tabla no existe o si no tiene el motor MergeTree. Devuelve un error cuando la base de datos no existe:

```sql
SYSTEM STOP TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-ttl-merges">
  ### SYSTEM START TTL MERGES
</div>

<CloudNotSupportedBadge />

Permite iniciar en segundo plano la eliminación de datos antiguos según la [expresión TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl) para las tablas de la familia MergeTree:
Devuelve `Ok.` incluso si la tabla no existe. Devuelve un error cuando la base de datos no existe:

```sql
SYSTEM START TTL MERGES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="stop-moves">
  ### SYSTEM STOP MOVES
</div>

Permite detener los movimientos de datos en segundo plano según la [expresión TTL de tabla con la cláusula TO VOLUME o TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) para las tablas de la familia MergeTree:
Devuelve `Ok.` incluso si la tabla no existe. Devuelve un error cuando la base de datos no existe:

```sql
SYSTEM STOP MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="start-moves">
  ### SYSTEM START MOVES
</div>

Permite iniciar movimientos de datos en segundo plano según la [expresión TTL de la tabla con las cláusulas TO VOLUME y TO DISK](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) para las tablas de la familia MergeTree:
Devuelve `Ok.` incluso si la tabla no existe. Devuelve un error cuando la base de datos no existe:

```sql
SYSTEM START MOVES [ON CLUSTER cluster_name] [[db.]merge_tree_family_table_name]
```

<div id="query_language-system-unfreeze">
  ### SYSTEM UNFREEZE
</div>

Elimina de todos los discos una copia de seguridad congelada con el nombre especificado. Consulte más información sobre cómo descongelar partes individuales en [ALTER TABLE table&#95;name UNFREEZE WITH NAME ](/es/sql-reference/statements/alter/partition#unfreeze-partition)

```sql
SYSTEM UNFREEZE WITH NAME <backup_name>
```

<div id="wait-loading-parts">
  ### SYSTEM WAIT LOADING PARTS
</div>

Espere hasta que se hayan cargado todas las partes de datos de una tabla que se cargan de forma asíncrona (partes de datos obsoletas).

```sql
SYSTEM WAIT LOADING PARTS [ON CLUSTER cluster_name] [db.]merge_tree_family_table_name
```

<div id="managing-replicatedmergetree-tables">
  ## Gestión de tablas ReplicatedMergeTree
</div>

ClickHouse puede gestionar los procesos en segundo plano relacionados con la replicación de las tablas [ReplicatedMergeTree](/es/engines/table-engines/mergetree-family/replication).

<div id="stop-fetches">
  ### SYSTEM STOP FETCHES
</div>

<CloudNotSupportedBadge />

Permite detener las recuperaciones en segundo plano de las partes insertadas en las tablas de la familia `ReplicatedMergeTree`:
Siempre devuelve `Ok.`, independientemente del motor de tabla, incluso si la tabla o la base de datos no existen.

```sql
SYSTEM STOP FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-fetches">
  ### SYSTEM START FETCHES
</div>

<CloudNotSupportedBadge />

Permite iniciar las operaciones de recuperación en segundo plano de las partes insertadas para tablas de la familia `ReplicatedMergeTree`:
Siempre devuelve `Ok.`, independientemente del motor de tabla e incluso si la tabla o la base de datos no existen.

```sql
SYSTEM START FETCHES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replicated-sends">
  ### SYSTEM STOP REPLICATED SENDS
</div>

Permite detener los envíos en segundo plano a otras réplicas del clúster de nuevas partes insertadas en tablas de la familia `ReplicatedMergeTree`:

```sql
SYSTEM STOP REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replicated-sends">
  ### SYSTEM START REPLICATED SENDS
</div>

Permite iniciar el envío en segundo plano a otras réplicas del cluster de las nuevas partes insertadas en tablas de la familia `ReplicatedMergeTree`:

```sql
SYSTEM START REPLICATED SENDS [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-replication-queues">
  ### SYSTEM STOP REPLICATION QUEUES
</div>

Permite detener las tareas de recuperación en segundo plano de las colas de replicación almacenadas en Zookeeper para tablas de la familia `ReplicatedMergeTree`. Posibles tipos de tareas en segundo plano: fusiones, recuperaciones, mutation, sentencias DDL con la cláusula ON CLUSTER:

```sql
SYSTEM STOP REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-replication-queues">
  ### SYSTEM START REPLICATION QUEUES
</div>

Permite iniciar tareas de recuperación en segundo plano desde las colas de replicación almacenadas en ZooKeeper para las tablas de la familia `ReplicatedMergeTree`. Posibles tipos de tareas en segundo plano: fusiones, recuperaciones, mutation y sentencias DDL con la cláusula ON CLUSTER:

```sql
SYSTEM START REPLICATION QUEUES [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="stop-pulling-replication-log">
  ### SYSTEM STOP PULLING REPLICATION LOG
</div>

Detiene la carga de nuevas entradas del log de replicación a la cola de replicación en una tabla `ReplicatedMergeTree`.

```sql
SYSTEM STOP PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="start-pulling-replication-log">
  ### SYSTEM START PULLING REPLICATION LOG
</div>

Cancela `SYSTEM STOP PULLING REPLICATION LOG`.

```sql
SYSTEM START PULLING REPLICATION LOG [ON CLUSTER cluster_name] [[db.]replicated_merge_tree_family_table_name]
```

<div id="sync-replica">
  ### SYSTEM SYNC REPLICA
</div>

Espera hasta que una tabla `ReplicatedMergeTree` se sincronice con otras réplicas de un clúster, pero sin superar los `receive_timeout` segundos.

```sql
SYSTEM SYNC REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name [IF EXISTS] [STRICT | LIGHTWEIGHT [FROM 'srcReplica1'[, 'srcReplica2'[, ...]]] | PULL]
```

Después de ejecutar esta instrucción, `[db.]replicated_merge_tree_family_table_name` toma comandos del log replicado compartido y los incorpora a su propia cola de replicación; luego, la consulta espera hasta que la réplica procese todos los comandos obtenidos. Se admiten los siguientes modificadores:

* Con `IF EXISTS` (disponible desde la versión 25.6), la consulta no generará un error si la tabla no existe. Esto resulta útil al añadir una nueva réplica a un clúster, cuando ya forma parte de la configuración del clúster pero todavía está creando y sincronizando la tabla.
* Si se especifica el modificador `STRICT`, la consulta espera a que la cola de replicación quede vacía. Es posible que la versión `STRICT` nunca llegue a completarse si siguen apareciendo nuevas entradas en la cola de replicación.
* Si se especifica el modificador `LIGHTWEIGHT`, la consulta espera únicamente a que se procesen las entradas `GET_PART`, `ATTACH_PART`, `DROP_RANGE`, `REPLACE_RANGE` y `DROP_PART`.
  Además, el modificador LIGHTWEIGHT admite una cláusula opcional FROM &#39;srcReplicas&#39;, donde &#39;srcReplicas&#39; es una lista de nombres de réplicas de origen separada por comas. Esta extensión permite una sincronización más específica al centrarse solo en las tareas de replicación originadas en las réplicas de origen indicadas.
* Si se especifica el modificador `PULL`, la consulta extrae nuevas entradas de la cola de replicación desde ZooKeeper, pero no espera a que se procese nada.

<div id="sync-database-replica">
  ### SYNC DATABASE REPLICA
</div>

Espera hasta que la [base de datos replicada](/es/engines/database-engines/replicated) especificada haya aplicado todos los cambios de esquema de la cola DDL de esa base de datos.

**Sintaxis**

```sql
SYSTEM SYNC DATABASE REPLICA replicated_database_name;
```

<div id="restart-replica">
  ### SYSTEM RESTART REPLICA
</div>

Permite reinicializar el estado de la sesión de ZooKeeper para una tabla `ReplicatedMergeTree`; compara el estado actual con ZooKeeper como fuente de verdad y añade tareas a la cola de ZooKeeper si es necesario.
La inicialización de la cola de replicación basada en los datos de ZooKeeper se realiza del mismo modo que con la instrucción `ATTACH TABLE`. Durante un breve período, la tabla no estará disponible para ninguna operación.

```sql
SYSTEM RESTART REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

<div id="restore-replica">
  ### SYSTEM RESTORE REPLICA
</div>

Restaura una réplica si los datos [posiblemente] están presentes, pero se han perdido los metadatos de ZooKeeper.

Solo funciona en tablas `ReplicatedMergeTree` de solo lectura.

La consulta se puede ejecutar después de:

* La pérdida de la raíz `/` de ZooKeeper.
* La pérdida de la ruta de réplicas `/replicas`.
* La pérdida de la ruta de una réplica individual `/replicas/replica_name/`.

La réplica adjunta las partes encontradas localmente y envía información sobre ellas a ZooKeeper.
Las partes presentes en una réplica antes de la pérdida de metadatos no se vuelven a obtener de otras si no están obsoletas (por lo tanto, restaurar la réplica no implica volver a descargar todos los datos por la red).

:::note
Las partes en todos los estados se mueven a la carpeta `detached/`. Las partes activas antes de la pérdida de datos (committed) se adjuntan.
:::

<div id="restore-database-replica">
  ### SYSTEM RESTORE DATABASE REPLICA
</div>

Restaura una réplica si [posiblemente] los datos están presentes, pero se han perdido los metadatos de ZooKeeper.

**Sintaxis**

```sql
SYSTEM RESTORE DATABASE REPLICA repl_db [ON CLUSTER cluster]
```

**Ejemplo**

```sql
CREATE DATABASE repl_db
ENGINE=Replicated("/clickhouse/repl_db", shard1, replica1);

CREATE TABLE repl_db.test_table (n UInt32)
ENGINE = ReplicatedMergeTree
ORDER BY n PARTITION BY n % 10;

-- zookeeper_delete_path("/clickhouse/repl_db", recursive=True) <- root loss.

SYSTEM RESTORE DATABASE REPLICA repl_db;
```

**Sintaxis**

```sql
SYSTEM RESTORE REPLICA [db.]replicated_merge_tree_family_table_name [ON CLUSTER cluster_name]
```

Sintaxis alternativa:

```sql
SYSTEM RESTORE REPLICA [ON CLUSTER cluster_name] [db.]replicated_merge_tree_family_table_name
```

**Ejemplo**

Creación de una tabla en varios servidores. Después de que se pierdan los metadatos de la réplica en ZooKeeper, la tabla se adjuntará en modo de solo lectura porque faltan los metadatos. La última consulta debe ejecutarse en cada réplica.

```sql
CREATE TABLE test(n UInt32)
ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/', '{replica}')
ORDER BY n PARTITION BY n % 10;

INSERT INTO test SELECT * FROM numbers(1000);

-- zookeeper_delete_path("/clickhouse/tables/test", recursive=True) <- root loss.

SYSTEM RESTART REPLICA test;
SYSTEM RESTORE REPLICA test;
```

Otra forma:

```sql
SYSTEM RESTORE REPLICA test ON CLUSTER cluster;
```

<div id="restart-replicas">
  ### SYSTEM RESTART REPLICAS
</div>

Permite reinicializar el estado de las sesiones de ZooKeeper para todas las tablas `ReplicatedMergeTree`; compara el estado actual con ZooKeeper como fuente de verdad y añade tareas a la cola de ZooKeeper si es necesario

<div id="drop-filesystem-cache">
  ### SYSTEM CLEAR|DROP FILESYSTEM CACHE
</div>

Permite vaciar la caché del sistema de archivos.

```sql
SYSTEM CLEAR FILESYSTEM CACHE [ON CLUSTER cluster_name]
```

<div id="sync-file-cache">
  ### SYSTEM SYNC FILE CACHE
</div>

:::note
Es una operación demasiado costosa y puede prestarse a un uso indebido.
:::

Ejecutará la llamada al sistema sync.

```sql
SYSTEM SYNC FILE CACHE [ON CLUSTER cluster_name]
```

<div id="load-primary-key">
  ### SYSTEM LOAD PRIMARY KEY
</div>

Carga las claves primarias de la tabla especificada o de todas las tablas.

```sql
SYSTEM LOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM LOAD PRIMARY KEY
```

<div id="unload-primary-key">
  ### SYSTEM UNLOAD PRIMARY KEY
</div>

Descarga las claves primarias de la tabla indicada o de todas las tablas.

```sql
SYSTEM UNLOAD PRIMARY KEY [db.]name
```

```sql
SYSTEM UNLOAD PRIMARY KEY
```

<div id="managing-refreshable-materialized-views">
  ## Gestión de vistas materializadas actualizables
</div>

Comandos para controlar las tareas en segundo plano que realizan las [vistas materializadas actualizables](../../sql-reference/statements/create/view.md#refreshable-materialized-view)

Supervisa [`system.view_refreshes`](../../operations/system-tables/view_refreshes.md) mientras las uses.

<div id="stop-view-stop-views">
  ### SYSTEM STOP [REPLICATED] VIEW, STOP VIEWS
</div>

Desactiva la actualización periódica de la vista indicada o de todas las vistas actualizables. Si hay una actualización en curso, también la cancela.

Si la vista está en una base de datos Replicated o Shared, `STOP VIEW` solo afecta a la réplica actual, mientras que `STOP REPLICATED VIEW` afecta a todas las réplicas.

:::note
El estado de detención no se conserva tras reiniciar el servidor. Después de un reinicio, las vistas volverán a seguir la programación de actualización configurada.
En bases de datos Replicated o Shared, `SYSTEM STOP VIEW` solo afecta a la réplica actual. Usa `SYSTEM STOP REPLICATED VIEW` para detener las actualizaciones en todas las réplicas.
:::

```sql
SYSTEM STOP VIEW [db.]name
```

```sql
SYSTEM STOP VIEWS
```

<div id="start-view-start-views">
  ### SYSTEM START [REPLICATED] VIEW, START VIEWS
</div>

Habilita la actualización periódica de la vista indicada o de todas las vistas actualizables. No se realiza ninguna actualización inmediata.

Si la vista está en una base de datos Replicated o Shared, `START VIEW` deshace el efecto de `STOP VIEW` y `START REPLICATED VIEW` deshace el efecto de `STOP REPLICATED VIEW`. `START VIEW` también deshace el efecto de `PAUSE VIEW`.

```sql
SYSTEM START VIEW [db.]name
```

```sql
SYSTEM START VIEWS
```

<div id="pause-view-pause-views">
  ### SYSTEM PAUSE VIEW, PAUSE VIEWS
</div>

Deshabilita la actualización periódica de la vista indicada o de todas las vistas actualizables.
A diferencia de `SYSTEM STOP VIEW`, `SYSTEM PAUSE VIEW` no interrumpe una actualización que ya esté en curso: deja que la actualización en ejecución finalice y solo impide las actualizaciones posteriores.

Para revertirlo, usa `SYSTEM START VIEW` o `SYSTEM START VIEWS`.

:::note
El estado de pausa no se conserva tras reiniciar el servidor. Después de un reinicio, las vistas retomarán su programación de actualización configurada.
En bases de datos Replicated o Shared, `SYSTEM PAUSE VIEW` solo afecta a la réplica actual.
:::

```sql
SYSTEM PAUSE VIEW [db.]name
```

```sql
SYSTEM PAUSE VIEWS
```

<div id="refresh-view">
  ### SYSTEM REFRESH VIEW
</div>

Ejecuta una actualización inmediata de una vista determinada, fuera de la programación.

```sql
SYSTEM REFRESH VIEW [db.]name
```

<div id="wait-view">
  ### SYSTEM WAIT VIEW
</div>

Espera a que termine la actualización en curso. Si no hay ninguna actualización en curso, devuelve inmediatamente. Si el último intento de actualización falló, informa un error.

Puede usarse justo después de crear una nueva vista materializada actualizable (sin la palabra clave EMPTY) para esperar a que termine la actualización inicial.

Si la vista está en una base de datos Replicated o Shared, y la actualización se está ejecutando en otra réplica, espera a que termine esa actualización.

```sql
SYSTEM WAIT VIEW [db.]name
```

<div id="cancel-view">
  ### SYSTEM CANCEL VIEW
</div>

Si hay una actualización en curso de la vista especificada en la réplica actual, interrúmpala y cancélela. De lo contrario, no haga nada.

```sql
SYSTEM CANCEL VIEW [db.]name
```

<div id="flush-object-storage-queue">
  ## SYSTEM FLUSH OBJECT STORAGE QUEUE
</div>

Bloquea hasta que el archivo indicado haya sido procesado o haya fallado de forma permanente en la tabla [S3Queue](../../engines/table-engines/integrations/s3queue.md) o [AzureQueue](../../engines/table-engines/integrations/azure-queue.md) dada. Devuelve inmediatamente si el archivo ya fue procesado. Genera un error si el archivo ha fallado de forma permanente (se agotaron todos los reintentos).

```sql
SYSTEM FLUSH OBJECT STORAGE QUEUE [db.]table_name PATH 'path'
```