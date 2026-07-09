---
description: 'El motor se basa en el motor Atomic. Admite la replicación de
  metadatos mediante un registro DDL que se escribe en ZooKeeper y se ejecuta en todas las réplicas
  de una base de datos determinada.'
sidebar_label: 'Replicated'
sidebar_position: 30
slug: /engines/database-engines/replicated
title: 'Replicated'
doc_type: 'referencia'
---

El motor se basa en el motor [Atomic](../../engines/database-engines/atomic.md). Admite la replicación de metadatos mediante un registro DDL que se escribe en ZooKeeper y se ejecuta en todas las réplicas de una base de datos determinada.

Un servidor ClickHouse puede tener varias bases de datos replicadas ejecutándose y actualizándose al mismo tiempo. Pero no puede haber varias réplicas de la misma base de datos replicada.

<div id="creating-a-database">
  ## Crear una base de datos
</div>

```sql
CREATE DATABASE testdb [UUID '...'] ENGINE = Replicated('zoo_path', 'shard_name', 'replica_name') [SETTINGS ...]
```

**Parámetros del motor**

* `zoo_path` — ruta de ZooKeeper. La misma ruta de ZooKeeper corresponde a la misma base de datos.
* `shard_name` — nombre del segmento. Las réplicas de la base de datos se agrupan en segmentos mediante `shard_name`.
* `replica_name` — nombre de la réplica. Los nombres de las réplicas deben ser distintos entre todas las réplicas del mismo segmento.

Los parámetros pueden omitirse; en ese caso, los que falten se sustituirán por los valores predeterminados.

Si `zoo_path` contiene la macro `{uuid}`, es necesario especificar un UUID explícito o añadir [ON CLUSTER](../../sql-reference/distributed-ddl.md) a la sentencia CREATE para garantizar que todas las réplicas usen el mismo UUID para esta base de datos.

En las tablas [ReplicatedMergeTree](/es/engines/table-engines/mergetree-family/replication), si no se proporcionan argumentos, se usan los argumentos predeterminados: `/clickhouse/tables/{uuid}/{shard}` y `{replica}`. Estos pueden cambiarse en la configuración del servidor mediante [default&#95;replica&#95;path](../../operations/server-configuration-parameters/settings.md#default_replica_path) y [default&#95;replica&#95;name](../../operations/server-configuration-parameters/settings.md#default_replica_name). La macro `{uuid}` se expande al uuid de la tabla; `{shard}` y `{replica}` se expanden a valores de la configuración del servidor, no a argumentos del motor de base de datos. No obstante, en el futuro será posible usar `shard_name` y `replica_name` de la base de datos Replicated.

También se admite un clúster auxiliar de ZooKeeper para almacenar los metadatos de una base de datos replicada en lugar de usar el clúster de ZooKeeper predeterminado. Podemos usar SQL para crear la base de datos replicada con un clúster auxiliar de ZooKeeper de la siguiente manera:

```sql
CREATE DATABASE database_name ENGINE = Replicated('zookeeper_name_configured_in_auxiliary_zookeepers:path', 'shard_name', 'replica_name')
```

<div id="specifics-and-recommendations">
  ## Aspectos específicos y recomendaciones
</div>

Las consultas DDL con la base de datos `Replicated` funcionan de forma similar a las consultas [ON CLUSTER](../../sql-reference/distributed-ddl.md), pero con pequeñas diferencias.

En primer lugar, la solicitud DDL intenta ejecutarse en el initiator (el host que recibió originalmente la solicitud del usuario). Si la solicitud no se completa, el usuario recibe inmediatamente un error y los demás hosts no intentan completarla. Si la solicitud se completa correctamente en el initiator, todos los demás hosts la reintentaránautomáticamente hasta completarla. El initiator intentará esperar a que la consulta se complete en los demás hosts (no más de [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout)) y devolverá una tabla con los estados de ejecución de la consulta en cada host.

El comportamiento en caso de error está regulado por la configuración [distributed&#95;ddl&#95;output&#95;mode](../../operations/settings/settings.md#distributed_ddl_output_mode); para una base de datos `Replicated` es mejor establecerla en `null_status_on_timeout`, es decir, si algunos hosts no tuvieron tiempo de ejecutar la solicitud durante [distributed&#95;ddl&#95;task&#95;timeout](../../operations/settings/settings.md#distributed_ddl_task_timeout), no se debe lanzar una excepción, sino mostrar el estado `NULL` para ellos en la tabla.

La tabla del sistema [system.clusters](../../operations/system-tables/clusters.md) contiene un clúster con el mismo nombre que la base de datos replicada, que consta de todas las réplicas de la base de datos. Este clúster se actualiza automáticamente al crear o eliminar réplicas, y puede utilizarse para tablas [Distributed](/es/engines/table-engines/special/distributed).

Al crear una nueva réplica de la base de datos, esta réplica crea las tablas por sí sola. Si la réplica no ha estado disponible durante mucho tiempo y se ha quedado atrás respecto al log de replicación, comprueba sus metadatos locales con los metadatos actuales en ZooKeeper, mueve las tablas adicionales con datos a una base de datos no replicada independiente (para no eliminar accidentalmente nada superfluo), crea las tablas que faltan y actualiza los nombres de las tablas si se han renombrado. Los datos se replican a nivel de `ReplicatedMergeTree`, es decir, si la tabla no está replicada, los datos no se replicarán (la base de datos solo es responsable de los metadatos).

Se permiten las consultas [`ALTER TABLE FREEZE|ATTACH|FETCH|DROP|DROP DETACHED|DETACH PARTITION|PART`](../../sql-reference/statements/alter/partition.md), pero no se replican. El motor de base de datos solo añadirá/recuperará/eliminará la partición/parte en la réplica actual. Sin embargo, si la propia tabla usa un motor de tabla Replicated, los datos se replicarán después de usar `ATTACH`.

Si solo necesita configurar un clúster sin mantener la replicación de tablas, consulte la funcionalidad [Cluster Discovery](../../operations/cluster-discovery.md).

<div id="usage-example">
  ## Ejemplo de uso
</div>

Crear un clúster con tres hosts:

```sql
node1 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','replica1');
node2 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','shard1','other_replica');
node3 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','{replica}');
```

Crear una base de datos en un clúster con parámetros implícitos:

```sql
CREATE DATABASE r ON CLUSTER default ENGINE=Replicated;
```

Al ejecutar la consulta DDL:

```sql
CREATE TABLE r.rmt (n UInt64) ENGINE=ReplicatedMergeTree ORDER BY n;
```

```text
┌─────hosts────────────┬──status─┬─error─┬─num_hosts_remaining─┬─num_hosts_active─┐
│ shard1|replica1      │    0    │       │          2          │        0         │
│ shard1|other_replica │    0    │       │          1          │        0         │
│ other_shard|r1       │    0    │       │          0          │        0         │
└──────────────────────┴─────────┴───────┴─────────────────────┴──────────────────┘
```

Se muestra la tabla del sistema:

```sql
SELECT cluster, shard_num, replica_num, host_name, host_address, port, is_local
FROM system.clusters WHERE cluster='r';
```

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

Crear una tabla distribuida e insertar los datos:

```sql
node2 :) CREATE TABLE r.d (n UInt64) ENGINE=Distributed('r','r','rmt', n % 2);
node3 :) INSERT INTO r.d SELECT * FROM numbers(10);
node1 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node3 │  [1,3,5,7,9]  │
│ node2 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

Añadir una réplica en otro host:

```sql
node4 :) CREATE DATABASE r ENGINE=Replicated('some/path/r','other_shard','r2');
```

Añadir una réplica en un host adicional si se usa la macro `{uuid}` en `zoo_path`:

```sql
node1 :) SELECT uuid FROM system.databases WHERE database='r';
node4 :) CREATE DATABASE r UUID '<uuid from previous query>' ENGINE=Replicated('some/path/{uuid}','other_shard','r2');
```

La configuración del clúster tendrá este aspecto:

```text
┌─cluster─┬─shard_num─┬─replica_num─┬─host_name─┬─host_address─┬─port─┬─is_local─┐
│ r       │     1     │      1      │   node3   │  127.0.0.1   │ 9002 │     0    │
│ r       │     1     │      2      │   node4   │  127.0.0.1   │ 9003 │     0    │
│ r       │     2     │      1      │   node2   │  127.0.0.1   │ 9001 │     0    │
│ r       │     2     │      2      │   node1   │  127.0.0.1   │ 9000 │     1    │
└─────────┴───────────┴─────────────┴───────────┴──────────────┴──────┴──────────┘
```

La tabla distribuida también recibirá datos del nuevo host:

```sql
node2 :) SELECT materialize(hostName()) AS host, groupArray(n) FROM r.d GROUP BY host;
```

```text
┌─hosts─┬─groupArray(n)─┐
│ node2 │  [1,3,5,7,9]  │
│ node4 │  [0,2,4,6,8]  │
└───────┴───────────────┘
```

<div id="settings">
  ## Configuración
</div>

Se admiten las siguientes configuraciones:

| Configuración                                                                | Predeterminado                 | Descripción                                                                                                                                                                                                                                                                                                                                                     |
| ---------------------------------------------------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `max_broken_tables_ratio`                                                    | 1                              | No recuperar automáticamente la réplica si la proporción de tablas obsoletas respecto al total de tablas es mayor                                                                                                                                                                                                                                               |
| `max_replication_lag_to_enqueue`                                             | 50                             | La réplica lanzará una excepción al intentar ejecutar una consulta si su retraso de replicación es mayor                                                                                                                                                                                                                                                        |
| `wait_entry_commited_timeout_sec`                                            | 3600                           | Las réplicas intentarán cancelar la consulta si se supera el tiempo de espera, pero el host iniciador aún no la ha ejecutado                                                                                                                                                                                                                                    |
| `collection_name`                                                            |                                | Nombre de una colección definida en la configuración del servidor donde se define toda la información para la autenticación del clúster                                                                                                                                                                                                                         |
| `check_consistency`                                                          | true                           | Comprueba la consistencia de los metadatos locales y los metadatos en Keeper, y recupera la réplica si hay inconsistencias                                                                                                                                                                                                                                      |
| `max_retries_before_automatic_recovery`                                      | 10                             | Número máximo de intentos para ejecutar una entrada de la cola antes de marcar la réplica como perdida y recuperarla desde un snapshot (0 significa infinito)                                                                                                                                                                                                   |
| `allow_skipping_old_temporary_tables_ddls_of_refreshable_materialized_views` | false                          | Si está habilitado, al procesar DDLs en bases de datos Replicated, omite la creación y el intercambio de DDLs de las tablas temporales de las vistas materializadas actualizables cuando sea posible                                                                                                                                                            |
| `logs_to_keep`                                                               | 1000                           | Número predeterminado de logs que se conservarán en ZooKeeper para la base de datos Replicated.                                                                                                                                                                                                                                                                 |
| `default_replica_path`                                                       | `/clickhouse/databases/{uuid}` | La ruta de la base de datos en ZooKeeper. Se usa durante la creación de la base de datos si se omiten los argumentos.                                                                                                                                                                                                                                           |
| `default_replica_shard_name`                                                 | `{shard}`                      | El nombre del segmento de la réplica en la base de datos. Se usa durante la creación de la base de datos si se omiten los argumentos.                                                                                                                                                                                                                           |
| `default_replica_name`                                                       | `{replica}`                    | El nombre de la réplica en la base de datos. Se usa durante la creación de la base de datos si se omiten los argumentos.                                                                                                                                                                                                                                        |
| `internal_replication`                                                       | false                          | Si una tabla Distributed creada con el clúster de esta base de datos Replicated enviará datos a una de las réplicas (la replicación interna significa que las réplicas del clúster realizan la replicación por sí mismas) o a todas las réplicas (sin replicación interna significa que la tabla Distributed enviará los datos insertados a todas las réplicas) |

Los valores predeterminados pueden sobrescribirse en el archivo de configuración

```xml
<clickhouse>
    <database_replicated>
        <max_broken_tables_ratio>0.75</max_broken_tables_ratio>
        <max_replication_lag_to_enqueue>100</max_replication_lag_to_enqueue>
        <wait_entry_commited_timeout_sec>1800</wait_entry_commited_timeout_sec>
        <collection_name>postgres1</collection_name>
        <check_consistency>false</check_consistency>
        <max_retries_before_automatic_recovery>5</max_retries_before_automatic_recovery>
        <default_replica_path>/clickhouse/databases/{uuid}</default_replica_path>
        <default_replica_shard_name>{shard}</default_replica_shard_name>
        <default_replica_name>{replica}</default_replica_name>
        <internal_replication>false</internal_replication>
    </database_replicated>
</clickhouse>
```