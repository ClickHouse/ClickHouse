---
description: 'El motor `Atomic` admite las consultas [`DROP TABLE`](#drop-detach-table)
  y [`RENAME TABLE`](#rename-table) sin bloqueo, así como las consultas atómicas [`EXCHANGE TABLES`](#exchange-tables). El motor de base de datos `Atomic` se usa
  de forma predeterminada.'
sidebar_label: 'Atomic'
sidebar_position: 10
slug: /engines/database-engines/atomic
title: 'Atomic'
doc_type: 'reference'
---

El motor `Atomic` admite las consultas [`DROP TABLE`](#drop-detach-table) y [`RENAME TABLE`](#rename-table) sin bloqueo, así como las consultas atómicas [`EXCHANGE TABLES`](#exchange-tables). El motor de base de datos `Atomic` se usa de forma predeterminada en ClickHouse open-source.

:::note
En ClickHouse Cloud, el [motor de base de datos `Shared`](/es/cloud/reference/shared-catalog#shared-database-engine) se usa de forma predeterminada y también admite
las operaciones mencionadas anteriormente.
:::

<div id="creating-a-database">
  ## Crear una base de datos
</div>

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS disk=...];
```

<div id="specifics-and-recommendations">
  ## Particularidades y recomendaciones
</div>

<div id="table-uuid">
  ### UUID de la tabla
</div>

Cada tabla de la base de datos `Atomic` tiene un [UUID](../../sql-reference/data-types/uuid.md) permanente y almacena sus datos en el siguiente directorio:

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

Donde `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` es el UUID de la tabla.

De forma predeterminada, el UUID se genera automáticamente. Sin embargo, los usuarios pueden especificarlo explícitamente al crear una tabla, aunque no se recomienda.

Por ejemplo:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
Puede usar el ajuste [show&#95;table&#95;uuid&#95;in&#95;table&#95;create&#95;query&#95;if&#95;not&#95;nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil) para mostrar el UUID en la consulta `SHOW CREATE`.
:::

<div id="rename-table">
  ### RENAME TABLE
</div>

Las consultas [`RENAME`](../../sql-reference/statements/rename.md) no modifican el UUID ni desplazan los datos de la tabla. Estas consultas se ejecutan de inmediato y no esperan a que terminen otras consultas que estén usando la tabla.

<div id="drop-detach-table">
  ### DROP/DETACH TABLE
</div>

Al usar `DROP TABLE`, no se elimina ningún dato. El motor `Atomic` simplemente marca la tabla como eliminada moviendo sus metadatos a `/clickhouse_path/metadata_dropped/` y notifica al hilo en segundo plano. El retraso antes de la eliminación definitiva de los datos de la tabla se especifica mediante la configuración [`database_atomic_delay_before_drop_table_sec`](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec).
Puede especificar el modo síncrono con el modificador `SYNC`. Use la configuración [`database_atomic_wait_for_drop_and_detach_synchronously`](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously) para ello. En este caso, `DROP` espera a que finalicen las consultas `SELECT`, `INSERT` y otras consultas en ejecución que estén usando la tabla. La tabla se eliminará cuando deje de estar en uso.

<div id="exchange-tables">
  ### EXCHANGE TABLES/DICCIONARIOS
</div>

La consulta [`EXCHANGE`](../../sql-reference/statements/exchange.md) intercambia tablas o diccionarios de forma atómica. Por ejemplo, en lugar de esta operación no atómica:

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```

puedes usar una de tipo Atomic:

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

<div id="replicatedmergetree-in-atomic-database">
  ### ReplicatedMergeTree en una base de datos Atomic
</div>

Para las tablas [`ReplicatedMergeTree`](/es/engines/table-engines/mergetree-family/replication), se recomienda no especificar los parámetros del motor para la ruta en ZooKeeper ni para el nombre de la réplica. En este caso, se usarán los parámetros de configuración [`default_replica_path`](../../operations/server-configuration-parameters/settings.md#default_replica_path) y [`default_replica_name`](../../operations/server-configuration-parameters/settings.md#default_replica_name). Si desea especificar explícitamente los parámetros del motor, se recomienda usar las macros `{uuid}`. Esto garantiza que se generen automáticamente rutas únicas para cada tabla en ZooKeeper.

<div id="metadata-disk">
  ### Disco de metadatos
</div>

Cuando se especifica `disk` en `SETTINGS`, el disco se utiliza para almacenar los archivos de metadatos de la tabla.
Por ejemplo:

```sql
CREATE TABLE db (n UInt64) ENGINE = Atomic SETTINGS disk=disk(type='local', path='/var/lib/clickhouse-disks/db_disk');
```

Si no se especifica, se usa por defecto el disco definido en `database_disk.disk`.

<div id="see-also">
  ## Véase también
</div>

* [system.databases](../../operations/system-tables/databases.md) tabla del sistema