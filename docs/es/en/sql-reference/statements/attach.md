---
description: 'Documentación de ATTACH'
sidebar_label: 'ATTACH'
sidebar_position: 40
slug: /sql-reference/statements/attach
title: 'Sentencia ATTACH'
doc_type: 'reference'
---

Adjunta una tabla o un diccionario, por ejemplo, al mover una base de datos a otro servidor.

**Sintaxis**

```sql
ATTACH TABLE|DICTIONARY|DATABASE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster] ...
```

La consulta no crea datos en disco, sino que asume que los datos ya se encuentran en las ubicaciones adecuadas y solo añade al servidor información sobre la tabla, el Diccionario o la base de datos especificados. Después de ejecutar la consulta `ATTACH`, el servidor sabrá que existen la tabla, el Diccionario o la base de datos.

Si una tabla se había desvinculado previamente (consulta [DETACH](../../sql-reference/statements/detach.md)), es decir, si se conoce su estructura, puede usar la forma abreviada sin definirla.

<div id="attach-existing-table">
  ## Adjuntar una tabla existente
</div>

**Sintaxis**

```sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

Esta consulta se usa al iniciar el servidor. El servidor almacena los metadatos de las tablas como archivos con consultas `ATTACH`, que simplemente ejecuta al arrancar (salvo algunas tablas del sistema, que se crean explícitamente en el servidor).

Si la tabla se desvinculó de forma permanente, no volverá a adjuntarse al iniciar el servidor, por lo que debe usar explícitamente la consulta `ATTACH`.

<div id="create-new-table-and-attach-data">
  ## Crear una tabla nueva y adjuntar los datos
</div>

<div id="with-specified-path-to-table-data">
  ### Con una ruta especificada a los datos de la tabla
</div>

La consulta crea una tabla nueva con la estructura especificada y adjunta los datos de la tabla desde el directorio especificado en `user_files`.

**Sintaxis**

```sql
ATTACH TABLE name FROM 'path/to/data/' (col1 Type1, ...)
```

**Ejemplo**

```sql title="Query"
DROP TABLE IF EXISTS test;
INSERT INTO TABLE FUNCTION file('01188_attach/test/data.TSV', 'TSV', 's String, n UInt8') VALUES ('test', 42);
ATTACH TABLE test FROM '01188_attach/test' (s String, n UInt8) ENGINE = File(TSV);
SELECT * FROM test;
```

```sql title="Response"
┌─s────┬──n─┐
│ test │ 42 │
└──────┴────┘
```

<div id="with-specified-table-uuid">
  ### Con un UUID de tabla especificado
</div>

Esta consulta crea una nueva tabla con la estructura proporcionada y adjunta los datos de la tabla con el UUID especificado.
Es compatible con el motor de base de datos [Atomic](../../engines/database-engines/atomic.md).

**Sintaxis**

```sql
ATTACH TABLE name UUID '<uuid>' (col1 Type1, ...)
```

<div id="attach-mergetree-table-as-replicatedmergetree">
  ## Adjuntar una tabla MergeTree como ReplicatedMergeTree
</div>

Permite adjuntar una tabla MergeTree no replicada como ReplicatedMergeTree. La tabla ReplicatedMergeTree se creará con los valores de configuración `default_replica_path` y `default_replica_name`. También es posible adjuntar una tabla replicada como una tabla MergeTree normal.

Tenga en cuenta que los datos de la tabla en ZooKeeper no se ven afectados por esta consulta. Esto significa que debe agregar metadata en ZooKeeper mediante `SYSTEM RESTORE REPLICA` o eliminarla con `SYSTEM DROP REPLICA ... FROM ZKPATH ...` después de adjuntarla.

Si está intentando agregar una réplica a una tabla ReplicatedMergeTree existente, tenga en cuenta que todos los datos locales de la tabla MergeTree convertida se marcarán como detached.

**Sintaxis**

```sql
ATTACH TABLE [db.]name AS [NOT] REPLICATED
```

**Convertir la tabla en replicada**

```sql
DETACH TABLE test;
ATTACH TABLE test AS REPLICATED;
SYSTEM RESTORE REPLICA test;
```

**Convertir la tabla para que deje de estar replicada**

Obtener la ruta de ZooKeeper y el nombre de la réplica de la tabla:

```sql title="Query"
SELECT replica_name, zookeeper_path FROM system.replicas WHERE table='test';
```

```sql title="Response"
┌─replica_name─┬─zookeeper_path─────────────────────────────────────────────┐
│ r1           │ /clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1 │
└──────────────┴────────────────────────────────────────────────────────────┘
```

Adjunte la tabla como no replicada y elimine los datos de la réplica de ZooKeeper:

```sql title="Query"
DETACH TABLE test;
ATTACH TABLE test AS NOT REPLICATED;
SYSTEM DROP REPLICA 'r1' FROM ZKPATH '/clickhouse/tables/401e6a1f-9bf2-41a3-a900-abb7e94dff98/s1';
```

<div id="attach-existing-dictionary">
  ## Adjuntar diccionario existente
</div>

Adjunta un diccionario previamente desvinculado.

**Sintaxis**

```sql
ATTACH DICTIONARY [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

<div id="attach-existing-database">
  ## Adjuntar base de datos existente
</div>

Adjunta una base de datos previamente desvinculada.

**Sintaxis**

```sql
ATTACH DATABASE [IF NOT EXISTS] name [ENGINE=<database engine>] [ON CLUSTER cluster]
```