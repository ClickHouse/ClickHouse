---
description: 'Documentación sobre la manipulación de índices de omisión de datos'
sidebar_label: 'INDEX'
sidebar_position: 42
slug: /sql-reference/statements/alter/skipping-index
title: 'Manipulación de índices de omisión de datos'
toc_hidden_folder: true
doc_type: 'referencia'
---

Están disponibles las siguientes operaciones:

<div id="add-index">
  ## ADD INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] ADD INDEX [IF NOT EXISTS] name expression TYPE type [GRANULARITY value] [FIRST|AFTER name]` - Añade la descripción del índice a los metadatos de la tabla.

<div id="drop-index">
  ## DROP INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] DROP INDEX [IF EXISTS] name` - Elimina la descripción del índice de los metadatos de la tabla y borra los archivos de índice del disco. Se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

<div id="materialize-index">
  ## MATERIALIZE INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] MATERIALIZE INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Reconstruye el índice secundario `name` para la partición `partition_name` especificada. Se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations). Si se omite la parte `IN PARTITION`, reconstruye el índice para todos los datos de la tabla.

<div id="clear-index">
  ## CLEAR INDEX
</div>

`ALTER TABLE [db.]table_name [ON CLUSTER cluster] CLEAR INDEX [IF EXISTS] name [IN PARTITION partition_name]` - Elimina del disco los archivos del índice secundario sin eliminar su descripción. Se implementa como una [mutación](/es/sql-reference/statements/alter/index.md#mutations).

Los comandos `ADD`, `DROP` y `CLEAR` son ligeros en el sentido de que solo modifican metadatos o eliminan archivos.
Además, se replican y sincronizan los metadatos de los índices mediante ClickHouse Keeper o ZooKeeper.

:::note
La manipulación de índices solo es compatible con tablas con el motor [`*MergeTree`](/es/engines/table-engines/mergetree-family/mergetree.md) (incluidas las variantes [replicadas](/es/engines/table-engines/mergetree-family/replication.md)).
:::