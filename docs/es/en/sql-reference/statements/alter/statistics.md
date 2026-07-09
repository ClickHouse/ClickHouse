---
description: 'Documentación para manipulación de estadísticas de columnas'
sidebar_label: 'STATISTICS'
sidebar_position: 45
slug: /sql-reference/statements/alter/statistics
title: 'manipulación de estadísticas de columnas'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="manipulating-column-statistics">
  # Manipulación de estadísticas de columnas
</div>

<CloudNotSupportedBadge />

Las siguientes operaciones están disponibles:

* `ALTER TABLE [db].table ADD STATISTICS [IF NOT EXISTS] (column list) TYPE (type list)` - Añade la descripción de las estadísticas a los metadatos de la tabla.

* `ALTER TABLE [db].table MODIFY STATISTICS (column list) TYPE (type list)` - Modifica la descripción de las estadísticas en los metadatos de la tabla.

* `ALTER TABLE [db].table DROP STATISTICS [IF EXISTS] (column list)` - Elimina las estadísticas de los metadatos de las columnas especificadas y borra todos los objetos de estadísticas de todas las partes para las columnas especificadas.

* `ALTER TABLE [db].table CLEAR STATISTICS [IF EXISTS] (column list)` - Borra todos los objetos de estadísticas de todas las partes para las columnas especificadas. Los objetos de estadísticas se pueden reconstruir usando `ALTER TABLE MATERIALIZE STATISTICS`.

* `ALTER TABLE [db.]table MATERIALIZE STATISTICS (ALL | [IF EXISTS] (column list))` - Reconstruye las estadísticas de las columnas. Está implementado como una [mutación](../../../sql-reference/statements/alter/index.md#mutations).

Los dos primeros comandos son ligeros, en el sentido de que solo cambian metadatos o eliminan archivos.

Además, se replican y sincronizan los metadatos de estadísticas a través de ZooKeeper.

<div id="example">
  ## Ejemplo:
</div>

Añadir dos tipos de estadísticas a dos columnas:

```sql
ALTER TABLE t1 MODIFY STATISTICS c, d TYPE TDigest, Uniq;
```

:::note
Las estadísticas solo se admiten en tablas con motor [`*MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (incluidas las variantes [replicadas](../../../engines/table-engines/mergetree-family/replication.md)).
:::