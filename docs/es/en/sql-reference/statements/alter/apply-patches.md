---
description: 'Documentación sobre APPLY PATCHES de actualizaciones ligeras'
sidebar_label: 'APPLY PATCHES'
sidebar_position: 47
slug: /sql-reference/statements/alter/apply-patches
title: 'APPLY PATCHES de actualizaciones ligeras'
doc_type: 'reference'
---

import BetaBadge from '@theme/badges/BetaBadge';

<BetaBadge />

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] APPLY PATCHES [IN PARTITION partition_id]
```

El comando activa manualmente la materialización física de las partes de parche creadas por las sentencias [actualización ligera `UPDATE`](/es/sql-reference/statements/update). Fuerza la aplicación de los parches pendientes a las partes de datos reescribiendo solo las columnas afectadas.

:::note

* Solo funciona con tablas de la familia [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (incluidas las tablas [replicadas](../../../engines/table-engines/mergetree-family/replication.md)).
* Se trata de una operación de mutación y se ejecuta de forma asíncrona en segundo plano.
  :::

<div id="when-to-use">
  ## Cuándo usar APPLY PATCHES
</div>

:::tip
En general, no debería ser necesario usar `APPLY PATCHES`
:::

Las partes de parche normalmente se aplican automáticamente durante las fusiones cuando la configuración [`apply_patches_on_merge`](/es/operations/settings/merge-tree-settings#apply_patches_on_merge) está habilitada (de forma predeterminada). Sin embargo, puede que quieras forzar manualmente la aplicación de parches en estos casos:

* Para reducir la sobrecarga de aplicar parches durante las consultas `SELECT`
* Para consolidar varias partes de parche antes de que se acumulen
* Para preparar los datos para copias de seguridad o para exportarlos con los parches ya materializados
* Cuando `apply_patches_on_merge` está deshabilitada y quieres controlar cuándo se aplican los parches

<div id="examples">
  ## Ejemplos
</div>

Aplique todos los parches pendientes a una tabla:

```sql
ALTER TABLE my_table APPLY PATCHES;
```

Aplique parches solo a una partición específica:

```sql
ALTER TABLE my_table APPLY PATCHES IN PARTITION '2024-01';
```

Combínelo con otras operaciones:

```sql
ALTER TABLE my_table APPLY PATCHES, UPDATE column = value WHERE condition;
```

<div id="monitor">
  ## Seguimiento de la aplicación de parches
</div>

Puede seguir el progreso de la aplicación de parches mediante la tabla [`system.mutations`](/es/operations/system-tables/mutations):

```sql
SELECT * FROM system.mutations
WHERE table = 'my_table' AND command LIKE '%APPLY PATCHES%';
```

<div id="see-also">
  ## Véase también
</div>

* [Actualización ligera `UPDATE`](/es/sql-reference/statements/update) - Crear partes de parche mediante actualizaciones ligeras
* [Ajuste `apply_patches_on_merge`](/es/operations/settings/merge-tree-settings#apply_patches_on_merge) - Controlar la aplicación automática de parches durante las fusiones