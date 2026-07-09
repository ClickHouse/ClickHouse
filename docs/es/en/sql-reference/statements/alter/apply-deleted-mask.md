---
description: 'Documentación de APPLY DELETED MASK para filas eliminadas'
sidebar_label: 'APPLY DELETED MASK'
sidebar_position: 46
slug: /sql-reference/statements/alter/apply-deleted-mask
title: 'APPLY DELETED MASK para filas eliminadas'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] APPLY DELETED MASK [IN PARTITION partition_id]
```

El comando aplica la máscara creada por la [eliminación ligera](/es/sql-reference/statements/delete) y elimina de forma forzada del disco las filas marcadas como eliminadas. Este comando es una mutación pesada y equivale semánticamente a la consulta `ALTER TABLE [db].name DELETE WHERE _row_exists = 0`.

:::note
Solo funciona con tablas de la familia [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (incluidas las tablas [replicadas](../../../engines/table-engines/mergetree-family/replication.md)).
:::

**Vea también**

* [Eliminaciones ligeras](/es/sql-reference/statements/delete)
* [Eliminaciones pesadas](/es/sql-reference/statements/alter/delete.md)