---
description: 'Documentación para modificar la expresión SAMPLE BY'
sidebar_label: 'SAMPLE BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/sample-by
title: 'Modificación de expresiones de clave de muestreo'
doc_type: 'reference'
---

Están disponibles las siguientes operaciones:

<div id="modify">
  ## MODIFY
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY SAMPLE BY new_expression
```

El comando cambia la [clave de muestreo](../../../engines/table-engines/mergetree-family/mergetree.md) de la tabla a `new_expression` (una expresión o una tupla de expresiones). La clave primaria debe contener la nueva clave de muestreo.

<div id="remove">
  ## ELIMINAR
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] REMOVE SAMPLE BY
```

El comando elimina la [clave de muestreo](../../../engines/table-engines/mergetree-family/mergetree.md) de la tabla.

Los comandos `MODIFY` y `REMOVE` son ligeros, en el sentido de que solo cambian los metadatos o eliminan archivos.

:::note
Solo funciona con tablas de la familia [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) (incluidas las tablas [replicadas](../../../engines/table-engines/mergetree-family/replication.md)).
:::