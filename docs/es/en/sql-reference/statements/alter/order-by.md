---
description: 'Documentación para manipular expresiones de clave'
sidebar_label: 'ORDER BY'
sidebar_position: 41
slug: /sql-reference/statements/alter/order-by
title: 'Manipulación de expresiones de clave'
doc_type: 'reference'
---

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY ORDER BY new_expression
```

El comando cambia la [clave de ordenación](../../../engines/table-engines/mergetree-family/mergetree.md) de la tabla a `new_expression` (una expresión o una tupla de expresiones). La clave primaria permanece igual.

El comando es lightweight en el sentido de que solo cambia los metadatos. Para mantener la propiedad de que las filas de cada data part estén ordenadas por la expresión de la clave de ordenación, no se pueden añadir a la clave de ordenación expresiones que contengan columnas existentes (solo columnas añadidas mediante el comando `ADD COLUMN` en la misma consulta `ALTER`, sin valor por defecto para la columna).

:::note
Solo funciona para tablas de la familia [`MergeTree`](../../../engines/table-engines/mergetree-family/mergetree.md) (incluidas las tablas [replicadas](../../../engines/table-engines/mergetree-family/replication.md)).
:::