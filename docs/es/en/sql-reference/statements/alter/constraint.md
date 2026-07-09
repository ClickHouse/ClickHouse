---
description: 'Documentación para gestionar restricciones'
sidebar_label: 'CONSTRAINT'
sidebar_position: 43
slug: /sql-reference/statements/alter/constraint
title: 'Gestión de restricciones'
doc_type: 'reference'
---

Las restricciones pueden añadirse, modificarse o eliminarse con la siguiente sintaxis:

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD CONSTRAINT [IF NOT EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY CONSTRAINT [IF EXISTS] constraint_name {CHECK|ASSUME} expression;
ALTER TABLE [db].name [ON CLUSTER cluster] DROP CONSTRAINT [IF EXISTS] constraint_name;
```

Al igual que en la creación de tablas, una restricción puede declararse como `CHECK` (se aplica en `INSERT`) o como `ASSUME` (el optimizador la da por válida sin comprobarla). Consulte [restricciones](../../../sql-reference/statements/create/table.md#constraints) para ver la diferencia entre ambas.

`MODIFY CONSTRAINT` reemplaza la declaración de una restricción existente y mantiene su posición en la definición de la tabla. También puede cambiar el tipo de restricción (por ejemplo, de `CHECK` a `ASSUME`). Equivale a eliminar la restricción y volver a añadirla con la nueva declaración. Si la restricción no existe, la consulta genera un error, a menos que se especifique `IF EXISTS`.

Consulte más información en [restricciones](../../../sql-reference/statements/create/table.md#constraints).

Las consultas añaden, modifican o eliminan metadatos sobre las restricciones de la tabla, por lo que se procesan inmediatamente.

:::tip
La comprobación de la restricción **no se ejecutará** sobre los datos existentes si se añadió o modificó.
:::

Todos los cambios en las tablas replicadas se transmiten a ZooKeeper y también se aplicarán en las demás réplicas.