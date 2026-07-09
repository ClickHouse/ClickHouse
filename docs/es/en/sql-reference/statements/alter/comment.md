---
description: 'Documentación de ALTER TABLE ... MODIFY COMMENT, que permite
agregar, modificar o eliminar comentarios de tablas'
sidebar_label: 'ALTER TABLE ... MODIFY COMMENT'
sidebar_position: 51
slug: /sql-reference/statements/alter/comment
title: 'ALTER TABLE ... MODIFY COMMENT'
keywords: ['ALTER TABLE', 'MODIFY COMMENT']
doc_type: 'reference'
---

Agrega, modifica o elimina el comentario de una tabla, independientemente de si se haya definido
previamente o no. El cambio del comentario se refleja tanto en [`system.tables`](../../../operations/system-tables/tables.md)
como en la consulta `SHOW CREATE TABLE`.

<div id="syntax">
  ## Sintaxis
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## Ejemplos
</div>

Para crear una tabla con un comentario:

```sql title="Query"
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

Para modificar el comentario de la tabla:

```sql title="Query"
ALTER TABLE table_with_comment 
MODIFY COMMENT 'new comment on a table';
```

Para ver el comentario modificado:

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment────────────────┐
│ new comment on a table │
└────────────────────────┘
```

Para eliminar el comentario de la tabla:

```sql title="Query"
ALTER TABLE table_with_comment MODIFY COMMENT '';
```

Para comprobar que se eliminó el comentario:

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="caveats">
  ## Advertencias
</div>

En las tablas Replicated, el comentario puede ser diferente en cada réplica.
La modificación del comentario se aplica a una sola réplica.

Esta funcionalidad está disponible desde la versión 23.9. No funciona en versiones anteriores de
ClickHouse.

<div id="related-content">
  ## Contenido relacionado
</div>

* cláusula [`COMMENT`](/es/sql-reference/statements/create/table#comment-clause)
* [`ALTER DATABASE ... MODIFY COMMENT`](./database-comment.md)