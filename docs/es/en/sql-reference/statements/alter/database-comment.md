---
description: 'Documentación de las sentencias ALTER DATABASE ... MODIFY COMMENT
que permiten añadir, modificar o eliminar comentarios de bases de datos.'
slug: /sql-reference/statements/alter/database-comment
sidebar_position: 51
sidebar_label: 'ALTER DATABASE ... MODIFY COMMENT'
title: 'Sentencias ALTER DATABASE ... MODIFY COMMENT'
keywords: ['ALTER DATABASE', 'MODIFY COMMENT']
doc_type: 'reference'
---

Añade, modifica o elimina un comentario de base de datos, independientemente de si se había establecido
antes o no. El cambio en el comentario se refleja tanto en [`system.databases`](/es/operations/system-tables/databases.md)
como en la consulta `SHOW CREATE DATABASE`.

<div id="syntax">
  ## Sintaxis
</div>

```sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## Ejemplos
</div>

Para crear una `DATABASE` con un comentario:

```sql title="Query"
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

Para modificar el comentario:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT 'new comment on a database';
```

Para ver el comentario modificado:

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE name = 'database_with_comment';
```

```text title="Response"
┌─comment─────────────────┐
│ new comment on database │
└─────────────────────────┘
```

Para eliminar el comentario de la base de datos:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT '';
```

Para comprobar que se eliminó el comentario:

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE  name = 'database_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="related-content">
  ## Contenido relacionado
</div>

* cláusula [`COMMENT`](/es/sql-reference/statements/create/table#comment-clause)
* [`ALTER TABLE ... MODIFY COMMENT`](./comment.md)