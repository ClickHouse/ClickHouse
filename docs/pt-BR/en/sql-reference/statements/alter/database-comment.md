---
description: 'Documentação das instruções ALTER DATABASE ... MODIFY COMMENT,
que permitem adicionar, modificar ou remover comentários de banco de dados.'
slug: /sql-reference/statements/alter/database-comment
sidebar_position: 51
sidebar_label: 'ALTER DATABASE ... MODIFY COMMENT'
title: 'Instruções ALTER DATABASE ... MODIFY COMMENT'
keywords: ['ALTER DATABASE', 'MODIFY COMMENT']
doc_type: 'reference'
---

Adiciona, modifica ou remove um comentário de banco de dados, independentemente de já ter sido definido ou não. A alteração do comentário é refletida tanto em [`system.databases`](/pt-BR/operations/system-tables/databases.md) quanto na consulta `SHOW CREATE DATABASE`.

<div id="syntax">
  ## Sintaxe
</div>

```sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## Exemplos
</div>

Para criar um `DATABASE` com comentário:

```sql title="Query"
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

Para alterar o comentário:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT 'new comment on a database';
```

Para visualizar o comentário modificado:

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

Para remover o comentário de banco de dados:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT '';
```

Para verificar se o comentário foi removido:

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
  ## Conteúdo relacionado
</div>

* cláusula [`COMMENT`](/pt-BR/sql-reference/statements/create/table#comment-clause)
* [`ALTER TABLE ... MODIFY COMMENT`](./comment.md)