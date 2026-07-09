---
description: 'Documentação para ALTER TABLE ... MODIFY COMMENT, que permite
adicionar, modificar ou remover comentários de tabela'
sidebar_label: 'ALTER TABLE ... MODIFY COMMENT'
sidebar_position: 51
slug: /sql-reference/statements/alter/comment
title: 'ALTER TABLE ... MODIFY COMMENT'
keywords: ['ALTER TABLE', 'MODIFY COMMENT']
doc_type: 'reference'
---

Adiciona, modifica ou remove um comentário de tabela, independentemente de ele já ter sido definido ou não. A alteração no comentário é refletida tanto em [`system.tables`](../../../operations/system-tables/tables.md)
quanto na consulta `SHOW CREATE TABLE`.

<div id="syntax">
  ## Sintaxe
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## Exemplos
</div>

Para criar uma tabela com comentário:

```sql title="Query"
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

Para alterar o comentário da tabela:

```sql title="Query"
ALTER TABLE table_with_comment 
MODIFY COMMENT 'new comment on a table';
```

Para ver o comentário modificado:

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

Para remover o comentário da tabela:

```sql title="Query"
ALTER TABLE table_with_comment MODIFY COMMENT '';
```

Para verificar se o comentário foi removido:

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
  ## Ressalvas
</div>

Em tabelas Replicated, o comentário pode ser diferente entre as réplicas.
A alteração do comentário se aplica a uma única réplica.

Esse recurso está disponível desde a versão 23.9. Ele não funciona em versões anteriores do
ClickHouse.

<div id="related-content">
  ## Conteúdo relacionado
</div>

* cláusula [`COMMENT`](/pt-BR/sql-reference/statements/create/table#comment-clause)
* [`ALTER DATABASE ... MODIFY COMMENT`](./database-comment.md)