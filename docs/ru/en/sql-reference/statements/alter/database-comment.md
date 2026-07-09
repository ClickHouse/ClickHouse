---
description: 'Документация по командам ALTER DATABASE ... MODIFY COMMENT,
которые позволяют добавлять, изменять или удалять комментарии базы данных.'
slug: /sql-reference/statements/alter/database-comment
sidebar_position: 51
sidebar_label: 'ALTER DATABASE ... MODIFY COMMENT'
title: 'Команды ALTER DATABASE ... MODIFY COMMENT'
keywords: ['ALTER DATABASE', 'MODIFY COMMENT']
doc_type: 'reference'
---

Добавляет, изменяет или удаляет комментарий базы данных вне зависимости от того, был ли он
задан ранее. Изменение комментария отражается как в [`system.databases`](/ru/operations/system-tables/databases.md),
так и в запросе `SHOW CREATE DATABASE`.

<div id="syntax">
  ## Синтаксис
</div>

```sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## Примеры
</div>

Чтобы создать `DATABASE` с комментарием:

```sql title="Query"
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

Чтобы изменить комментарий:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT 'new comment on a database';
```

Чтобы просмотреть изменённый комментарий:

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

Чтобы удалить комментарий к базе данных:

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT '';
```

Чтобы убедиться, что комментарий удалён:

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
  ## Связанные материалы
</div>

* клауза [`COMMENT`](/ru/sql-reference/statements/create/table#comment-clause)
* [`ALTER TABLE ... MODIFY COMMENT`](./comment.md)