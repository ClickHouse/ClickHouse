---
description: 'Документация по ALTER TABLE ... MODIFY COMMENT: добавление,
изменение или удаление комментариев к таблице'
sidebar_label: 'ALTER TABLE ... MODIFY COMMENT'
sidebar_position: 51
slug: /sql-reference/statements/alter/comment
title: 'ALTER TABLE ... MODIFY COMMENT'
keywords: ['ALTER TABLE', 'MODIFY COMMENT']
doc_type: 'reference'
---

Добавляет, изменяет или удаляет комментарий к таблице вне зависимости от того, был ли он
задан ранее. Изменение комментария отражается как в [`system.tables`](../../../operations/system-tables/tables.md),
так и в запросе `SHOW CREATE TABLE`.

<div id="syntax">
  ## Синтаксис
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## Примеры
</div>

Чтобы создать таблицу с комментарием:

```sql title="Query"
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

Чтобы изменить комментарий к таблице:

```sql title="Query"
ALTER TABLE table_with_comment 
MODIFY COMMENT 'new comment on a table';
```

Чтобы просмотреть изменённый комментарий:

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

Чтобы удалить комментарий к таблице:

```sql title="Query"
ALTER TABLE table_with_comment MODIFY COMMENT '';
```

Чтобы убедиться, что комментарий удалён:

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
  ## Ограничения
</div>

Для таблиц Replicated комментарий может различаться между репликами.
Изменение комментария применяется только к одной реплике.

Эта возможность доступна начиная с версии 23.9. В более ранних
версиях ClickHouse она не работает.

<div id="related-content">
  ## См. также
</div>

* секция [`COMMENT`](/ru/sql-reference/statements/create/table#comment-clause)
* [`ALTER DATABASE ... MODIFY COMMENT`](./database-comment.md)