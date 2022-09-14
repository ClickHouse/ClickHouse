---
toc_priority: 51
toc_title: COMMENT
---

# ALTER TABLE … MODIFY COMMENT {#alter-modify-comment}

Добавляет, изменяет или удаляет комментарий к таблице, независимо от того, был ли он установлен раньше или нет. Изменение комментария отражается как в системной таблице [system.tables](../../../operations/system-tables/tables.md), так и в результате выполнения запроса `SHOW CREATE TABLE`.

**Синтаксис**

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

**Примеры**

Создание таблицы с комментарием (для более подробной информации смотрите секцию [COMMENT](../../../sql-reference/statements/create/table.md#comment-table)):

``` sql
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

Изменение комментария:

``` sql
ALTER TABLE table_with_comment MODIFY COMMENT 'new comment on a table';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'table_with_comment';
```

Вывод нового комментария:

```text
┌─comment────────────────┐
│ new comment on a table │
└────────────────────────┘
```

Удаление комментария:

``` sql
ALTER TABLE table_with_comment MODIFY COMMENT '';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'table_with_comment';
```

Вывод удаленного комментария:

```text
┌─comment─┐
│         │
└─────────┘
```
