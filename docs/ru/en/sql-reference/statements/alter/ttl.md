---
description: 'Документация по изменению TTL таблицы'
sidebar_label: 'TTL'
sidebar_position: 44
slug: /sql-reference/statements/alter/ttl
title: 'Изменение TTL таблицы'
doc_type: 'reference'
---

:::note
Если вам нужна подробная информация об использовании TTL для управления устаревшими данными, ознакомьтесь с руководством пользователя [Manage Data with TTL](/ru/guides/developer/ttl.md). Ниже показано, как изменить или удалить существующее правило TTL.
:::

<div id="modify-ttl">
  ## ИЗМЕНИТЬ TTL
</div>

Вы можете изменить [TTL таблицы](../../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) с помощью запроса следующего вида:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] MODIFY TTL ttl_expression;
```

<div id="remove-ttl">
  ## УДАЛЕНИЕ TTL
</div>

Свойство TTL можно удалить из таблицы с помощью следующего запроса:

```sql
ALTER TABLE [db.]table_name [ON CLUSTER cluster] REMOVE TTL
```

**Пример**

Рассмотрим таблицу с `TTL` таблицы:

```sql
CREATE TABLE table_with_ttl
(
    event_time DateTime,
    UserID UInt64,
    Comment String
)
ENGINE MergeTree()
ORDER BY tuple()
TTL event_time + INTERVAL 3 MONTH
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO table_with_ttl VALUES (now(), 1, 'username1');

INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
```

Выполните `OPTIMIZE`, чтобы принудительно выполнить очистку `TTL`:

```sql
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

Из таблицы была удалена вторая строка.

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
└───────────────────────┴─────────┴──────────────┘
```

Теперь удалите `TTL` на уровне таблицы с помощью следующего запроса:

```sql
ALTER TABLE table_with_ttl REMOVE TTL;
```

Повторно вставьте удалённую строку и снова принудительно запустите очистку `TTL` с помощью `OPTIMIZE`:

```sql
INSERT INTO table_with_ttl VALUES (now() - INTERVAL 4 MONTH, 2, 'username2');
OPTIMIZE TABLE table_with_ttl FINAL;
SELECT * FROM table_with_ttl FORMAT PrettyCompact;
```

`TTL` больше нет, поэтому вторая строка не удаляется:

```text
┌─────────event_time────┬──UserID─┬─────Comment──┐
│   2020-12-11 12:44:57 │       1 │    username1 │
│   2020-08-11 12:44:57 │       2 │    username2 │
└───────────────────────┴─────────┴──────────────┘
```

**См. также**

* Подробнее о [TTL-выражении](../../../sql-reference/statements/create/table.md#ttl-expression).
* Изменение столбца [с политикой TTL](/ru/sql-reference/statements/alter/ttl).