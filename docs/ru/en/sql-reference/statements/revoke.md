---
description: 'Документация по оператору REVOKE'
sidebar_label: 'REVOKE'
sidebar_position: 39
slug: /sql-reference/statements/revoke
title: 'Оператор REVOKE'
doc_type: 'reference'
---

Отзывает привилегии у пользователей или ролей.

<div id="syntax">
  ## Синтаксис
</div>

**Отзыв привилегий у пользователей**

```sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**Отзыв ролей у пользователей**

```sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

<div id="description">
  ## Описание
</div>

Чтобы отозвать определённую привилегию, можно использовать привилегию с более широкой областью действия, чем та, которую требуется отозвать. Например, если у пользователя есть привилегия `SELECT (x,y)`, администратор может выполнить запрос `REVOKE SELECT(x,y) ...`, `REVOKE SELECT * ...` или даже `REVOKE ALL PRIVILEGES ...`, чтобы отозвать эту привилегию.

<div id="partial-revokes">
  ### Частичный отзыв привилегий
</div>

Можно отозвать часть привилегии. Например, если у пользователя есть привилегия `SELECT *.*`, у него можно отозвать привилегию на чтение данных из определённой таблицы или базы данных.

<div id="examples">
  ## Примеры
</div>

Предоставьте учётной записи пользователя `john` привилегию `SELECT` для всех баз данных, кроме `accounts`:

```sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

Предоставьте учётной записи пользователя `mira` привилегию на выборку из всех столбцов таблицы `accounts.staff`, кроме столбца `wage`.

```sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

[Статья в оригинале](/ru/operations/settings/settings/)