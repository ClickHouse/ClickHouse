---
slug: /ru/sql-reference/statements/create/role
sidebar_position: 40
sidebar_label: "Роль"
---

# CREATE ROLE {#create-role-statement}

Создает [роли](../../../operations/access-rights.md#role-management). Роль — это набор [привилегий](../grant.md#grant-privileges). Пользователь, которому назначена роль, получает все привилегии этой роли.

Синтаксис:

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

## Управление ролями {#managing-roles}

Одному пользователю можно назначить несколько ролей. Пользователи могут применять назначенные роли в произвольных комбинациях с помощью выражения [SET ROLE](../set-role.md). Конечный объем привилегий — это комбинация всех привилегий всех примененных ролей. Если у пользователя имеются привилегии, присвоенные его аккаунту напрямую, они также прибавляются к привилегиям, присвоенным через роли.

Роли по умолчанию применяются при входе пользователя в систему. Установить роли по умолчанию можно с помощью выражений [SET DEFAULT ROLE](../set-role.md#set-default-role) или [ALTER USER](../alter/index.md#alter-user-statement).

Для отзыва роли используется выражение [REVOKE](../../../sql-reference/statements/revoke.md).

Для удаления роли используется выражение [DROP ROLE](../drop.md#drop-role). Удаленная роль автоматически отзывается у всех пользователей, которым была назначена.

## Примеры {#create-role-examples}

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

Такая последовательность запросов создаст роль `accountant`, у которой есть привилегия на чтение из базы данных `accounting`.

Назначить роль `accountant` аккаунту `mira`:

```sql
GRANT accountant TO mira;
```

После назначения роли пользователь может ее применить и выполнять разрешенные ей запросы. Например:

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```
