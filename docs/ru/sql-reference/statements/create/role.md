---
toc_priority: 40
toc_title: "\u0420\u043e\u043b\u044c"
---

# CREATE ROLE {#create-role-statement}

Создает [роль](../../../operations/access-rights.md#role-management).

### Синтаксис {#create-role-syntax}

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### Описание {#create-role-description}

Роль — это набор [привилегий](../grant.md#grant-privileges). Пользователь, которому назначена роль, получает все привилегии этой роли.

Одному пользователю можно назначить несколько ролей. Пользователи могут применять назначенные роли в произвольных комбинациях с помощью выражения [SET ROLE](../misc.md#set-role-statement). Конечный объем привилегий — это комбинация всех привилегий всех примененных ролей. Если у пользователя имеются привилегии, присвоенные его аккаунту напрямую, они также прибавляются к привилегиям, присвоенным через роли.

Роли по умолчанию применяются при входе пользователя в систему. Установить роли по умолчанию можно с помощью выражений [SET DEFAULT ROLE](../misc.md#set-default-role-statement) или [ALTER USER](../alter/index.md#alter-user-statement).

Для отзыва роли используется выражение [REVOKE](../../../sql-reference/statements/revoke.md).

Для удаления роли используется выражение [DROP ROLE](../misc.md#drop-role-statement). Удаленная роль автоматически отзывается у всех пользователей, которым была назначена.

### Примеры {#create-role-examples}

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

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/create/role) 
<!--hide-->