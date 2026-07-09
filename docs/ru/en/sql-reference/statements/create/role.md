---
description: 'Документация по роли'
sidebar_label: 'ROLE'
sidebar_position: 40
slug: /sql-reference/statements/create/role
title: 'CREATE ROLE'
doc_type: 'reference'
---

Создаёт новые [роли](../../../guides/sre/user-management/index.md#role-management). Роль — это набор [привилегий](/ru/sql-reference/statements/grant#granting-privilege-syntax). [Пользователь](../../../sql-reference/statements/create/user.md), которому назначена роль, получает все привилегии этой роли.

Синтаксис:

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

<div id="managing-roles">
  ## Управление ролями
</div>

Пользователю можно назначить несколько ролей. Пользователи могут применять назначенные им роли в произвольных комбинациях с помощью оператора [SET ROLE](../../../sql-reference/statements/set-role.md). Итоговая область привилегий представляет собой совокупность всех привилегий всех применённых ролей. Если у пользователя есть привилегии, выданные непосредственно его учётной записи, они также объединяются с привилегиями, выданными ролями.

У пользователя могут быть роли по умолчанию, которые применяются при входе в систему. Чтобы задать роли по умолчанию, используйте оператор [SET DEFAULT ROLE](/ru/sql-reference/statements/set-role#set-default-role) или оператор [ALTER USER](/ru/sql-reference/statements/alter/user).

Чтобы отозвать роль, используйте оператор [REVOKE](../../../sql-reference/statements/revoke.md).

Чтобы удалить роль, используйте оператор [DROP ROLE](/ru/sql-reference/statements/drop#drop-role). Удалённая роль автоматически отзывается у всех пользователей и ролей, которым она была назначена.

<div id="examples">
  ## Примеры
</div>

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

Эта последовательность запросов создает роль `accountant` с привилегией на чтение данных из базы данных `db`.

Назначение роли пользователю `mira`:

```sql
GRANT accountant TO mira;
```

После назначения роли пользователь может использовать её и выполнять разрешённые запросы. Например:

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```