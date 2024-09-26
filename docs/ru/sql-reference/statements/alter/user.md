---
slug: /ru/sql-reference/statements/alter/user
sidebar_position: 45
sidebar_label: USER
---

# ALTER USER {#alter-user-statement}

Изменяет аккаунты пользователей ClickHouse.

Синтаксис:

``` sql
ALTER USER [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [NOT IDENTIFIED | IDENTIFIED {[WITH {no_password | plaintext_password | sha256_password | sha256_hash | double_sha1_password | double_sha1_hash}] BY {'password' | 'hash'}} | {WITH ldap SERVER 'server_name'} | {WITH kerberos [REALM 'realm']}]
    [[ADD | DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [GRANTEES {user | role | ANY | NONE} [,...] [EXCEPT {user | role} [,...]]]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY | WRITABLE] | PROFILE 'profile_name'] [,...]
```

Для выполнения `ALTER USER` необходима привилегия [ALTER USER](../grant.md#grant-access-management).

## Секция GRANTEES {#grantees}

Определяет пользователей или роли, которым разрешено получать [привилегии](../../../sql-reference/statements/grant.md#grant-privileges) от указанного пользователя при условии, что этому пользователю также предоставлен весь необходимый доступ с использованием [GRANT OPTION](../../../sql-reference/statements/grant.md#grant-privigele-syntax). Параметры секции `GRANTEES`:

-   `user` — пользователь, которому разрешено получать привилегии от указанного пользователя.
-   `role` — роль, которой разрешено получать привилегии от указанного пользователя.
-   `ANY` — любому пользователю или любой роли разрешено получать привилегии от указанного пользователя. Используется по умолчанию.
-   `NONE` — никому не разрешено получать привилегии от указанного пользователя.

Вы можете исключить любого пользователя или роль, используя выражение `EXCEPT`. Например, `ALTER USER user1 GRANTEES ANY EXCEPT user2`. Это означает, что если `user1` имеет привилегии, предоставленные с использованием `GRANT OPTION`, он сможет предоставить их любому, кроме `user2`.

## Примеры {#alter-user-examples}

Установить ролями по умолчанию роли, назначенные пользователю:

``` sql
ALTER USER user DEFAULT ROLE role1, role2;
```

Если роли не были назначены пользователю, ClickHouse выбрасывает исключение.

Установить ролями по умолчанию все роли, назначенные пользователю:

``` sql
ALTER USER user DEFAULT ROLE ALL;
```

Если роль будет впоследствии назначена пользователю, она автоматически станет ролью по умолчанию.

Установить ролями по умолчанию все назначенные пользователю роли кроме `role1` и `role2`:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2;
```

Разрешить пользователю с аккаунтом `john` предоставить свои привилегии пользователю с аккаунтом `jack`:

``` sql
ALTER USER john GRANTEES jack;
```
