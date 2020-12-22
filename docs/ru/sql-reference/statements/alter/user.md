---
toc_priority: 45
toc_title: USER
---

# ALTER USER {#alter-user-statement}

Изменяет аккаунт пользователя ClickHouse.

## Синтаксис {#alter-user-syntax}

``` sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```  

## Описание {#alter-user-dscr}

Для выполнения `ALTER USER` необходима привилегия [ALTER USER](../grant.md#grant-access-management).

## Примеры {#alter-user-examples}

Установить ролями по умолчанию роли, назначенные пользователю:

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

Если роли не были назначены пользователю, ClickHouse выбрасывает исключение.

Установить ролями по умолчанию все роли, назначенные пользователю:

``` sql
ALTER USER user DEFAULT ROLE ALL
```

Если роль будет впоследствии назначена пользователю, она автоматически станет ролью по умолчанию.

Установить ролями по умолчанию все назначенные пользователю роли кроме `role1` и `role2`:

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/user/) <!--hide-->