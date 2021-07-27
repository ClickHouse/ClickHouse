---
toc_priority: 46
toc_title: ROLE
---

# ALTER ROLE {#alter-role-statement}

Изменяет роль.

## Синтаксис {#alter-role-syntax}

``` sql
ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/role/) <!--hide-->
