---
toc_priority: 48
toc_title: SETTINGS PROFILE
---

# ALTER SETTINGS PROFILE {#alter-settings-profile-statement}

Изменяет профили настроек.

## Синтаксис {#alter-settings-profile-syntax}

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/alter/settings-profile) <!--hide-->