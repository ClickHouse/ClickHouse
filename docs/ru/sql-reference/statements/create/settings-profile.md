---
toc_priority: 43
toc_title: "Профиль настроек"
---

# CREATE SETTINGS PROFILE {#create-settings-profile-statement}

Создает [профили настроек](../../../operations/access-rights.md#settings-profiles-management), которые могут быть присвоены пользователю или роли.

Синтаксис:

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] TO name1 [ON CLUSTER cluster_name1] 
        [, name2 [ON CLUSTER cluster_name2] ...]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

Секция `ON CLUSTER` позволяет создавать профили на кластере, см. [Распределенные DDL запросы](../../../sql-reference/distributed-ddl.md).

## Пример {#create-settings-profile-syntax}

Создать профиль настроек `max_memory_usage_profile`, который содержит значение и ограничения для настройки `max_memory_usage`. Присвоить профиль пользователю `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```

<!--hide-->