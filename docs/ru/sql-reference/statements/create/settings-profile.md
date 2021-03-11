---
toc_priority: 9
toc_title: Профиль настроек
---

# CREATE SETTINGS PROFILE {#create-settings-profile-statement}

Создает [профиль настроек](../../../operations/access-rights.md#settings-profiles-management), который может быть присвоен пользователю или роли.

### Синтаксис {#create-settings-profile-syntax}

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

### Пример {#create-settings-profile-syntax}

Создать профиль настроек `max_memory_usage_profile`, который содержит значение и ограничения для настройки `max_memory_usage`. Присвоить профиль пользователю `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/statements/create/settings-profile) 
<!--hide-->