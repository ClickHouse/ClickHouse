---
toc_priority: 43
toc_title: "\u041f\u0440\u043e\u0444\u0438\u043b\u044c\u0020\u043d\u0430\u0441\u0442\u0440\u043e\u0435\u043a"
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