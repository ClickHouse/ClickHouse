---
sidebar_position: 48
sidebar_label: SETTINGS PROFILE
---

# ALTER SETTINGS PROFILE {#alter-settings-profile-statement}

Изменяет профили настроек.

Синтаксис:

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] TO name1 [ON CLUSTER cluster_name1] [RENAME TO new_name1]
        [, name2 [ON CLUSTER cluster_name2] [RENAME TO new_name2] ...]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

