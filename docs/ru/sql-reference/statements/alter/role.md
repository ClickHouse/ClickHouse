---
slug: /ru/sql-reference/statements/alter/role
sidebar_position: 46
sidebar_label: ROLE
---

# ALTER ROLE {#alter-role-statement}

Изменяет роли.

Синтаксис:

``` sql
ALTER ROLE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
    [ADD PROFILES 'profile_name' [,...] ]
```
