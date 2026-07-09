---
description: 'Документация по роли'
sidebar_label: 'ROLE'
sidebar_position: 46
slug: /sql-reference/statements/alter/role
title: 'ALTER ROLE'
doc_type: 'справочник'
---

Изменяет роли.

Синтаксис:

```sql
ALTER ROLE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP PROFILES 'profile_name' [,...] ]
    [DROP SETTINGS variable [,...] ]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
    [SET variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] [,...] ]
    [ADD PROFILES 'profile_name' [,...] ]
```

`SET variable = value` — это псевдоним для `MODIFY SETTING variable = value`: он изменяет один параметр, сохраняя остальные, в отличие от клаузы `SETTINGS` без ADD, MODIFY или DROP, которая заменяет весь список настроек и также удаляет все наследуемые (родительские) профили.