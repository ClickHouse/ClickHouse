---
slug: /ja/sql-reference/statements/alter/role
sidebar_position: 46
sidebar_label: ROLE
---

## ALTER ROLE

ロールを変更します。

構文:

``` sql
ALTER ROLE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

