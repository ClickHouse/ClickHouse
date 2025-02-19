---
slug: /en/sql-reference/statements/alter/role
sidebar_position: 46
sidebar_label: ROLE
---

## ALTER ROLE

Changes roles.

Syntax:

``` sql
ALTER ROLE [IF EXISTS] name1 [ON CLUSTER cluster_name1] [RENAME TO new_name1]
        [, name2 [ON CLUSTER cluster_name2] [RENAME TO new_name2] ...]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```
