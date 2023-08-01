---
slug: /en/sql-reference/statements/alter/settings-profile
sidebar_position: 48
sidebar_label: SETTINGS PROFILE
---

## ALTER SETTINGS PROFILE

Changes settings profiles.

Syntax:

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] TO name1 [ON CLUSTER cluster_name1] [RENAME TO new_name1]
        [, name2 [ON CLUSTER cluster_name2] [RENAME TO new_name2] ...]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
```
