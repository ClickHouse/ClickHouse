---
description: 'Documentation for Settings Profile'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 48
slug: /sql-reference/statements/alter/settings-profile
title: 'ALTER SETTINGS PROFILE'
---

Changes settings profiles.

Syntax:

```sql
ALTER SETTINGS PROFILE [IF EXISTS] name1 [RENAME TO new_name |, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [DROP ALL PROFILES]
    [DROP ALL SETTINGS]
    [DROP SETTINGS variable [,...] ]
    [DROP PROFILES 'profile_name' [,...] ]
    [ADD|MODIFY SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
    [ADD PROFILES 'profile_name' [,...] ]
```
