---
slug: /en/sql-reference/statements/create/settings-profile
sidebar_position: 43
sidebar_label: SETTINGS PROFILE
title: "CREATE SETTINGS PROFILE"
---

Creates [settings profiles](../../../guides/sre/user-management/index.md#settings-profiles-management) that can be assigned to a user or a role.

Syntax:

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

`ON CLUSTER` clause allows creating settings profiles on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

## Example

Create a user:
```sql
CREATE USER robin IDENTIFIED BY 'password';
```

Create the `max_memory_usage_profile` settings profile with value and constraints for the `max_memory_usage` setting and assign it to user `robin`:

``` sql
CREATE
SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000
TO robin
```
