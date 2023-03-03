---
sidebar_position: 43
sidebar_label: SETTINGS PROFILE
---

# CREATE SETTINGS PROFILE

Creates [settings profiles](../../../operations/access-rights.md#settings-profiles-management) that can be assigned to a user or a role.

Syntax:

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] TO name1 [ON CLUSTER cluster_name1]
        [, name2 [ON CLUSTER cluster_name2] ...]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

`ON CLUSTER` clause allows creating settings profiles on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

## Example

Create the `max_memory_usage_profile` settings profile with value and constraints for the `max_memory_usage` setting and assign it to user `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```
