---
toc_priority: 9
toc_title: SETTINGS PROFILE
---

# CREATE SETTINGS PROFILE {#create-settings-profile-statement}

Creates a [settings profile](../../../operations/access-rights.md#settings-profiles-management) that can be assigned to a user or a role.

Syntax:

``` sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

`ON CLUSTER` clause allows creating settings profiles on a cluster, see [Distributed DDL](../../../sql-reference/distributed-ddl.md).

## Example {#create-settings-profile-syntax}

Create the `max_memory_usage_profile` settings profile with value and constraints for the `max_memory_usage` setting and assign it to user `robin`:

``` sql
CREATE SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000 TO robin
```
