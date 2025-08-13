---
slug: /en/sql-reference/statements/move
sidebar_position: 54
sidebar_label: MOVE
---

# MOVE access entity statement

This statement allows to move an access entity from one access storage to another.

Syntax:

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

Currently, there are five access storages in ClickHouse:
 - `local_directory`
 - `memory`
 - `replicated`
 - `users_xml` (ro)
 - `ldap` (ro)

Examples:

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```
