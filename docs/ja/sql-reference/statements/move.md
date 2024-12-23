---
slug: /ja/sql-reference/statements/move
sidebar_position: 54
sidebar_label: MOVE
---

# アクセスエンティティのMOVEステートメント

このステートメントは、アクセスエンティティを一つのアクセスストレージから別のストレージへ移動させることができます。

構文:

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

現在、ClickHouseには五つのアクセスストレージがあります:
 - `local_directory`
 - `memory`
 - `replicated`
 - `users_xml` (読み取り専用)
 - `ldap` (読み取り専用)

例:

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```
