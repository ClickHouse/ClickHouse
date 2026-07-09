---
description: 'MOVE アクセスエンティティ ステートメントのドキュメント'
sidebar_label: 'MOVE'
sidebar_position: 54
slug: /sql-reference/statements/move
title: 'MOVE アクセスエンティティ ステートメント'
doc_type: 'reference'
---

このステートメントでは、アクセスエンティティをあるアクセスストレージから別のアクセスストレージに移動できます。

構文:

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

現在、ClickHouse には 5 つのアクセスストレージがあります:

* `local_directory`
* `memory`
* `replicated`
* `users_xml` (ro)
* `ldap` (ro)

例:

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```