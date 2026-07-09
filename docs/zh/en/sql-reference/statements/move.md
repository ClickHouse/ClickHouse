---
description: 'MOVE 访问实体语句文档'
sidebar_label: 'MOVE'
sidebar_position: 54
slug: /sql-reference/statements/move
title: 'MOVE 访问实体语句'
doc_type: '参考'
---

该语句用于将访问实体从一种访问存储移动到另一种访问存储。

语法：

```sql
MOVE {USER, ROLE, QUOTA, SETTINGS PROFILE, ROW POLICY} name1 [, name2, ...] TO access_storage_type
```

目前，ClickHouse 中有五种访问存储类型：

* `local_directory`
* `memory`
* `replicated`
* `users_xml` (ro)
* `ldap` (ro)

示例：

```sql
MOVE USER test TO local_directory
```

```sql
MOVE ROLE test TO memory
```