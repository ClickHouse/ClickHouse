---
description: 'SETTINGS PROFILE 参考文档'
sidebar_label: 'SETTINGS PROFILE'
sidebar_position: 43
slug: /sql-reference/statements/create/settings-profile
title: 'CREATE SETTINGS PROFILE'
doc_type: 'reference'
---

创建可分配给用户或角色的 [SETTINGS PROFILE](../../../guides/sre/user-management/index.md#settings-profiles-management)。

语法：

```sql
CREATE SETTINGS PROFILE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] 
    [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | INHERIT 'profile_name'] [,...]
    [TO {{role1 | user1 [, role2 | user2 ...]} | NONE | ALL | ALL EXCEPT {role1 | user1 [, role2 | user2 ...]}}]
```

使用 `ON CLUSTER` 子句可以在集群中创建 SETTINGS PROFILE，参见[分布式 DDL](../../../sql-reference/distributed-ddl.md)。

<div id="example">
  ## 示例
</div>

创建用户：

```sql
CREATE USER robin IDENTIFIED BY 'password';
```

创建 `max_memory_usage_profile` 设置 SETTINGS PROFILE，为 `max_memory_usage` 这一设置指定值和约束，并将其分配给用户 `robin`：

```sql
CREATE
SETTINGS PROFILE max_memory_usage_profile SETTINGS max_memory_usage = 100000001 MIN 90000000 MAX 110000000
TO robin
```