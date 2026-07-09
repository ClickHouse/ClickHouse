---
description: '角色文档'
sidebar_label: 'ROLE'
sidebar_position: 40
slug: /sql-reference/statements/create/role
title: 'CREATE ROLE'
doc_type: 'reference'
---

创建新[角色](../../../guides/sre/user-management/index.md#role-management)。角色是一组[特权](/zh/sql-reference/statements/grant#granting-privilege-syntax)的集合。被分配了某个角色的[用户](../../../sql-reference/statements/create/user.md)将获得该角色的全部特权。

语法：

```sql
CREATE ROLE [IF NOT EXISTS | OR REPLACE] name1 [, name2 [,...]] [ON CLUSTER cluster_name]
    [IN access_storage_type]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [CONST|READONLY|WRITABLE|CHANGEABLE_IN_READONLY] | PROFILE 'profile_name'] [,...]
```

<div id="managing-roles">
  ## 管理角色
</div>

一个用户可以被分配多个角色。用户可以通过 [SET ROLE](../../../sql-reference/statements/set-role.md) 语句，以任意组合启用已分配给自己的角色。最终生效的特权范围，是所有已启用角色特权的组合。如果某个用户的用户账户被直接授予了特权，这些特权也会与通过角色授予的特权合并。

用户可以拥有在登录时自动生效的默认角色。要设置默认角色，请使用 [SET DEFAULT ROLE](/zh/sql-reference/statements/set-role#set-default-role) 语句或 [ALTER USER](/zh/sql-reference/statements/alter/user) 语句。

要撤销角色，请使用 [REVOKE](../../../sql-reference/statements/revoke.md) 语句。

要删除角色，请使用 [DROP ROLE](/zh/sql-reference/statements/drop#drop-role) 语句。删除角色后，系统会自动从所有被分配了该角色的用户和角色中撤销该角色。

<div id="examples">
  ## 示例
</div>

```sql
CREATE ROLE accountant;
GRANT SELECT ON db.* TO accountant;
```

这一系列查询会创建角色 `accountant`，该角色拥有读取 `db` 数据库中数据的权限。

将该角色分配给用户 `mira`：

```sql
GRANT accountant TO mira;
```

分配该角色后，用户即可启用该角色并执行允许的查询。例如：

```sql
SET ROLE accountant;
SELECT * FROM db.*;
```