---
description: 'REVOKE 语句文档'
sidebar_label: 'REVOKE'
sidebar_position: 39
slug: /sql-reference/statements/revoke
title: 'REVOKE 语句'
doc_type: 'reference'
---

撤销授予用户或角色的特权。

<div id="syntax">
  ## 语法
</div>

**撤销用户的特权**

```sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**撤销用户的角色**

```sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

<div id="description">
  ## 描述
</div>

要撤销某项特权，可以使用比你打算撤销的特权范围更广的特权。例如，如果某个用户拥有 `SELECT (x,y)` 特权，管理员可以执行 `REVOKE SELECT(x,y) ...`、`REVOKE SELECT * ...`，甚至 `REVOKE ALL PRIVILEGES ...` 查询来撤销该特权。

<div id="partial-revokes">
  ### 部分撤销
</div>

你可以撤销部分特权。例如，如果某个用户拥有 `SELECT *.*` 特权，你可以撤销其对某些表或数据库的读取数据特权。

<div id="examples">
  ## 示例
</div>

授予 `john` 用户账户对除 `accounts` 之外的所有数据库执行 SELECT 的特权：

```sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

向 `mira` 用户账户授予查询 `accounts.staff` 表中除 `wage` 列外所有列的特权。

```sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

[原文](/zh/operations/settings/settings/)