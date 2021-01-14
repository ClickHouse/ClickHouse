---
toc_priority: 40
toc_title: REVOKE
---

# 权限取消 {#revoke}

取消用户或角色的权限

## 语法 {#revoke-语法}

**取消用户的权限**

``` sql
REVOKE [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
```

**取消用户的角色**

``` sql
REVOKE [ON CLUSTER cluster_name] [ADMIN OPTION FOR] role [,...] FROM {user | role | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user_name | role_name | CURRENT_USER} [,...]
```

## 说明 {#revoke-description}

要取消某些权限，可使用比要撤回的权限更大范围的权限。例如，当用户有  `SELECT (x,y)`权限时，管理员可执行 `REVOKE SELECT(x,y) ...`, 或 `REVOKE SELECT * ...`, 甚至是 `REVOKE ALL PRIVILEGES ...`来取消原有权限。

### 取消部分权限 {#partial-revokes-dscr}

可以取消部分权限。例如，当用户有 `SELECT *.*` 权限时，可以通过授予对部分库或表的读取权限来撤回原有权限。

## 示例 {#revoke-example}

授权 `john`账号能查询所有库的所有表，除了 `account`库。

``` sql
GRANT SELECT ON *.* TO john;
REVOKE SELECT ON accounts.* FROM john;
```

授权 `mira`账号能查询 `accounts.staff`表的所有列，除了  `wage`这一列。

``` sql
GRANT SELECT ON accounts.staff TO mira;
REVOKE SELECT(wage) ON accounts.staff FROM mira;
```

{## [原始文档](https://clickhouse.tech/docs/en/operations/settings/settings/) ##}
