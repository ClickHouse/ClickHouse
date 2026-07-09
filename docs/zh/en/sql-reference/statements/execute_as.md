---
description: 'EXECUTE AS 语句文档'
sidebar_label: 'EXECUTE AS'
sidebar_position: 53
slug: /sql-reference/statements/execute_as
title: 'EXECUTE AS 语句'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<CloudNotSupportedBadge />

<div id="execute-as-statement">
  # EXECUTE AS 语句
</div>

允许以其他用户身份执行查询。

<div id="syntax">
  ## 语法
</div>

```sql
EXECUTE AS target_user;
EXECUTE AS target_user subquery;
```

第一种形式 (不带 `subquery`) 会将当前会话中后续的所有查询设置为以指定的 `target_user` 身份执行。

第二种形式 (带 `subquery`) 则仅将指定的 `subquery` 以指定的 `target_user` 身份执行。

要使这两种形式都能生效，需要将配置项 `access_control_improvements.allow_impersonate_user`
设置为 `1`，并授予 `IMPERSONATE` 权限。例如，以下命令

```sql
GRANT IMPERSONATE ON user1 TO user2;
GRANT IMPERSONATE ON * TO user3;
```

允许用户 `user2` 执行 `EXECUTE AS user1 ...` 命令，同时也允许用户 `user3` 以任意用户身份执行命令。

在模拟其他用户身份时，函数 [currentUser()](/zh/sql-reference/functions/other-functions#currentUser) 返回该用户的名称，
而函数 [authenticatedUser()](/zh/sql-reference/functions/other-functions#authenticatedUser) 返回实际通过身份验证的用户名称。

<div id="examples">
  ## 示例
</div>

```sql
SELECT currentUser(), authenticatedUser(); -- outputs "default    default"
CREATE USER james;
EXECUTE AS james SELECT currentUser(), authenticatedUser(); -- outputs "james    default"
```