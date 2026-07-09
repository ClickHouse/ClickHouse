---
description: '关于 ALTER DATABASE ... MODIFY COMMENT 语句的文档，
该语句允许添加、修改或删除数据库注释。'
slug: /sql-reference/statements/alter/database-comment
sidebar_position: 51
sidebar_label: 'ALTER DATABASE ... MODIFY COMMENT'
title: 'ALTER DATABASE ... MODIFY COMMENT 语句'
keywords: ['ALTER DATABASE', 'MODIFY COMMENT']
doc_type: 'reference'
---

用于添加、修改或删除数据库注释，无论此前是否已设置注释。
注释的变更会同时反映在 [`system.databases`](/zh/operations/system-tables/databases.md)
和 `SHOW CREATE DATABASE` 查询中。

<div id="syntax">
  ## 语法
</div>

```sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## 示例
</div>

如需创建带有注释的 `DATABASE`：

```sql title="Query"
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

要修改注释：

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT 'new comment on a database';
```

要查看修改后的注释：

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE name = 'database_with_comment';
```

```text title="Response"
┌─comment─────────────────┐
│ new comment on database │
└─────────────────────────┘
```

要删除数据库注释：

```sql title="Query"
ALTER DATABASE database_with_comment 
MODIFY COMMENT '';
```

要验证该注释已被移除：

```sql title="Query"
SELECT comment 
FROM system.databases 
WHERE  name = 'database_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="related-content">
  ## 相关内容
</div>

* [`COMMENT`](/zh/sql-reference/statements/create/table#comment-clause) 子句
* [`ALTER TABLE ... MODIFY COMMENT`](./comment.md)