---
description: 'ALTER TABLE ... MODIFY COMMENT 文档，支持添加、修改或删除表注释'
sidebar_label: 'ALTER TABLE ... MODIFY COMMENT'
sidebar_position: 51
slug: /sql-reference/statements/alter/comment
title: 'ALTER TABLE ... MODIFY COMMENT'
keywords: ['ALTER TABLE', 'MODIFY COMMENT']
doc_type: 'reference'
---

添加、修改或删除表注释，无论之前是否已设置。
注释的更改会同时反映在 [`system.tables`](../../../operations/system-tables/tables.md)
和 `SHOW CREATE TABLE` 查询中。

<div id="syntax">
  ## 语法
</div>

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

<div id="examples">
  ## 示例
</div>

要创建带注释的表：

```sql title="Query"
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

要修改表的注释：

```sql title="Query"
ALTER TABLE table_with_comment 
MODIFY COMMENT 'new comment on a table';
```

要查看修改后的注释：

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment────────────────┐
│ new comment on a table │
└────────────────────────┘
```

如需删除表注释：

```sql title="Query"
ALTER TABLE table_with_comment MODIFY COMMENT '';
```

要验证注释是否已被移除：

```sql title="Query"
SELECT comment 
FROM system.tables 
WHERE database = currentDatabase() AND name = 'table_with_comment';
```

```text title="Response"
┌─comment─┐
│         │
└─────────┘
```

<div id="caveats">
  ## 注意事项
</div>

对于 Replicated 表，不同副本上的注释可能不同。
修改注释仅对单个副本生效。

该功能自 23.9 版本起可用。在更早的
ClickHouse 版本中不可用。

<div id="related-content">
  ## 相关内容
</div>

* [`COMMENT`](/zh/sql-reference/statements/create/table#comment-clause) 子句
* [`ALTER DATABASE ... MODIFY COMMENT`](./database-comment.md)