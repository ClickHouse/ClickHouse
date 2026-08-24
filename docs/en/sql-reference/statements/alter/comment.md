---
description: 'Documentation for ALTER TABLE ... MODIFY COMMENT which allow
adding, modifying, or removing table comments'
sidebar_label: 'ALTER TABLE ... MODIFY COMMENT'
sidebar_position: 51
slug: /sql-reference/statements/alter/comment
title: 'ALTER TABLE ... MODIFY COMMENT'
keywords: ['ALTER TABLE', 'MODIFY COMMENT']
doc_type: 'reference'
---

# ALTER TABLE ... MODIFY COMMENT

Adds, modifies, or removes a table comment, regardless of whether it was set 
before or not. The comment change is reflected in both [`system.tables`](../../../operations/system-tables/tables.md) 
and in the `SHOW CREATE TABLE` query.

## Syntax {#syntax}

```sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

## Examples {#examples}

To create a table with a comment:

```sql
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

To modify the table comment:

```sql
ALTER TABLE table_with_comment 
MODIFY COMMENT 'new comment on a table';
```

To view the modified comment:

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

To remove the table comment:

```sql
ALTER TABLE table_with_comment MODIFY COMMENT '';
```

To verify that the comment was removed:

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

## Caveats {#caveats}

For Replicated tables, the comment can be different on different replicas. 
Modifying the comment applies to a single replica.

The feature is available since version 23.9. It does not work in previous 
ClickHouse versions.

## Related content {#related-content}

- [`COMMENT`](/sql-reference/statements/create/table#comment-clause) clause
- [`ALTER DATABASE ... MODIFY COMMENT`](./database-comment.md)
