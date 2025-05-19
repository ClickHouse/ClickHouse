---
description: 'Documentation for ALTER DATABASE ... MODIFY COMMENT statements
which allow adding, modifying, or removing database comments.'
sidebar_position: 51
sidebar_label: 'ALTER DATABASE ... MODIFY COMMENT'
title: 'ALTER DATABASE ... MODIFY COMMENT Statements'
keywords: ['ALTER DATABASE', 'MODIFY COMMENT']
---

# ALTER DATABASE ... MODIFY COMMENT

Adds, modifies, or removes a database comment, regardless of whether it was set
before or not. The comment change is reflected in both [`system.databases`](/operations/system-tables/databases.md) 
and the `SHOW CREATE DATABASE` query.

## Syntax 

``` sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

## Examples 

To create a `DATABASE` with a comment:

``` sql
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

To modify the comment:

``` sql
ALTER DATABASE database_with_comment 
MODIFY COMMENT 'new comment on a database';
```

To view the modified comment:

```sql
SELECT comment 
FROM system.databases 
WHERE name = 'database_with_comment';
```

```text
┌─comment─────────────────┐
│ new comment on database │
└─────────────────────────┘
```

To remove the database comment:

``` sql
ALTER DATABASE database_with_comment 
MODIFY COMMENT '';
```

To verify that the comment was removed:

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

## Related content 

- [`COMMENT`](/sql-reference/statements/create/table#comment-clause) clause
- [`ALTER TABLE ... MODIFY COMMENT`](./comment.md)
