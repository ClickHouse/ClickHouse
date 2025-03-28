---
description: 'Documentation for ALTER DATABASE ... MODIFY COMMENT Statements'
slug: /sql-reference/statements/alter/database-comment
sidebar_position: 51
sidebar_label: 'UPDATE'
title: 'ALTER DATABASE ... MODIFY COMMENT Statements'
---

# ALTER DATABASE ... MODIFY COMMENT

Adds, modifies, or removes comment to the database, regardless of whether it was set before. Comment change is reflected in both [system.databases](/operations/system-tables/databases.md) and `SHOW CREATE DATABASE` query.

**Syntax**

``` sql
ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

**Examples**

Creating a DATABASE with comment (for more information, see the [COMMENT](/sql-reference/statements/create/table#comment-clause) clause):

``` sql
CREATE DATABASE database_with_comment ENGINE = Memory COMMENT 'The temporary database';
```

Modifying the table comment:

``` sql
ALTER DATABASE database_with_comment MODIFY COMMENT 'new comment on a database';
SELECT comment FROM system.databases WHERE name = 'database_with_comment';
```

Output of a new comment:

```text
┌─comment─────────────────┐
│ new comment on database │
└─────────────────────────┘
```

Removing the database comment:

``` sql
ALTER DATABASE database_with_comment MODIFY COMMENT '';
SELECT comment FROM system.databases WHERE  name = 'database_with_comment';
```

Output of a removed comment:

```text
┌─comment─┐
│         │
└─────────┘
```
