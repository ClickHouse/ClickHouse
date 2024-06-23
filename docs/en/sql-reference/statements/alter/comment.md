---
slug: /en/sql-reference/statements/alter/comment
sidebar_position: 51
sidebar_label: COMMENT
---

# ALTER TABLE ... MODIFY COMMENT

Adds, modifies, or removes comment to the table, regardless if it was set before or not. Comment change is reflected in both [system.tables](../../../operations/system-tables/tables.md) and `SHOW CREATE TABLE` query.

**Syntax**

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment'
```

**Examples**

Creating a table with comment (for more information, see the [COMMENT](../../../sql-reference/statements/create/table.md#comment-table) clause):

``` sql
CREATE TABLE table_with_comment
(
    `k` UInt64,
    `s` String
)
ENGINE = Memory()
COMMENT 'The temporary table';
```

Modifying the table comment:

``` sql
ALTER TABLE table_with_comment MODIFY COMMENT 'new comment on a table';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'table_with_comment';
```

Output of a new comment:

```text
┌─comment────────────────┐
│ new comment on a table │
└────────────────────────┘
```

Removing the table comment:

``` sql
ALTER TABLE table_with_comment MODIFY COMMENT '';
SELECT comment FROM system.tables WHERE database = currentDatabase() AND name = 'table_with_comment';
```

Output of a removed comment:

```text
┌─comment─┐
│         │
└─────────┘
```

**Caveats**

For Replicated tables, the comment can be different on different replicas. Modifying the comment applies to a single replica.

The feature is available since version 23.9. It does not work in previous ClickHouse versions.
