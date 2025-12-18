---
description: 'Documentation for CREATE DATABASE'
sidebar_label: 'DATABASE'
sidebar_position: 35
slug: /sql-reference/statements/create/database
title: 'CREATE DATABASE'
doc_type: 'reference'
---

# CREATE DATABASE

Creates a new database.

```sql
CREATE [TEMPORARY] DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [COMMENT 'Comment']
```

## Clauses {#clauses}

### IF NOT EXISTS {#if-not-exists}

If the `db_name` database already exists, then ClickHouse does not create a new database and:

- Doesn't throw an exception if clause is specified.
- Throws an exception if clause isn't specified.

### ON CLUSTER {#on-cluster}

ClickHouse creates the `db_name` database on all the servers of a specified cluster. More details in a [Distributed DDL](../../../sql-reference/distributed-ddl.md) article.

### ENGINE {#engine}

By default, ClickHouse uses its own [Atomic](../../../engines/database-engines/atomic.md) database engine. There are also [Lazy](../../../engines/database-engines/lazy.md), [MySQL](../../../engines/database-engines/mysql.md), [PostgresSQL](../../../engines/database-engines/postgresql.md), [MaterializedPostgreSQL](../../../engines/database-engines/materialized-postgresql.md), [Replicated](../../../engines/database-engines/replicated.md), [SQLite](../../../engines/database-engines/sqlite.md).

### COMMENT {#comment}

You can add a comment to the database when you are creating it.

The comment is supported for all database engines.

**Syntax**

```sql
CREATE DATABASE db_name ENGINE = engine(...) COMMENT 'Comment'
```

**Example**

Query:

```sql
CREATE DATABASE db_comment ENGINE = Memory COMMENT 'The temporary database';
SELECT name, comment FROM system.databases WHERE name = 'db_comment';
```

Result:

```text
┌─name───────┬─comment────────────────┐
│ db_comment │ The temporary database │
└────────────┴────────────────────────┘
```

### TEMPORARY {#temporary}
Creates a temporary database that will be automatically deleted when the current session ends.
Temporary databases are visible in the `system.databases` table, but available to access only in the current session.

[ON CLUSTER](#on-cluster) clause and [Replicated](../../../engines/database-engines/replicated.md) engines are not supported for temporary databases.

After creation the user granted all permissions to the database.

**See Also**

- [allow_experimental_temporary_databases](/operations/settings/settings.md#allow_experimental_temporary_databases)
- [show_others_temporary_databases_in_system_tables](/operations/settings/settings.md#show_others_temporary_databases_in_system_tables)
- [temporary_databases_cleanup_async](/operations/server-configuration-parameters/settings.md/#temporary_databases_cleanup_async)
- [temporary_databases_cleanup_interval_sec](/operations/server-configuration-parameters/settings.md/#temporary_databases_cleanup_interval_sec)
