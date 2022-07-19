---
toc_priority: 35
toc_title: DATABASE
---

# CREATE DATABASE {#query-language-create-database}

Creates a new database.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

## Clauses {#clauses}

### IF NOT EXISTS {#if-not-exists}

If the `db_name` database already exists, then ClickHouse does not create a new database and:

-   Doesn’t throw an exception if clause is specified.
-   Throws an exception if clause isn’t specified.

### ON CLUSTER {#on-cluster}

ClickHouse creates the `db_name` database on all the servers of a specified cluster. More details in a [Distributed DDL](../../../sql-reference/distributed-ddl.md) article.

### ENGINE {#engine}

[MySQL](../../../engines/database-engines/mysql.md) allows you to retrieve data from the remote MySQL server. By default, ClickHouse uses its own [database engine](../../../engines/database-engines/index.md). There’s also a [lazy](../../../engines/database-engines/lazy.md) engine.
