---
toc_priority: 43
toc_title: DETACH
---

# DETACH Statement {#detach}

Deletes information about the `name` table from the server. The server stops knowing about the table’s existence.

Syntax:

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

This does not delete the table’s data or metadata. On the next server launch, the server will read the metadata and find out about the table again.

Similarly, a “detached” table can be re-attached using the [ATTACH](../../sql-reference/statements/attach) query (with the exception of system tables, which do not have metadata stored for them).

## DETACH PERMAMENTLY {#detach-permamently}

Deletes information about `name` table or view from the server. Permamently detached tables won't automatically reappear after the server restart.

Syntax:

``` sql
DETACH TABLE/VIEW [IF EXISTS] [db.]name PERMAMENTLY [ON CLUSTER cluster]
```

This statement does not delete the table’s data or metadata.

Permamently detached table or view can be reattached with [ATTACH](../../sql-reference/statements/attach) query and can be shown with [SHOW CREATE TABLE](../../sql-reference/statements/show.md#show-create-table) query.

[Original article](https://clickhouse.tech/docs/en/sql-reference/statements/detach/) <!--hide-->
