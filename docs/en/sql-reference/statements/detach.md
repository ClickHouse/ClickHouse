---
toc_priority: 45
toc_title: DETACH
---

# DETACH Statement {#detach}

Deletes information about the ‘name’ table from the server. The server stops knowing about the table’s existence.

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

This does not delete the table’s data or metadata. On the next server launch, the server will read the metadata and find out about the table again.

Similarly, a “detached” table can be re-attached using the `ATTACH` query (with the exception of system tables, which do not have metadata stored for them).
