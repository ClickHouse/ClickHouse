---
toc_priority: 40
toc_title: ATTACH
---

# ATTACH Statement {#attach}

This query is exactly the same as [CREATE](../../sql-reference/statements/create/table.md), but

-   Instead of the word `CREATE` it uses the word `ATTACH`.
-   The query does not create data on the disk, but assumes that data is already in the appropriate places, and just adds information about the table to the server.
    After executing an ATTACH query, the server will know about the existence of the table.

If the table was previously detached ([DETACH](../../sql-reference/statements/detach.md)), meaning that its structure is known, you can use shorthand without defining the structure.

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

This query is used when starting the server. The server stores table metadata as files with `ATTACH` queries, which it simply runs at launch (with the exception of system tables, which are explicitly created on the server).
