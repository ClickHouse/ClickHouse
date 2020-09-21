---
toc_priority: 43
toc_title: MaterializedView
---

# MaterializedView Table Engine {#materializedview}

Used for implementing materialized views (for more information, see [CREATE TABLE](../../../sql-reference/statements/create/table.md)). For storing data, it uses a different engine that was specified when creating the view. When reading from a table, it just uses that engine.

[Original article](https://clickhouse.tech/docs/en/operations/table_engines/materializedview/) <!--hide-->
