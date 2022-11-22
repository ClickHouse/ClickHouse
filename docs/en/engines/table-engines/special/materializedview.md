---
sidebar_position: 100
sidebar_label: MaterializedView
---

# MaterializedView Table Engine

Used for implementing materialized views (for more information, see [CREATE VIEW](../../../sql-reference/statements/create/view.md#materialized)). For storing data, it uses a different engine that was specified when creating the view. When reading from a table, it just uses that engine.

[Original article](https://clickhouse.com/docs/en/engines/table-engines/special/materializedview/) <!--hide-->
