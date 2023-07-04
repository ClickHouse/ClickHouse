---
slug: /en/sql-reference/table-functions/
sidebar_label: Table Functions
sidebar_position: 1
---

# Table Functions

Table functions are methods for constructing tables.

You can use table functions in:

- [FROM](../../sql-reference/statements/select/from.md) clause of the `SELECT` query.

   The method for creating a temporary table that is available only in the current query. The table is deleted when the query finishes.

- [CREATE TABLE AS table_function()](../../sql-reference/statements/create/table.md) query.

   It's one of the methods of creating a table.

- [INSERT INTO TABLE FUNCTION](../../sql-reference/statements/insert-into.md#inserting-into-table-function) query.

:::note
You can’t use table functions if the [allow_ddl](../../operations/settings/permissions-for-queries.md#settings_allow_ddl) setting is disabled.
:::
