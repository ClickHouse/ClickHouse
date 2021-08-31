# system.views {#system-views}

Contains the dependencies of all views and the type to which the view belongs. The metadata of the view comes from the [system.tables](tables.md).

Columns:

-   `database` ([String](../../sql-reference/data-types/string.md)) — The name of the database the view is in.

-   `name` ([String](../../sql-reference/data-types/string.md)) — Name of the view.

-   `main_dependency_database` ([String](../../sql-reference/data-types/string.md)) — The name of the database on which the view depends.

-   `main_dependency_table` ([String](../../sql-reference/data-types/string.md)) - The name of the table on which the view depends.

-   `view_type` ([Enum8](../../sql-reference/data-types/enum.md)) — Type of the view. Values:
    -   `'Default' = 1` — [Default views](../../sql-reference/statements/create/view.md#normal). Should not appear in this log.
    -   `'Materialized' = 2` — [Materialized views](../../sql-reference/statements/create/view.md#materialized).
    -   `'Live' = 3` — [Live views](../../sql-reference/statements/create/view.md#live-view).

The `system.tables` table is used in `SHOW TABLES` query implementation.

**Example**

```sql
SELECT * FROM system.views LIMIT 2 FORMAT Vertical;
```

```text
Row 1:
──────
database:                 default
name:                     live_view
main_dependency_database: default
main_dependency_table:    view_source_tb
view_type:                Live

Row 2:
──────
database:                 default
name:                     materialized_view
main_dependency_database: default
main_dependency_table:    view_source_tb
view_type:                Materialized
```

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/views) <!--hide-->
