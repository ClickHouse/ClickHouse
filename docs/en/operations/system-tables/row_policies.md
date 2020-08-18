# system.row_policies {#system_tables-row_policies}

Contains filters for one particular table, as well as a list of roles and/or users which should use this row policy.

Columns:
-    `name` ([String](../sql-reference/data-types/string.md)) — Name of row policies.

-    `short_name` ([String](../sql-reference/data-types/string.md)) — 

-    `database` ([String](../sql-reference/data-types/string.md)) — Name of a remote database.

-    `table` ([String](../sql-reference/data-types/string.md)) — Name of a remote table.

-    `id` ([UUID](../sql-reference/data-types/uuid.md)) — Row policies ID.

-    `storage` ([String](../sql-reference/data-types/string.md)) — Path to the storage of row policies. Configured in the `access_control_path` parameter. 

-    `select_filter` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — 

-    `is_restrictive` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the row policy restricts access to rows.

-    `apply_to_all` ([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows that the row policies set for all roles and/or users.

-    `apply_to_list` ([Array](../sql-reference/data-types/array.md)([String](../sql-reference/data-types/string.md))) — List of the roles and/or users to which the row policies is applied.

-    `apply_to_except` ([Array](../sql-reference/data-types/array.md)([String](../sql-reference/data-types/string.md))) — The row policies is applied to all roles and/or users excepting of the listed ones.

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/row_policies) <!--hide-->
