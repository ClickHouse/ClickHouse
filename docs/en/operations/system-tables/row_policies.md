# system.row_policies {#system_tables-row_policies}

Contains filters for one particular table, as well as a list of roles and/or users which should use this row policy.

Columns:
-    `name` ([String](../../sql-reference/data-types/string.md)) — Name of a row policy.

-    `short_name` ([String](../../sql-reference/data-types/string.md)) — Short name of a row policy. Names of row policies are compound, for example: myfilter ON mydb.mytable. Here "myfilter ON mydb.mytable" is the name of the row policy, "myfilter" is it's short name.

-    `database` ([String](../../sql-reference/data-types/string.md)) — Database name.

-    `table` ([String](../../sql-reference/data-types/string.md)) — Table name.

-    `id` ([UUID](../../sql-reference/data-types/uuid.md)) — Row policy ID.

-    `storage` ([String](../../sql-reference/data-types/string.md)) — Name of the directory where the row policy is stored. 

-    `select_filter` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Condition which is used to filter rows.

-    `is_restrictive` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the row policy restricts access to rows, see [CREATE ROW POLICY](../../sql-reference/statements/create/row-policy.md#create-row-policy-as). Value:
- `0` — The row policy is defined with `AS PERMISSIVE` clause.
- `1` — The row policy is defined with `AS RESTRICTIVE` clause.

-    `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows that the row policies set for all roles and/or users.

-    `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — List of the roles and/or users to which the row policies is applied.

-    `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — The row policies is applied to all roles and/or users excepting of the listed ones.

## See Also {#see-also}

-   [SHOW POLICIES](../../sql-reference/statements/show.md#show-policies-statement)

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/row_policies) <!--hide-->
