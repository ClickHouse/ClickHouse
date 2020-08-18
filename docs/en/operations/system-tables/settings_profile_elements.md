# system.settings_profile_elements {#system_tables-settings_profile_elements}

Contains settings and constraints, as well as a list of roles and/or users to which this profile is applied.

Columns:
-    `profile_name` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — Name of setting profile.

-    `user_name` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — User name.

-    `role_name` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — Role name.

-    `index` ([UInt64](../sql-reference/data-types/int-uint.md)) — 

-    `setting_name` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — Setting name.

-    `value` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — Setting value.

-    `min` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — 

-    `max` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — 

-    `readonly` ([Nullable](../sql-reference/data-types/nullable.md)([UInt8](../sql-reference/data-types/int-uint.md#uint-ranges))) — Profile that allows only read queries.

-    `inherit_profile` ([Nullable](../sql-reference/data-types/nullable.md)([String](../sql-reference/data-types/string.md))) — 

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/settings_profile_elements) <!--hide-->
