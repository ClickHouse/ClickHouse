# system.settings_profile_elements {#system_tables-settings_profile_elements}

Describes the content of the settings profile:

- Сonstraints.
- Roles and users that the setting applies to.
- Parent settings profiles.

Columns:
-    `profile_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Setting profile name.

-    `user_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — User name.

-    `role_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Role name.

-    `index` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Sequential number of the settings profile element.

-    `setting_name` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Setting name.

-    `value` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Setting value.

-    `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The minimum value of the setting. `NULL` if not set.

-    `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — The maximum value of the setting. NULL if not set.

-    `readonly` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges))) — Profile that allows only read queries.

-    `inherit_profile` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — A parent profile for this setting profile. `NULL` if not set. Setting profile will inherit all the settings' values and constraints (`min`, `max`, `readonly`) from its parent profiles.

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/settings_profile_elements) <!--hide-->
