# settings_profiles

Contains properties of configured setting profiles.

Columns:
-    `name` ([String](../../sql-reference/data-types/string.md)) — Setting profile name.

-    `id` ([UUID](../../sql-reference/data-types/uuid.md)) — Setting profile ID.

-    `storage` ([String](../../sql-reference/data-types/string.md)) — Path to the storage of setting profiles. Configured in the `access_control_path` parameter.

-    `num_elements` ([UInt64](../../sql-reference/data-types/int-uint.md)) — Number of elements for this profile in the `system.settings_profile_elements` table.

-    `apply_to_all` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows that the settings profile set for all roles and/or users.

-    `apply_to_list` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — List of the roles and/or users to which the setting profile is applied.

-    `apply_to_except` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — The setting profile is applied to all roles and/or users excepting of the listed ones.

## See Also {#see-also}

-   [SHOW PROFILES](../../sql-reference/statements/show.md#show-profiles-statement)

[Original article](https://clickhouse.com/docs/en/operations/system-tables/settings_profiles) <!--hide-->
