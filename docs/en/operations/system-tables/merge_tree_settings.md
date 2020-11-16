# system.merge_tree_settings {#system-merge_tree_settings}

Contains information about settings for `MergeTree` tables.

Columns:

-   `name` (String) — Setting name.
-   `value` (String) — Setting value.
-   `description` (String) — Setting description.
-   `type` (String) — Setting type (implementation specific string value).
-   `changed` (UInt8) — Whether the setting was explicitly defined in the config or explicitly changed.

[Original article](https://clickhouse.tech/docs/en/operations/system_tables/merge_tree_settings) <!--hide-->
