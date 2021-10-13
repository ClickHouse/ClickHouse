# system.merge_tree_settings {#system-merge_tree_settings}

Contains information about settings for `MergeTree` tables.

Columns:

-   `name` (String) — Setting name.
-   `value` (String) — Setting value.
-   `description` (String) — Setting description.
-   `type` (String) — Setting type (implementation specific string value).
-   `changed` (UInt8) — Whether the setting was explicitly defined in the config or explicitly changed.

**Example**
```sql
:) SELECT * FROM system.merge_tree_settings LIMIT 4 FORMAT Vertical;
```

```text
Row 1:
──────
name:        index_granularity
value:       8192
changed:     0
description: How many rows correspond to one primary key value.
type:        SettingUInt64

Row 2:
──────
name:        min_bytes_for_wide_part
value:       0
changed:     0
description: Minimal uncompressed size in bytes to create part in wide format instead of compact
type:        SettingUInt64

Row 3:
──────
name:        min_rows_for_wide_part
value:       0
changed:     0
description: Minimal number of rows to create part in wide format instead of compact
type:        SettingUInt64

Row 4:
──────
name:        merge_max_block_size
value:       8192
changed:     0
description: How many rows in blocks should be formed for merge operations.
type:        SettingUInt64

4 rows in set. Elapsed: 0.001 sec. 
```

[Original article](https://clickhouse.tech/docs/en/operations/system-tables/merge_tree_settings) <!--hide-->
