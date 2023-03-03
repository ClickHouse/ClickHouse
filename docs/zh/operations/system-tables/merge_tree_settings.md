# system.merge_tree_settings {#system-merge_tree_settings}

包含 `MergeTree` 表的设置 (Setting) 信息。

列:

-   `name` (String) — 设置名称。
-   `value` (String) — 设置的值。
-   `description` (String) — 设置描述。
-   `type` (String) — 设置类型 (执行特定的字符串值)。
-   `changed` (UInt8) — 该设置是否在配置中明确定义或是明确改变。


**示例**
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

[原文](https://clickhouse.com/docs/zh/operations/system-tables/merge_tree_settings) <!--hide-->
