---
slug: /en/operations/system-tables/merge_tree_settings
title: merge_tree_settings
---

Contains information about settings for `MergeTree` tables.

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) — Setting name.
- `value` ([String](../../sql-reference/data-types/string.md)) — Setting value.
- `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Whether the setting was explicitly defined in the config or explicitly changed.
- `description` ([String](../../sql-reference/data-types/string.md)) — Setting description.
- `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [constraints](../../operations/settings/constraints-on-settings.md#constraints-on-settings). If the setting has no minimum value, contains [NULL](../../sql-reference/syntax.md#null-literal).
- `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [constraints](../../operations/settings/constraints-on-settings.md#constraints-on-settings). If the setting has no maximum value, contains [NULL](../../sql-reference/syntax.md#null-literal).
- `readonly` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    - `0` — Current user can change the setting.
    - `1` — Current user can’t change the setting.
- `type` ([String](../../sql-reference/data-types/string.md)) — Setting type (implementation specific string value).
- `is_obsolete` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) - Shows whether a setting is obsolete.
- `tier` ([Enum8](../../sql-reference/data-types/enum.md)) — Support level for this feature. ClickHouse features are organized in tiers, varying depending on the current status of their development and the expectations one might have when using them. Values:
    - `'Production'` — The feature is stable, safe to use and does not have issues interacting with other **production** features. .
    - `'Beta'` — The feature is stable and safe. The outcome of using it together with other features is unknown and correctness is not guaranteed. Testing and reports are welcome.
    - `'Experimental'` — The feature is under development. Only intended for developers and ClickHouse enthusiasts. The feature might or might not work and could be removed at any time.
    - `'Obsolete'` — No longer supported. Either it is already removed or it will be removed in future releases.

**Example**
```sql
SELECT * FROM system.merge_tree_settings LIMIT 4 FORMAT Vertical;
```

```response
Row 1:
──────
name:        min_compress_block_size
value:       0
changed:     0
description: When granule is written, compress the data in buffer if the size of pending uncompressed data is larger or equal than the specified threshold. If this setting is not set, the corresponding global setting is used.
min:         ____
max:         ____
readonly:    0
type:        UInt64
is_obsolete: 0

Row 2:
──────
name:        max_compress_block_size
value:       0
changed:     0
description: Compress the pending uncompressed data in buffer if its size is larger or equal than the specified threshold. Block of data will be compressed even if the current granule is not finished. If this setting is not set, the corresponding global setting is used.
min:         ____
max:         ____
readonly:    0
type:        UInt64
is_obsolete: 0

Row 3:
──────
name:        index_granularity
value:       8192
changed:     0
description: How many rows correspond to one primary key value.
min:         ____
max:         ____
readonly:    0
type:        UInt64
is_obsolete: 0

Row 4:
──────
name:        max_digestion_size_per_segment
value:       268435456
changed:     0
description: Max number of bytes to digest per segment to build GIN index.
min:         ____
max:         ____
readonly:    0
type:        UInt64
is_obsolete: 0

4 rows in set. Elapsed: 0.009 sec.
```
