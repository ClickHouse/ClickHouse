---
slug: /en/operations/system-tables/settings
---
# settings

Contains information about session settings for current user.

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) — Setting name.
- `value` ([String](../../sql-reference/data-types/string.md)) — Setting value.
- `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
- `description` ([String](../../sql-reference/data-types/string.md)) — Short setting description.
- `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [constraints](../../operations/settings/constraints-on-settings.md#constraints-on-settings). If the setting has no minimum value, contains [NULL](../../sql-reference/syntax.md#null-literal).
- `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [constraints](../../operations/settings/constraints-on-settings.md#constraints-on-settings). If the setting has no maximum value, contains [NULL](../../sql-reference/syntax.md#null-literal).
- `readonly` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    - `0` — Current user can change the setting.
    - `1` — Current user can’t change the setting.
- `default` ([String](../../sql-reference/data-types/string.md)) — Setting default value.
- `is_obsolete` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) - Shows whether a setting is obsolete.
- `tier` ([Enum8](../../sql-reference/data-types/enum.md)) — Support level for this feature. ClickHouse features are organized in tiers, varying depending on the current status of their development and the expectations one might have when using them. Values:
    - `'Production'` — The feature is stable, safe to use and does not have issues interacting with other **production** features. .
    - `'Beta'` — The feature is stable and safe. The outcome of using it together with other features is unknown and correctness is not guaranteed. Testing and reports are welcome.
    - `'Experimental'` — The feature is under development. Only intended for developers and ClickHouse enthusiasts. The feature might or might not work and could be removed at any time.
    - `'Obsolete'` — No longer supported. Either it is already removed or it will be removed in future releases.

**Example**

The following example shows how to get information about settings which name contains `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_insert_block_size_%'
FORMAT Vertical
```

``` text
Row 1:
──────
name:        min_insert_block_size_rows
value:       1048449
changed:     0
description: Sets the minimum number of rows in the block that can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     1048449
alias_for:   
is_obsolete: 0
tier:        Production

Row 2:
──────
name:        min_insert_block_size_bytes
value:       268402944
changed:     0
description: Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     268402944
alias_for:   
is_obsolete: 0
tier:        Production

Row 3:
──────
name:        min_insert_block_size_rows_for_materialized_views
value:       0
changed:     0
description: Sets the minimum number of rows in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

**See Also**

- [min_insert_block_size_rows](#min-insert-block-size-rows)
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     0
alias_for:   
is_obsolete: 0
tier:        Production

Row 4:
──────
name:        min_insert_block_size_bytes_for_materialized_views
value:       0
changed:     0
description: Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

**See also**

- [min_insert_block_size_bytes](#min-insert-block-size-bytes)
min:         ᴺᵁᴸᴸ
max:         ᴺᵁᴸᴸ
readonly:    0
type:        UInt64
default:     0
alias_for:   
is_obsolete: 0
tier:        Production
 ```

Using of `WHERE changed` can be useful, for example, when you want to check:

- Whether settings in configuration files are loaded correctly and are in use.
- Settings that changed in the current session.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**See also**

- [Settings](../../operations/settings/index.md#session-settings-intro)
- [Permissions for Queries](../../operations/settings/permissions-for-queries.md#settings_readonly)
- [Constraints on Settings](../../operations/settings/constraints-on-settings.md)
- [SHOW SETTINGS](../../sql-reference/statements/show.md#show-settings) statement
