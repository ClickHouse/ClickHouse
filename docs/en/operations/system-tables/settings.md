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
- `is_obsolete` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) _ Shows whether a setting is obsolete.

**Example**

The following example shows how to get information about settings which name contains `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_i%'
```

``` text
┌─name───────────────────────────────────────────────_─value─────_─changed─_─description───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────_─min──_─max──_─readonly─_─type─────────_─default───_─alias_for─_─is_obsolete─┐
│ min_insert_block_size_rows                         │ 1048449   │       0 │ Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.                                                                         │ ____ │ ____ │        0 │ UInt64       │ 1048449   │           │           0 │
│ min_insert_block_size_bytes                        │ 268402944 │       0 │ Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.                                                                        │ ____ │ ____ │        0 │ UInt64       │ 268402944 │           │           0 │
│ min_insert_block_size_rows_for_materialized_views  │ 0         │       0 │ Like min_insert_block_size_rows, but applied only during pushing to MATERIALIZED VIEW (default: min_insert_block_size_rows)                                           │ ____ │ ____ │        0 │ UInt64       │ 0         │           │           0 │
│ min_insert_block_size_bytes_for_materialized_views │ 0         │       0 │ Like min_insert_block_size_bytes, but applied only during pushing to MATERIALIZED VIEW (default: min_insert_block_size_bytes)                                         │ ____ │ ____ │        0 │ UInt64       │ 0         │           │           0 │
│ read_backoff_min_interval_between_events_ms        │ 1000      │       0 │ Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time. │ ____ │ ____ │        0 │ Milliseconds │ 1000      │           │           0 │
└────────────────────────────────────────────────────┴───────────┴─────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────
──────────────────────────────────────────────────────┴──────┴──────┴──────────┴──────────────┴───────────┴───────────┴─────────────┘
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
