# settings

Contains information about session settings for current user.

Columns:

-   `name` ([String](../../sql-reference/data-types/string.md)) — Setting name.
-   `value` ([String](../../sql-reference/data-types/string.md)) — Setting value.
-   `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
-   `description` ([String](../../sql-reference/data-types/string.md)) — Short setting description.
-   `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [constraints](../../operations/settings/constraints-on-settings.md#constraints-on-settings). If the setting has no minimum value, contains [NULL](../../sql-reference/syntax.md#null-literal).
-   `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [constraints](../../operations/settings/constraints-on-settings.md#constraints-on-settings). If the setting has no maximum value, contains [NULL](../../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    -   `0` — Current user can change the setting.
    -   `1` — Current user can’t change the setting.

**Example**

The following example shows how to get information about settings which name contains `min_i`.

``` sql
SELECT *
FROM system.settings
WHERE name LIKE '%min_i%'
```

``` text
┌─name────────────────────────────────────────┬─value─────┬─changed─┬─description───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─min──┬─max──┬─readonly─┐
│ min_insert_block_size_rows                  │ 1048576   │       0 │ Squash blocks passed to INSERT query to specified size in rows, if blocks are not big enough.                                                                         │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ min_insert_block_size_bytes                 │ 268435456 │       0 │ Squash blocks passed to INSERT query to specified size in bytes, if blocks are not big enough.                                                                        │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
│ read_backoff_min_interval_between_events_ms │ 1000      │       0 │ Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time. │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │        0 │
└─────────────────────────────────────────────┴───────────┴─────────┴───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴──────┴──────┴──────────┘
```

Using of `WHERE changed` can be useful, for example, when you want to check:

-   Whether settings in configuration files are loaded correctly and are in use.
-   Settings that changed in the current session.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**See also**

-   [Settings](../../operations/settings/index.md#session-settings-intro)
-   [Permissions for Queries](../../operations/settings/permissions-for-queries.md#settings_readonly)
-   [Constraints on Settings](../../operations/settings/constraints-on-settings.md)
-   [SHOW SETTINGS](../../sql-reference/statements/show.md#show-settings) statement

[Original article](https://clickhouse.com/docs/en/operations/system-tables/settings) <!--hide-->
