---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。设置 {#system-tables-system-settings}

包含有关当前用户的会话设置的信息。

列:

-   `name` ([字符串](../../sql-reference/data-types/string.md)) — Setting name.
-   `value` ([字符串](../../sql-reference/data-types/string.md)) — Setting value.
-   `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting is changed from its default value.
-   `description` ([字符串](../../sql-reference/data-types/string.md)) — Short setting description.
-   `min` ([可为空](../../sql-reference/data-types/nullable.md)([字符串](../../sql-reference/data-types/string.md))) — Minimum value of the setting, if any is set via [制约因素](../../operations/settings/constraints-on-settings.md#constraints-on-settings). 如果设置没有最小值，则包含 [NULL](../../sql-reference/syntax.md#null-literal).
-   `max` ([可为空](../../sql-reference/data-types/nullable.md)([字符串](../../sql-reference/data-types/string.md))) — Maximum value of the setting, if any is set via [制约因素](../../operations/settings/constraints-on-settings.md#constraints-on-settings). 如果设置没有最大值，则包含 [NULL](../../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether the current user can change the setting:
    -   `0` — Current user can change the setting.
    -   `1` — Current user can't change the setting.

**示例**

下面的示例演示如何获取有关名称包含的设置的信息 `min_i`.

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

使用 `WHERE changed` 可以是有用的，例如，当你想检查:

-   配置文件中的设置是否正确加载并正在使用。
-   在当前会话中更改的设置。

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**另请参阅**

-   [设置](../../operations/settings/index.md#session-settings-intro)
-   [查询权限](../../operations/settings/permissions-for-queries.md#settings_readonly)
-   [对设置的限制](../../operations/settings/constraints-on-settings.md)
