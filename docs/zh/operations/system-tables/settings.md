# system.settings {#system-tables-system-settings}

包含当前用户会话设置的相关信息。

列:

-   `name` ([字符串](../../sql-reference/data-types/string.md)) — 设置名称。
-   `value` ([字符串](../../sql-reference/data-types/string.md)) — 设置的值。
-   `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示该设置是否从其默认值修改。
-   `description` ([字符串](../../sql-reference/data-types/string.md)) — 该设置的简要描述。
-   `min` ([可为空](../../sql-reference/data-types/nullable.md)([字符串](../../sql-reference/data-types/string.md))) — 该设置的最小值，如果有最小值，则是通过[约束](../../operations/settings/constraints-on-settings.md#constraints-on-settings)设置的。如果该设置没有最小值，则包含 [NULL](../../sql-reference/syntax.md#null-literal).
-   `max` ([可为空](../../sql-reference/data-types/nullable.md)([字符串](../../sql-reference/data-types/string.md))) — 该设置的最大值, 如果有最大值，则是通过[约束](../../operations/settings/constraints-on-settings.md#constraints-on-settings)设置的。如果该设置没有最大值，则包含 [NULL](../../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 当前用户是否可以修改该设置:
    -   `0` — 当前用户可以修改此设置.
    -   `1` — 当前用户不能修改此设置.

**示例**

下面的例子显示了如何获得设置名称中包含`min_i`的设置信息。

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

比如，当你想要检查以下情况时，使用 `WHERE changed` 会很有用:

-   配置文件中的设置是否正确加载，并正在使用。
-   在当前会话中更改过的设置。

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**另请参阅**

-   [设置](../../operations/settings/index.md#session-settings-intro)
-   [查询权限](../../operations/settings/permissions-for-queries.md#settings_readonly)
-   [对设置的约束](../../operations/settings/constraints-on-settings.md)

[原文](https://clickhouse.com/docs/zh/operations/system-tables/settings) <!--hide-->
