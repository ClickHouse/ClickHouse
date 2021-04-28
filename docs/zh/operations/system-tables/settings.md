# system.settings {#system-tables-system-settings}

包含有关当前用户的会话设置的信息。

列:

-   `name` ([String](../../sql-reference/data-types/string.md)) — 设置名称。
-   `value` ([String](../../sql-reference/data-types/string.md)) — 设置值。
-   `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示设置是否从其默认值更改。
-   `description` ([String](../../sql-reference/data-types/string.md)) — 简短的设置说明。
-   `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 设置的最小值，如果有，则通过 [constaints](../../operations/settings/constraints-on-settings.md#constraints-on-settings)进行设置。如果设置没有最小值，则包含 [NULL](../../sql-reference/syntax.md#null-literal)。
-   `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — 设置的最大值，如果有，则通过 [constaints](../../operations/settings/constraints-on-settings.md#constraints-on-settings)进行设置。如果设置没有最大值，则包含 [NULL](../../sql-reference/syntax.md#null-literal)。
-   `readonly` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — 显示当前用户是否可以更改设置：
    -   `0` — 当前用户可以更改设置。
    -   `1` — 当前用户无法更改设置。

**示例**

下面的示例演示如何获取有关名称包含 `min_i` 的设置的信息。

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

使用 `WHERE changed` 可能很有用，例如，当你想检查:

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

[原始文章](https://clickhouse.tech/docs/en/operations/system_tables/settings) <!--hide-->
