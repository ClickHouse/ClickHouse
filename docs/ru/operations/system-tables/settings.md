# system.settings {#system-tables-system-settings}

Содержит информацию о сессионных настройках для текущего пользователя.

Столбцы:

-   `name` ([String](../../sql-reference/data-types/string.md)) — имя настройки.
-   `value` ([String](../../sql-reference/data-types/string.md)) — значение настройки.
-   `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — показывает, изменена ли настройка по отношению к значению по умолчанию.
-   `description` ([String](../../sql-reference/data-types/string.md)) — краткое описание настройки.
-   `min` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — минимальное значение настройки, если задано [ограничение](../settings/constraints-on-settings.md#constraints-on-settings). Если нет, то поле содержит [NULL](../../sql-reference/syntax.md#null-literal).
-   `max` ([Nullable](../../sql-reference/data-types/nullable.md)([String](../../sql-reference/data-types/string.md))) — максимальное значение настройки, если задано [ограничение](../settings/constraints-on-settings.md#constraints-on-settings). Если нет, то поле содержит [NULL](../../sql-reference/syntax.md#null-literal).
-   `readonly` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Показывает, может ли пользователь изменять настройку:
    -   `0` — Текущий пользователь может изменять настройку.
    -   `1` — Текущий пользователь не может изменять настройку.

**Пример**

Пример показывает как получить информацию о настройках, имена которых содержат `min_i`.

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

Использование `WHERE changed` может быть полезно, например, если необходимо проверить:

-   Что настройки корректно загрузились из конфигурационного файла и используются.
-   Настройки, изменённые в текущей сессии.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='load_balancing'
```

**Cм. также**

-   [Настройки](../settings/index.md#settings)
-   [Разрешения для запросов](../settings/permissions-for-queries.md#settings_readonly)
-   [Ограничения для значений настроек](../settings/constraints-on-settings.md)

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/settings) <!--hide-->
