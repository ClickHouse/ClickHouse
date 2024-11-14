---
slug: /en/operations/system-tables/server_settings
---
# server_settings

Contains information about global settings for the server, which were specified in `config.xml`.
Currently, the table shows only settings from the first layer of `config.xml` and doesn't support nested configs (e.g. [logger](../../operations/server-configuration-parameters/settings.md#logger)).

Columns:

- `name` ([String](../../sql-reference/data-types/string.md)) — Server setting name.
- `value` ([String](../../sql-reference/data-types/string.md)) — Server setting value.
- `default` ([String](../../sql-reference/data-types/string.md)) — Server setting default value.
- `changed` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Shows whether a setting was specified in `config.xml`
- `description` ([String](../../sql-reference/data-types/string.md)) — Short server setting description.
- `type` ([String](../../sql-reference/data-types/string.md)) — Server setting value type.
- `changeable_without_restart` ([Enum8](../../sql-reference/data-types/enum.md)) — Whether the setting can be changed at server runtime. Values:
    - `'No' `
    - `'IncreaseOnly'`
    - `'DecreaseOnly'`
    - `'Yes'`
- `is_obsolete` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) - Shows whether a setting is obsolete.

**Example**

The following example shows how to get information about server settings which name contains `thread_pool`.

``` sql
SELECT *
FROM system.server_settings
WHERE name LIKE '%thread_pool%'
```

``` text
┌─name──────────────────────────────────────────┬─value─┬─default─┬─changed─┬─description─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─type───┬─changeable_without_restart─┬─is_obsolete─┐
│ max_thread_pool_size                          │ 10000 │ 10000   │       0 │ The maximum number of threads that could be allocated from the OS and used for query execution and background operations.                           │ UInt64 │                         No │           0 │
│ max_thread_pool_free_size                     │ 1000  │ 1000    │       0 │ The maximum number of threads that will always stay in a global thread pool once allocated and remain idle in case of insufficient number of tasks. │ UInt64 │                         No │           0 │
│ thread_pool_queue_size                        │ 10000 │ 10000   │       0 │ The maximum number of tasks that will be placed in a queue and wait for execution.                                                                  │ UInt64 │                         No │           0 │
│ max_io_thread_pool_size                       │ 100   │ 100     │       0 │ The maximum number of threads that would be used for IO operations                                                                                  │ UInt64 │                         No │           0 │
│ max_io_thread_pool_free_size                  │ 0     │ 0       │       0 │ Max free size for IO thread pool.                                                                                                                   │ UInt64 │                         No │           0 │
│ io_thread_pool_queue_size                     │ 10000 │ 10000   │       0 │ Queue size for IO thread pool.                                                                                                                      │ UInt64 │                         No │           0 │
│ max_active_parts_loading_thread_pool_size     │ 64    │ 64      │       0 │ The number of threads to load active set of data parts (Active ones) at startup.                                                                    │ UInt64 │                         No │           0 │
│ max_outdated_parts_loading_thread_pool_size   │ 32    │ 32      │       0 │ The number of threads to load inactive set of data parts (Outdated ones) at startup.                                                                │ UInt64 │                         No │           0 │
│ max_unexpected_parts_loading_thread_pool_size │ 32    │ 32      │       0 │ The number of threads to load inactive set of data parts (Unexpected ones) at startup.                                                              │ UInt64 │                         No │           0 │
│ max_parts_cleaning_thread_pool_size           │ 128   │ 128     │       0 │ The number of threads for concurrent removal of inactive data parts.                                                                                │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_size               │ 1000  │ 1000    │       0 │ The maximum number of threads that would be used for IO operations for BACKUP queries                                                               │ UInt64 │                         No │           0 │
│ max_backups_io_thread_pool_free_size          │ 0     │ 0       │       0 │ Max free size for backups IO thread pool.                                                                                                           │ UInt64 │                         No │           0 │
│ backups_io_thread_pool_queue_size             │ 0     │ 0       │       0 │ Queue size for backups IO thread pool.                                                                                                              │ UInt64 │                         No │           0 │
└───────────────────────────────────────────────┴───────┴─────────┴─────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┴────────────────────────────┴─────────────┘

```

Using of `WHERE changed` can be useful, for example, when you want to check
whether settings in configuration files are loaded correctly and are in use.

<!-- -->

``` sql
SELECT * FROM system.server_settings WHERE changed AND name='max_thread_pool_size'
```

**See also**

- [Settings](../../operations/system-tables/settings.md)
- [Configuration Files](../../operations/configuration-files.md)
- [Server Settings](../../operations/server-configuration-parameters/settings.md)
