---
slug: /ru/operations/system-tables/server_settings
---
# system.server_settings

Содержит информацию о конфигурации сервера. 
В настоящий момент таблица содержит только верхнеуровневые параметры из файла `config.xml` и не поддерживает вложенные конфигурации
(например [logger](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-logger))

Столбцы:

-   `name` ([String](../../sql-reference/data-types/string.md)) — имя настройки.
-   `value` ([String](../../sql-reference/data-types/string.md)) — значение настройки.
-   `default` ([String](../../sql-reference/data-types/string.md)) — значению настройки по умолчанию.
-   `changed` ([UInt8](/sql-reference/data-types/int-uint#integer-ranges)) — показывает, была ли настройка указана в `config.xml` или является значением по-умолчанию.
-   `description` ([String](../../sql-reference/data-types/string.md)) — краткое описание настройки.
-   `type` ([String](../../sql-reference/data-types/string.md)) — тип настройки.

**Пример**

Пример показывает как получить информацию о настройках, имена которых содержат `thread_pool`.

``` sql
SELECT *
FROM system.server_settings
WHERE name LIKE '%thread_pool%'
```

``` text
┌─name─────────────────────────┬─value─┬─default─┬─changed─┬─description─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─type───┐
│ max_thread_pool_size         │ 5000  │ 10000   │       1 │ The maximum number of threads that could be allocated from the OS and used for query execution and background operations.                           │ UInt64 │
│ max_thread_pool_free_size    │ 1000  │ 1000    │       0 │ The maximum number of threads that will always stay in a global thread pool once allocated and remain idle in case of insufficient number of tasks. │ UInt64 │
│ thread_pool_queue_size       │ 10000 │ 10000   │       0 │ The maximum number of tasks that will be placed in a queue and wait for execution.                                                                  │ UInt64 │
│ max_io_thread_pool_size      │ 100   │ 100     │       0 │ The maximum number of threads that would be used for IO operations                                                                                  │ UInt64 │
│ max_io_thread_pool_free_size │ 0     │ 0       │       0 │ Max free size for IO thread pool.                                                                                                                   │ UInt64 │
│ io_thread_pool_queue_size    │ 10000 │ 10000   │       0 │ Queue size for IO thread pool.                                                                                                                      │ UInt64 │
└──────────────────────────────┴───────┴─────────┴─────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴────────┘
```

Использование `WHERE changed` может быть полезно, например, если необходимо проверить, 
что настройки корректно загрузились из конфигурационного файла и используются.

<!-- -->

``` sql
SELECT * FROM system.settings WHERE changed AND name='max_thread_pool_size'
```

**Cм. также**

-   [Настройки](../../operations/system-tables/settings.md)
-   [Конфигурационные файлы](../../operations/configuration-files.md)
-   [Настройки сервера](../../operations/server-configuration-parameters/settings.md)
