# system.columns {#system-columns}

Содержит информацию о столбцах всех таблиц.

С помощью этой таблицы можно получить информацию аналогично запросу [DESCRIBE TABLE](../../sql-reference/statements/misc.md#misc-describe-table), но для многих таблиц сразу.

Таблица `system.columns` содержит столбцы (тип столбца указан в скобках):

-   `database` (String) — имя базы данных.
-   `table` (String) — имя таблицы.
-   `name` (String) — имя столбца.
-   `type` (String) — тип столбца.
-   `default_kind` (String) — тип выражения (`DEFAULT`, `MATERIALIZED`, `ALIAS`) значения по умолчанию, или пустая строка.
-   `default_expression` (String) — выражение для значения по умолчанию или пустая строка.
-   `data_compressed_bytes` (UInt64) — размер сжатых данных в байтах.
-   `data_uncompressed_bytes` (UInt64) — размер распакованных данных в байтах.
-   `marks_bytes` (UInt64) — размер засечек в байтах.
-   `comment` (String) — комментарий к столбцу или пустая строка.
-   `is_in_partition_key` (UInt8) — флаг, показывающий включение столбца в ключ партиционирования.
-   `is_in_sorting_key` (UInt8) — флаг, показывающий включение столбца в ключ сортировки.
-   `is_in_primary_key` (UInt8) — флаг, показывающий включение столбца в первичный ключ.
-   `is_in_sampling_key` (UInt8) — флаг, показывающий включение столбца в ключ выборки.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/columns) <!--hide-->
