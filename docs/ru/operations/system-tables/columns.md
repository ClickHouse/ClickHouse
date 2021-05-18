# system.columns {#system-columns}

Содержит информацию о столбцах всех таблиц.

С помощью этой таблицы можно получить информацию аналогично запросу [DESCRIBE TABLE](../../sql-reference/statements/misc.md#misc-describe-table), но для многих таблиц сразу.

Таблица `system.columns` содержит столбцы (тип столбца указан в скобках):

-   `database` ([String](../../sql-reference/data-types/string.md)) — имя базы данных.
-   `table` ([String](../../sql-reference/data-types/string.md)) — имя таблицы.
-   `name` ([String](../../sql-reference/data-types/string.md)) — имя столбца.
-   `type` ([String](../../sql-reference/data-types/string.md)) — тип столбца.
-   `position` ([UInt64](../../sql-reference/data-types/int-uint.md)) — порядковый номер столбца в таблице (нумерация начинается с 1).
-   `default_kind` ([String](../../sql-reference/data-types/string.md)) — тип выражения (`DEFAULT`, `MATERIALIZED`, `ALIAS`) для значения по умолчанию или пустая строка.
-   `default_expression` ([String](../../sql-reference/data-types/string.md)) — выражение для значения по умолчанию или пустая строка.
-   `data_compressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер сжатых данных в байтах.
-   `data_uncompressed_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер распакованных данных в байтах.
-   `marks_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — размер засечек в байтах.
-   `comment` ([String](../../sql-reference/data-types/string.md)) — комментарий к столбцу или пустая строка.
-   `is_in_partition_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий включение столбца в ключ партиционирования.
-   `is_in_sorting_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий включение столбца в ключ сортировки.
-   `is_in_primary_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий включение столбца в первичный ключ.
-   `is_in_sampling_key` ([UInt8](../../sql-reference/data-types/int-uint.md)) — флаг, показывающий включение столбца в ключ выборки.
-   `compression_codec` ([String](../../sql-reference/data-types/string.md)) — имя кодека сжатия.

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/columns) <!--hide-->
