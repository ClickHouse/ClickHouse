# system.quotas_usage {#system_tables-quotas_usage}

Использование квот всеми пользователями.

Столбцы:

-   `quota_name` ([String](../../sql-reference/data-types/string.md)) — Имя квоты.
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — Ключ квоты.
-   `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — Квота используется для текущего пользователя.
-   `start_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md)))) — Время начала расчета потребления ресурсов.
-   `end_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md)))) — Время окончания расчета потребления ресурсов.
-   `duration` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt32](../../sql-reference/data-types/int-uint.md))) — Длина временного интервала для расчета потребления ресурсов, в секундах.
-   `queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Общее количество запросов на этом интервале.
-   `max_queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Максимальное число запросов.
-   `errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Число запросов, вызвавших ошибки.
-   `max_errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Максимальное число ошибок.
-   `result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — The total number of rows given as a result.
-   `max_result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Maximum of source rows read from tables.
-   `result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Объем оперативной памяти в байтах, используемый для хранения результата запроса.
-   `max_result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Максимальный объем оперативной памяти, используемый для хранения результата запроса, в байтах.
-   `read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Общее число исходных строк, считываемых из таблиц для выполнения запроса на всех удаленных серверах.
-   `max_read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Максимальное количество строк, считываемых из всех таблиц и табличных функций, участвующих в запросах.
-   `read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Общее количество байт, считанных из всех таблиц и табличных функций, участвующих в запросах.
-   `max_read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — Максимальное количество байт, считываемых из всех таблиц и табличных функций.
-   `execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — Общее время выполнения запроса, в секундах.
-   `max_execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — Максимальное время выполнения запроса.

## Смотрите также {#see-also}

-   [SHOW QUOTA](../../sql-reference/statements/show.md#show-quota-statement)

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/system_tables/quotas_usage) <!--hide-->
