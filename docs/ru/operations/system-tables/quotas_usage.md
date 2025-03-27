---
slug: /ru/operations/system-tables/quotas_usage
---
# system.quotas_usage {#system_tables-quotas_usage}

Использование квот всеми пользователями.

Столбцы:

-   `quota_name` ([String](../../sql-reference/data-types/string.md)) — имя квоты.
-   `quota_key` ([String](../../sql-reference/data-types/string.md)) — ключ квоты.
-   `is_current` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — квота используется для текущего пользователя.
-   `start_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md)))) — время начала расчета потребления ресурсов.
-   `end_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md)))) — время окончания расчета потребления ресурсов.
-   `duration` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt32](../../sql-reference/data-types/int-uint.md))) — длина временного интервала для расчета потребления ресурсов, в секундах.
-   `queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — общее количество запросов на этом интервале.
-   `max_queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное число запросов.
-   `query_selects` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — общее количество запросов `SELECT` на этом интервале.
-   `max_query_selects` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное количество запросов `SELECT` на этом интервале.
-   `query_inserts` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — общее количество запросов `INSERT` на этом интервале.
-   `max_query_inserts` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное количество запросов `INSERT` на этом интервале.
-   `errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — число запросов, вызвавших ошибки.
-   `max_errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное число ошибок.
-   `result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — общее количество строк, приведенных в результате.
-   `max_result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное количество исходных строк, считываемых из таблиц.
-   `result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — объем оперативной памяти в байтах, используемый для хранения результата запроса.
-   `max_result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальный объем оперативной памяти, используемый для хранения результата запроса, в байтах.
-   `read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — общее число исходных строк, считываемых из таблиц для выполнения запроса на всех удаленных серверах.
-   `max_read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное количество строк, считываемых из всех таблиц и табличных функций, участвующих в запросах.
-   `read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — общее количество байт, считанных из всех таблиц и табличных функций, участвующих в запросах.
-   `max_read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное количество байт, считываемых из всех таблиц и табличных функций.
- `failed_sequential_authentications` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/float.md))) — Общее количество неудачных попыток подряд ввести пароль. Если пользователь ввел верный пароль до преодоления порогового значения `max_failed_sequential_authentications` то счетчик неудачных попыток будет сброшен.
- `max_failed_sequential_authentications` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/float.md))) — Максимальное количество неудачных попыток подряд ввести пароль.
-   `execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — общее время выполнения запроса, в секундах.
-   `max_execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — максимальное время выполнения запроса.
## Смотрите также {#see-also}

-   [SHOW QUOTA](../../sql-reference/statements/show.md#show-quota-statement)
