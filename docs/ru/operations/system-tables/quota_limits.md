# system.quota_limits {#system_tables-quota_limits}

Содержит информацию о максимумах для всех интервалов всех квот. Одной квоте могут соответствовать любое количество строк или ноль.

Столбцы:

-   `quota_name` ([String](../../sql-reference/data-types/string.md)) — имя квоты.
-   `duration` ([UInt32](../../sql-reference/data-types/int-uint.md)) — длина временного интервала для расчета потребления ресурсов, в секундах.
-   `is_randomized_interval` ([UInt8](../../sql-reference/data-types/int-uint.md#uint-ranges)) — логическое значение. Оно показывает, является ли интервал рандомизированным. Интервал всегда начинается в одно и то же время, если он не рандомизирован. Например, интервал в 1 минуту всегда начинается с целого числа минут (то есть он может начинаться в 11:20:00, но никогда не начинается в 11:20:01), интервал в один день всегда начинается в полночь UTC. Если интервал рандомизирован, то самый первый интервал начинается в произвольное время, а последующие интервалы начинаются один за другим. Значения:
    -   `0` — интервал рандомизирован.
    -   `1` — интервал не рандомизирован.
-   `max_queries` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное число запросов.
- `max_query_selects` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное число запросов `SELECT`.
- `max_query_inserts` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное число запросов `INSERT`.
-   `max_errors` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное количество ошибок.
-   `max_result_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное количество строк результата.
-   `max_result_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальный объем оперативной памяти в байтах, используемый для хранения результата запроса.
-   `max_read_rows` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное количество строк, считываемых из всех таблиц и табличных функций, участвующих в запросе.
-   `max_read_bytes` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — максимальное количество байтов, считываемых из всех таблиц и табличных функций, участвующих в запросе.
-   `max_execution_time` ([Nullable](../../sql-reference/data-types/nullable.md)([Float64](../../sql-reference/data-types/float.md))) — максимальное время выполнения запроса, в секундах.