# system.opentelemetry_span_log {#system_tables-opentelemetry_span_log}

Содержит информацию о [trace spans](https://opentracing.io/docs/overview/spans/) для выполненных запросов.

Столбцы:

-   `trace_id` ([UUID](../../sql-reference/data-types/uuid.md)) — идентификатор трассировки для выполненного запроса.

-   `span_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — идентификатор `trace span`.

-   `parent_span_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — идентификатор родительского `trace span`.

-   `operation_name` ([String](../../sql-reference/data-types/string.md)) — имя операции.

-   `start_time_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — время начала `trace span` (в микросекундах).

-   `finish_time_us` ([UInt64](../../sql-reference/data-types/int-uint.md)) — время окончания `trace span` (в микросекундах).

-   `finish_date` ([Date](../../sql-reference/data-types/date.md)) — дата окончания `trace span`.

-   `attribute.names` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — имена [атрибутов](https://opentelemetry.io/docs/go/instrumentation/#attributes) в зависимости от `trace span`. Заполняются согласно рекомендациям в стандарте [OpenTelemetry](https://opentelemetry.io/).

-   `attribute.values` ([Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md))) — значения атрибутов в зависимости от `trace span`. Заполняются согласно рекомендациям в стандарте `OpenTelemetry`.

**Пример**

Запрос:

``` sql
SELECT * FROM system.opentelemetry_span_log LIMIT 1 FORMAT Vertical;
```

Результат:

``` text
Row 1:
──────
trace_id:         cdab0847-0d62-61d5-4d38-dd65b19a1914
span_id:          701487461015578150
parent_span_id:   2991972114672045096
operation_name:   DB::Block DB::InterpreterSelectQuery::getSampleBlockImpl()
start_time_us:    1612374594529090
finish_time_us:   1612374594529108
finish_date:      2021-02-03
attribute.names:  []
attribute.values: []
```

