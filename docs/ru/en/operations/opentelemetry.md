---
description: 'Руководство по использованию OpenTelemetry для распределённой трассировки и сбора метрик
  в ClickHouse'
sidebar_label: 'Трассировка ClickHouse с OpenTelemetry'
sidebar_position: 62
slug: /operations/opentelemetry
title: 'Трассировка ClickHouse с OpenTelemetry'
doc_type: 'guide'
---

[OpenTelemetry](https://opentelemetry.io/) — это открытый стандарт для сбора трасс и метрик в распределённых приложениях. ClickHouse в определённой степени поддерживает OpenTelemetry.

<div id="supplying-trace-context-to-clickhouse">
  ## Передача контекста трассировки в ClickHouse
</div>

ClickHouse принимает HTTP-заголовки контекста трассировки, как описано в [рекомендации W3C](https://www.w3.org/TR/trace-context/). ClickHouse также принимает контекст трассировки через собственный протокол, который используется для связи между серверами ClickHouse или между клиентом и сервером. Для ручного тестирования заголовки контекста трассировки, соответствующие рекомендации Trace Context, можно передать в `clickhouse-client` с помощью флагов `--opentelemetry-traceparent` и `--opentelemetry-tracestate`.

Если родительский контекст трассировки не передан или переданный контекст трассировки не соответствует указанному выше стандарту W3C, ClickHouse может начать новую трассировку с вероятностью, задаваемой настройкой [opentelemetry&#95;start&#95;trace&#95;probability](/ru/operations/settings/settings#opentelemetry_start_trace_probability).

<div id="propagating-the-trace-context">
  ## Передача контекста трассировки
</div>

Контекст трассировки передаётся в последующие сервисы в следующих случаях:

* Запросы к удалённым серверам ClickHouse, например при использовании движка таблицы [Distributed](../engines/table-engines/special/distributed.md).

* Табличная функция [url](../sql-reference/table-functions/url.md). Информация о контексте трассировки отправляется в HTTP-заголовках.

<div id="tracing-clickhouse-keeper-requests">
  ## Трассировка запросов ClickHouse Keeper
</div>

ClickHouse поддерживает трассировку OpenTelemetry для запросов [ClickHouse Keeper](../guides/sre/keeper/index.md) (сервиса координации, совместимого с ZooKeeper). Эта возможность позволяет подробно отслеживать весь жизненный цикл операций Keeper — от отправки запроса клиентом до его обработки на стороне сервера.

<div id="enabling-keeper-tracing">
  ### Включение трассировки Keeper
</div>

Чтобы включить трассировку запросов Keeper, настройте следующие параметры в конфигурации клиента ZooKeeper/Keeper:

```xml
<clickhouse>
    <zookeeper>
        <node>
            <host>keeper1</host>
            <port>9181</port>
        </node>
        <!-- Enable OpenTelemetry tracing context propagation -->
        <pass_opentelemetry_tracing_context>true</pass_opentelemetry_tracing_context>
    </zookeeper>
</clickhouse>
```

<div id="keeper-span-types">
  ### Типы спанов Keeper
</div>

Когда трассировка включена, ClickHouse создает спаны как для клиентских, так и для серверных операций Keeper:

**Клиентские спаны:**

* `zookeeper.create` — Создание нового узла
* `zookeeper.get` — Получение данных узла
* `zookeeper.set` — Запись данных узла
* `zookeeper.remove` — Удаление узла
* `zookeeper.list` — Получение списка дочерних узлов
* `zookeeper.exists` — Проверка существования узла
* `zookeeper.multi` — Атомарное выполнение нескольких операций
* `zookeeper.client.requests_queue` — Время ожидания запросов в очереди перед отправкой

**Серверные спаны (Keeper):**

* `keeper.receive_request` — Получение и разбор запроса от клиента
* `keeper.dispatcher.requests_queue` — Ожидание запроса в очереди диспетчера
* `keeper.write.pre_commit` — Предварительная обработка запросов на запись перед фиксацией в Raft
* `keeper.write.commit` — Обработка запросов на запись после фиксации в Raft
* `keeper.read.wait_for_write` — Ожидание запросов на чтение, зависящих от запросов на запись
* `keeper.read.process` — Обработка запросов на чтение
* `keeper.dispatcher.responses_queue` — Ожидание ответа в очереди диспетчера
* `keeper.send_response` — Отправка ответа клиенту

<div id="sampling-and-performance">
  ### Сэмплирование и производительность
</div>

Чтобы снизить накладные расходы трассировки, Keeper использует динамическое сэмплирование. Частота сэмплирования автоматически регулируется в диапазоне от 1/10,000 до 1/10 в зависимости от размера запроса. Для мониторинга производительности длительность всех запросов (как сэмплированных, так и не сэмплированных) записывается в метриках-гистограммах.

<div id="tracing-the-clickhouse-itself">
  ## Трассировка в самом ClickHouse
</div>

ClickHouse создает `trace spans` для каждого запроса и некоторых этапов его выполнения, таких как планирование запроса или распределенные запросы.

Чтобы эта информация была полезной, данные трассировки нужно экспортировать в систему мониторинга с поддержкой OpenTelemetry, например [Jaeger](https://jaegertracing.io/) или [Prometheus](https://prometheus.io/). ClickHouse не зависит от какой-либо конкретной системы мониторинга и предоставляет данные трассировки только через системную таблицу. Информация OpenTelemetry о спанах трассировки, [требуемая стандартом](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span), хранится в таблице [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md).

Таблица должна быть включена в конфигурации сервера; см. элемент `opentelemetry_span_log` в файле конфигурации по умолчанию `config.xml`. По умолчанию она включена.

Теги или атрибуты сохраняются в виде двух параллельных массивов, содержащих ключи и значения. Используйте [ARRAY JOIN](../sql-reference/statements/select/array-join.md), чтобы работать с ними.

<div id="log-query-settings">
  ## Настройка log_query_settings
</div>

Настройка [log&#95;query&#95;settings](settings/settings.md) позволяет фиксировать изменения настроек запроса во время его выполнения. Когда она включена, любые изменения настроек запроса записываются в журнал спана OpenTelemetry. Эта возможность особенно полезна в продакшн-среде для отслеживания изменений конфигурации, которые могут повлиять на производительность запроса.

<div id="integration-with-monitoring-systems">
  ## Интеграция с системами мониторинга
</div>

На данный момент не существует готового инструмента для экспорта данных трассировки из ClickHouse в систему мониторинга.

Для тестирования можно настроить экспорт с помощью materialized view на движке [URL](../engines/table-engines/special/url.md) над таблицей [system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md), которое будет отправлять поступающие данные логов в HTTP-конечную точку коллектора трассировки. Например, чтобы отправлять минимальный набор данных спана в экземпляр Zipkin, доступный по адресу `http://localhost:9411`, в формате Zipkin v2 JSON:

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(trace_id)) AS traceId,
    CASE WHEN parent_span_id = 0 THEN '' ELSE lower(hex(parent_span_id)) END AS parentId,
    lower(hex(span_id)) AS id,
    operation_name AS name,
    start_time_us AS timestamp,
    finish_time_us - start_time_us AS duration,
    cast(tuple('clickhouse'), 'Tuple(serviceName text)') AS localEndpoint,
    cast(tuple(
        attribute.values[indexOf(attribute.names, 'db.statement')]),
        'Tuple("db.statement" text)') AS tags
FROM system.opentelemetry_span_log
```

В случае любых ошибок часть данных логов, в которой возникла ошибка, будет незаметно потеряна. Если данные не поступают, проверьте журнал сервера на наличие сообщений об ошибках.

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Создание решения для обсервабилити с ClickHouse — часть 2 — трейсы](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)