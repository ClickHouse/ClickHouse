---
sidebar_position: 62
sidebar_label: Поддержка OpenTelemetry
---

# [экспериментально] Поддержка OpenTelemetry

ClickHouse поддерживает [OpenTelemetry](https://opentelemetry.io/) — открытый стандарт для сбора трассировок и метрик из распределенного приложения.

:::danger "Предупреждение"
Поддержка стандарта экспериментальная и будет со временем меняться.

## Обеспечение поддержки контекста трассировки в ClickHouse

ClickHouse принимает контекстную информацию трассировки через HTTP заголовок `tracecontext`, как описано в [рекомендации W3C](https://www.w3.org/TR/trace-context/). Также он принимает контекстную информацию через нативный протокол, который используется для связи между серверами ClickHouse или между клиентом и сервером. Для ручного тестирования стандартный заголовок `tracecontext`, содержащий контекст трассировки, может быть передан в `clickhouse-client` через флаги: `--opentelemetry-traceparent` и `--opentelemetry-tracestate`.

Если входящий контекст трассировки не указан, ClickHouse может начать трассировку с вероятностью, задаваемой настройкой [opentelemetry_start_trace_probability](../operations/settings/settings.md#opentelemetry-start-trace-probability).

## Распространение контекста трассировки

Контекст трассировки распространяется на нижестоящие сервисы в следующих случаях:

* При использовании запросов к удаленным серверам ClickHouse, например, при использовании движка таблиц [Distributed](../engines/table-engines/special/distributed.md).

* При использовании табличной функции [url](../sql-reference/table-functions/url.md). Информация контекста трассировки передается в HTTP заголовки.

## Как ClickHouse выполняет трассировку

ClickHouse создает `trace spans` для каждого запроса и некоторых этапов выполнения запроса, таких как планирование запросов или распределенные запросы.

Чтобы анализировать информацию трассировки, ее следует экспортировать в систему мониторинга, поддерживающую OpenTelemetry, такую как [Jaeger](https://jaegertracing.io/) или [Prometheus](https://prometheus.io/). ClickHouse не зависит от конкретной системы мониторинга, вместо этого предоставляя данные трассировки только через системную таблицу. Информация о диапазоне трассировки в OpenTelemetry, [требуемая стандартом](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span), хранится в системной таблице [system.opentelemetry_span_log](../operations/system-tables/opentelemetry_span_log.md).

Таблица должна быть включена в конфигурации сервера, смотрите элемент `opentelemetry_span_log` в файле конфигурации `config.xml`. По умолчанию таблица включена всегда.

Теги или атрибуты сохраняются в виде двух параллельных массивов, содержащих ключи и значения. Для работы с ними используйте [ARRAY JOIN](../sql-reference/statements/select/array-join.md).

