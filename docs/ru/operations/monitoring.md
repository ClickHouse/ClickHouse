---
toc_priority: 45
toc_title: "Мониторинг"
---

# Мониторинг {#monitoring}

Вы можете отслеживать:

-   Использование аппаратных ресурсов.
-   Метрики сервера ClickHouse.

## Использование ресурсов {#ispolzovanie-resursov}

ClickHouse не отслеживает состояние аппаратных ресурсов самостоятельно.

Рекомендуем контролировать:

-   Загрузку и температуру процессоров.

        Можно использовать [dmesg](https://en.wikipedia.org/wiki/Dmesg), [turbostat](https://www.linux.org/docs/man8/turbostat.html) или другие инструменты.

-   Использование системы хранения, оперативной памяти и сети.

## Метрики сервера ClickHouse {#metriki-servera-clickhouse}

Сервер ClickHouse имеет встроенные инструменты мониторинга.

Для отслеживания событий на сервере используйте логи. Подробнее смотрите в разделе конфигурационного файла [logger](server-configuration-parameters/settings.md#server_configuration_parameters-logger).

ClickHouse собирает:

-   Различные метрики того, как сервер использует вычислительные ресурсы.
-   Общую статистику обработки запросов.

Метрики находятся в таблицах [system.metrics](system-tables/metrics.md#system_tables-metrics), [system.events](system-tables/events.md#system_tables-events) и [system.asynchronous_metrics](system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics).

Можно настроить экспорт метрик из ClickHouse в [Graphite](https://github.com/graphite-project). Смотрите секцию [graphite](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) конфигурационного файла ClickHouse. Перед настройкой экспорта метрик необходимо настроить Graphite, как указано в [официальном руководстве](https://graphite.readthedocs.io/en/latest/install.html).

Можно настроить экспорт метрик из ClickHouse в [Prometheus](https://prometheus.io). Смотрите секцию [prometheus](server-configuration-parameters/settings.md#server_configuration_parameters-prometheus) конфигурационного файла ClickHouse. Перед настройкой экспорта метрик необходимо настроить Prometheus, как указано в [официальном руководстве](https://prometheus.io/docs/prometheus/latest/installation/).

Также, можно отслеживать доступность сервера через HTTP API. Отправьте `HTTP GET` к ресурсу `/ping`. Если сервер доступен, он отвечает `200 OK`.

Для мониторинга серверов в кластерной конфигурации необходимо установить параметр [max_replica_delay_for_distributed_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries) и использовать HTTP ресурс `/replicas_status`. Если реплика доступна и не отстаёт от других реплик, то запрос к `/replicas_status` возвращает `200 OK`. Если реплика отстаёт, то запрос возвращает `503 HTTP_SERVICE_UNAVAILABLE`, включая информацию о размере отставания.

