---
alias: []
description: 'Документация по формату Prometheus'
input_format: false
keywords: ['Prometheus']
output_format: true
slug: /interfaces/formats/Prometheus
title: 'Prometheus'
doc_type: 'reference'
---

| Ввод | Вывод | Псевдоним |
| ---- | ----- | --------- |
| ✗    | ✔     |           |

<div id="description">
  ## Описание
</div>

Предоставляет метрики в [текстовом формате экспозиции Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format).

Для этого формата требуется, чтобы выходная таблица была правильно структурирована в соответствии со следующими правилами:

* Обязательны столбцы `name` ([String](/ru/sql-reference/data-types/string.md)) и `value` (number).
* Строки могут необязательно содержать `help` ([String](/ru/sql-reference/data-types/string.md)) и `timestamp` (number).
* Столбец `type` ([String](/ru/sql-reference/data-types/string.md)) должен иметь одно из значений: `counter`, `gauge`, `histogram`, `summary`, `untyped` или быть пустым.
* Каждое значение метрики также может иметь метки `labels` ([Map(String, String)](/ru/sql-reference/data-types/map.md)).
* Несколько последовательных строк могут относиться к одной метрике с разными метками. Таблица должна быть отсортирована по имени метрики (например, с помощью `ORDER BY name`).

Для меток `histogram` и `summary` действуют специальные требования — подробности см. в [документации Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/#histograms-and-summaries).
К строкам с метками `{'count':''}` и `{'sum':''}` применяются специальные правила: они преобразуются в `<metric_name>_count` и `<metric_name>_sum` соответственно.

<div id="example-usage">
  ## Пример использования
</div>

```yaml
┌─name────────────────────────────────┬─type──────┬─help──────────────────────────────────────┬─labels─────────────────────────┬────value─┬─────timestamp─┐
│ http_request_duration_seconds       │ histogram │ A histogram of the request duration.      │ {'le':'0.05'}                  │    24054 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.1'}                   │    33444 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.2'}                   │   100392 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'0.5'}                   │   129389 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'1'}                     │   133988 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'le':'+Inf'}                  │   144320 │             0 │
│ http_request_duration_seconds       │ histogram │                                           │ {'sum':''}                     │    53423 │             0 │
│ http_requests_total                 │ counter   │ Total number of HTTP requests             │ {'method':'post','code':'200'} │     1027 │ 1395066363000 │
│ http_requests_total                 │ counter   │                                           │ {'method':'post','code':'400'} │        3 │ 1395066363000 │
│ metric_without_timestamp_and_labels │           │                                           │ {}                             │    12.47 │             0 │
│ rpc_duration_seconds                │ summary   │ A summary of the RPC duration in seconds. │ {'quantile':'0.01'}            │     3102 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.05'}            │     3272 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.5'}             │     4773 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.9'}             │     9001 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'quantile':'0.99'}            │    76656 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'count':''}                   │     2693 │             0 │
│ rpc_duration_seconds                │ summary   │                                           │ {'sum':''}                     │ 17560473 │             0 │
│ something_weird                     │           │                                           │ {'problem':'division by zero'} │      inf │      -3982045 │
└─────────────────────────────────────┴───────────┴───────────────────────────────────────────┴────────────────────────────────┴──────────┴───────────────┘
```

Будет иметь следующий формат:

```text
# HELP http_request_duration_seconds A histogram of the request duration.
# TYPE http_request_duration_seconds histogram
http_request_duration_seconds_bucket{le="0.05"} 24054
http_request_duration_seconds_bucket{le="0.1"} 33444
http_request_duration_seconds_bucket{le="0.5"} 129389
http_request_duration_seconds_bucket{le="1"} 133988
http_request_duration_seconds_bucket{le="+Inf"} 144320
http_request_duration_seconds_sum 53423
http_request_duration_seconds_count 144320

# HELP http_requests_total Total number of HTTP requests
# TYPE http_requests_total counter
http_requests_total{code="200",method="post"} 1027 1395066363000
http_requests_total{code="400",method="post"} 3 1395066363000

metric_without_timestamp_and_labels 12.47

# HELP rpc_duration_seconds A summary of the RPC duration in seconds.
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile="0.01"} 3102
rpc_duration_seconds{quantile="0.05"} 3272
rpc_duration_seconds{quantile="0.5"} 4773
rpc_duration_seconds{quantile="0.9"} 9001
rpc_duration_seconds{quantile="0.99"} 76656
rpc_duration_seconds_sum 17560473
rpc_duration_seconds_count 2693

something_weird{problem="division by zero"} +Inf -3982045
```

<div id="format-settings">
  ## Настройки формата
</div>
