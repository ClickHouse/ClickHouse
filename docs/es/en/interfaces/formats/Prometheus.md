---
alias: []
description: 'Documentación sobre el formato Prometheus'
input_format: false
keywords: ['Prometheus']
output_format: true
slug: /interfaces/formats/Prometheus
title: 'Prometheus'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✗       | ✔      |       |

<div id="description">
  ## Descripción
</div>

Expone métricas en el [formato de exposición de texto de Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format).

En este formato, es obligatorio que la tabla de salida esté estructurada correctamente, de acuerdo con las siguientes reglas:

* Las columnas `name` ([String](/es/sql-reference/data-types/string.md)) y `value` (número) son obligatorias.
* Las filas pueden incluir opcionalmente `help` ([String](/es/sql-reference/data-types/string.md)) y `timestamp` (número).
* La columna `type` ([String](/es/sql-reference/data-types/string.md)) debe ser uno de estos valores: `counter`, `gauge`, `histogram`, `summary`, `untyped`, o estar vacía.
* Cada valor de métrica también puede tener algunas `labels` ([Map(String, String)](/es/sql-reference/data-types/map.md)).
* Varias filas consecutivas pueden hacer referencia a la misma métrica con distintas `labels`. La tabla debe estar ordenada por nombre de métrica (por ejemplo, con `ORDER BY name`).

Hay requisitos especiales para las `labels` `histogram` y `summary`; consulta la [documentación de Prometheus](https://prometheus.io/docs/instrumenting/exposition_formats/#histograms-and-summaries) para obtener más información.
Se aplican reglas especiales a las filas con las `labels` `{'count':''}` y `{'sum':''}`, que se convierten en `<metric_name>_count` y `<metric_name>_sum`, respectivamente.

<div id="example-usage">
  ## Ejemplo de uso
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

Tendrá el siguiente formato:

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
  ## Configuración del formato
</div>
