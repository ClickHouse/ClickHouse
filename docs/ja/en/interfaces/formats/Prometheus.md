---
alias: []
description: 'Prometheusフォーマットに関するドキュメント'
input_format: false
keywords: ['Prometheus']
output_format: true
slug: /interfaces/formats/Prometheus
title: 'Prometheus'
doc_type: 'reference'
---

| 入力 | 出力 | エイリアス |
| -- | -- | ----- |
| ✗  | ✔  |       |

<div id="description">
  ## 説明
</div>

[Prometheus のテキストベースのエクスポジション形式](https://prometheus.io/docs/instrumenting/exposition_formats/#text-based-format)でメトリクスを公開します。

このフォーマットでは、出力テーブルが次のルールに従って正しく構成されている必要があります。

* カラム `name` ([String](/ja/sql-reference/data-types/string.md)) と `value` (数値) は必須です。
* 行には、必要に応じて `help` ([String](/ja/sql-reference/data-types/string.md)) と `timestamp` (数値) を含めることができます。
* カラム `type` ([String](/ja/sql-reference/data-types/string.md)) は、`counter`、`gauge`、`histogram`、`summary`、`untyped` のいずれか、または空である必要があります。
* 各メトリクス値には、`labels` ([Map(String, String)](/ja/sql-reference/data-types/map.md)) を含めることもできます。
* 連続する複数の行が、異なるラベルを持つ同じメトリクスを参照することがあります。テーブルはメトリクス名でソートされている必要があります (例: `ORDER BY name`) 。

`histogram` と `summary` のラベルには特別な要件があります。詳細については、[Prometheus のドキュメント](https://prometheus.io/docs/instrumenting/exposition_formats/#histograms-and-summaries)を参照してください。
また、ラベル `{'count':''}` および `{'sum':''}` を持つ行には特別なルールが適用され、それぞれ `<metric_name>_count` と `<metric_name>_sum` に変換されます。

<div id="example-usage">
  ## 使用例
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

次のような形式になります:

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
  ## フォーマット設定
</div>
