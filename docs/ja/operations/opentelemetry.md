---
slug: /ja/operations/opentelemetry
sidebar_position: 62
sidebar_label: OpenTelemetryでClickHouseをトレースする
title: "OpenTelemetryでClickHouseをトレースする"
---

[OpenTelemetry](https://opentelemetry.io/)は、分散アプリケーションからトレースやメトリクスを収集するためのオープン標準です。ClickHouseはOpenTelemetryをサポートしています。

## ClickHouseへのトレースコンテキストの提供

ClickHouseは、[W3Cの勧告](https://www.w3.org/TR/trace-context/)で説明されているトレースコンテキストHTTPヘッダーを受け入れます。また、ClickHouseサーバー間やクライアントとサーバー間で通信するために使用されるネイティブプロトコルを介してトレースコンテキストを受け入れます。手動テストのために、`clickhouse-client`に対して`--opentelemetry-traceparent`および`--opentelemetry-tracestate`フラグを使用してトレースコンテキストのヘッダーを指定することができます。

提供されたトレースコンテキストがW3C標準に準拠していないか、トレースコンテキストが提供されていない場合、ClickHouseは[opentelemetry_start_trace_probability](../operations/settings/settings.md#opentelemetry-start-trace-probability)設定で制御される確率で新しいトレースを開始できます。

## トレースコンテキストの伝播

トレースコンテキストは以下のケースで下流サービスに伝播されます：

* [分散テーブル](../engines/table-engines/special/distributed.md)エンジンを使用する際のリモートのClickHouseサーバーへのクエリ。

* [url](../sql-reference/table-functions/url.md)テーブル関数。トレースコンテキスト情報はHTTPヘッダーに送信されます。

## ClickHouse自体のトレース

ClickHouseは各クエリおよびクエリの実行ステージ（クエリプランニングや分散クエリなど）ごとに`トレーススパン`を作成します。

トレース情報を有用にするには、OpenTelemetryをサポートする監視システム（例えば、[Jaeger](https://jaegertracing.io/)や[Prometheus](https://prometheus.io/)）にエクスポートする必要があります。ClickHouseは特定の監視システムへの依存を避けるため、システムテーブルを介してトレースデータを提供するだけです。OpenTelemetryのトレーススパン情報は、[標準で要求される情報](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span)が[system.opentelemetry_span_log](../operations/system-tables/opentelemetry_span_log.md)テーブルに格納されます。

このテーブルはサーバー構成で有効にする必要があります。デフォルトの構成ファイル`config.xml`の`opentelemetry_span_log`要素を参照してください。デフォルトでは有効になっています。

タグや属性はキーと値を含む2つの並列配列として保存されます。それらを操作するには[ARRAY JOIN](../sql-reference/statements/select/array-join.md)を使用します。

## Log-query-settings

[log_query_settings](settings/settings.md)設定を使用すると、クエリ実行中にクエリ設定の変更をログに記録できます。有効にすると、クエリ設定に加えられた変更がOpenTelemetryスパンログに記録されます。この機能は、クエリパフォーマンスに影響を与える可能性のある設定変更を追跡するために、特に本番環境で便利です。

## 監視システムとの統合

現在、ClickHouseから監視システムへのトレースデータをエクスポートするための準備されたツールはありません。

テストのために、[system.opentelemetry_span_log](../operations/system-tables/opentelemetry_span_log.md)テーブルを使用して、[URL](../engines/table-engines/special/url.md)エンジンを使用したマテリアライズドビューをセットアップすることでエクスポートすることが可能です。これにより、到着したログデータをトレースコレクタのHTTPエンドポイントにプッシュします。例えば、`http://localhost:9411`で実行されているZipkinのインスタンスに、Zipkin v2 JSON形式で最小限のスパンデータをプッシュするには、次のようにします。

```sql
CREATE MATERIALIZED VIEW default.zipkin_spans
ENGINE = URL('http://127.0.0.1:9411/api/v2/spans', 'JSONEachRow')
SETTINGS output_format_json_named_tuples_as_objects = 1,
    output_format_json_array_of_rows = 1 AS
SELECT
    lower(hex(trace_id)) AS traceId,
    case when parent_span_id = 0 then '' else lower(hex(parent_span_id)) end AS parentId,
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

エラーが発生した場合、エラーが発生したログデータの一部は静かに失われます。データが届かない場合は、サーバーログでエラーメッセージを確認してください。

## 関連コンテンツ

- ブログ: [ClickHouseでの観測性ソリューションの構築 - パート2 - トレース](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)
