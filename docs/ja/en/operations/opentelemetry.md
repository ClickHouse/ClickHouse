---
description: 'ClickHouse で OpenTelemetry を使用して分散トレーシングとメトリクス収集を行うためのガイド'
sidebar_label: 'OpenTelemetry による ClickHouse のトレーシング'
sidebar_position: 62
slug: /operations/opentelemetry
title: 'OpenTelemetry による ClickHouse のトレーシング'
doc_type: 'guide'
---

[OpenTelemetry](https://opentelemetry.io/) は、分散アプリケーションからトレースとメトリクスを収集するためのオープン標準です。ClickHouse は OpenTelemetry を一部サポートしています。

<div id="supplying-trace-context-to-clickhouse">
  ## ClickHouse にトレースコンテキストを渡す
</div>

ClickHouse は、[W3C 勧告](https://www.w3.org/TR/trace-context/)で規定されているトレースコンテキストの HTTP ヘッダーを受け付けます。また、ClickHouse サーバー間、またはクライアントとサーバー間の通信に使用されるネイティブプロトコルでもトレースコンテキストを受け付けます。手動テストでは、Trace Context 勧告に準拠したトレースコンテキストヘッダーを、`--opentelemetry-traceparent` および `--opentelemetry-tracestate` フラグを使って `clickhouse-client` に渡せます。

親トレースコンテキストが渡されていない場合、または渡されたトレースコンテキストが上記の W3C 標準に準拠していない場合、ClickHouse は新しいトレースを開始できます。その確率は、[opentelemetry&#95;start&#95;trace&#95;probability](/ja/operations/settings/settings#opentelemetry_start_trace_probability) 設定で制御されます。

<div id="propagating-the-trace-context">
  ## トレースコンテキストの伝播
</div>

トレースコンテキストは、次のような場合にダウンストリームサービスへ伝播されます。

* [Distributed](../engines/table-engines/special/distributed.md) テーブルエンジンを使用する場合など、リモートの ClickHouse サーバーへのクエリ。

* [url](../sql-reference/table-functions/url.md) テーブル関数。トレースコンテキストの情報は HTTP ヘッダーで送信されます。

<div id="tracing-clickhouse-keeper-requests">
  ## ClickHouse Keeper リクエストのトレーシング
</div>

ClickHouse は、[ClickHouse Keeper](../guides/sre/keeper/index.md) のリクエスト (ZooKeeper 互換の協調サービス) に対する OpenTelemetry トレーシングをサポートしています。この機能により、クライアントからのリクエスト送信からサーバー側での処理まで、Keeper 操作のライフサイクルを詳細に可視化できます。

<div id="enabling-keeper-tracing">
  ### Keeper トレーシングを有効にする
</div>

Keeper のリクエストに対するトレーシングを有効にするには、ZooKeeper/Keeper クライアントの設定で次の項目を構成します。

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
  ### Keeper スパンの種類
</div>

トレーシングが有効な場合、ClickHouse はクライアント側とサーバー側の両方の Keeper 操作に対してスパンを作成します。

**クライアント側のスパン:**

* `zookeeper.create` — 新しいノードを作成
* `zookeeper.get` — ノードデータを取得
* `zookeeper.set` — ノードデータを設定
* `zookeeper.remove` — ノードを削除
* `zookeeper.list` — 子ノードを一覧表示
* `zookeeper.exists` — ノードが存在するか確認
* `zookeeper.multi` — 複数の操作をアトミックに実行
* `zookeeper.client.requests_queue` — 送信前にリクエストがキューで待機した時間

**サーバー側のスパン (Keeper) :**

* `keeper.receive_request` — client からのリクエストの受信とパース
* `keeper.dispatcher.requests_queue` — dispatcher でのリクエストのキュー待ち
* `keeper.write.pre_commit` — Raft のコミット前の書き込みリクエストの前処理
* `keeper.write.commit` — Raft のコミット後の書き込みリクエストの処理
* `keeper.read.wait_for_write` — 依存する書き込みを待機している読み取りリクエスト
* `keeper.read.process` — 読み取りリクエストの処理
* `keeper.dispatcher.responses_queue` — dispatcher でのレスポンスのキュー待ち
* `keeper.send_response` — client へのレスポンスの送信

<div id="sampling-and-performance">
  ### サンプリングとパフォーマンス
</div>

トレーシングのオーバーヘッドを抑えるため、Keeper は動的サンプリングを実装しています。サンプリング率は、リクエストサイズに応じて 1/10,000 から 1/10 の範囲で自動的に調整されます。すべてのリクエスト (サンプリングされたものとされていないものの両方) について、パフォーマンス監視のために所要時間がヒストグラムメトリクスに記録されます。

<div id="tracing-the-clickhouse-itself">
  ## ClickHouse 自体のトレーシング
</div>

ClickHouse は、各クエリと、クエリプランの作成や分散クエリなど一部のクエリ実行段階について、`trace spans` を作成します。

これを有用なものにするには、トレーシング情報を [Jaeger](https://jaegertracing.io/) や [Prometheus](https://prometheus.io/) などの OpenTelemetry をサポートする監視システムにエクスポートする必要があります。ClickHouse は特定の監視システムへの依存を避けるため、代わりにシステムテーブル経由でのみトレーシングデータを提供します。標準で[必須とされている](https://github.com/open-telemetry/opentelemetry-specification/blob/master/specification/overview.md#span) OpenTelemetry のトレーススパン情報は、[system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md) テーブルに保存されます。

このテーブルはサーバー設定で有効にする必要があります。デフォルトの設定ファイル `config.xml` 内の `opentelemetry_span_log` 要素を参照してください。デフォルトで有効になっています。

タグまたは属性は、キーと値を含む 2 つの並列 Array として保存されます。これらを扱うには [ARRAY JOIN](../sql-reference/statements/select/array-join.md) を使用してください。

<div id="log-query-settings">
  ## クエリ設定のログ
</div>

設定 [log&#95;query&#95;settings](settings/settings.md) を使用すると、クエリ実行中にクエリ設定の変更を記録できます。有効にすると、クエリ設定に対するすべての変更が OpenTelemetry の スパン ログに記録されます。この機能は、クエリパフォーマンスに影響を与える可能性がある設定変更を追跡するうえで、特に本番環境で役立ちます。

<div id="integration-with-monitoring-systems">
  ## 監視システムとのインテグレーション
</div>

現時点では、ClickHouse から監視システムへトレースデータをエクスポートするための既製ツールはありません。

テスト用途では、[system.opentelemetry&#95;span&#95;log](../operations/system-tables/opentelemetry_span_log.md) テーブルに対して [URL](../engines/table-engines/special/url.md) エンジンを使用する materialized view を使うことでエクスポートを設定でき、到着したログデータを trace collector の HTTP エンドポイントへプッシュできます。たとえば、最小限の スパン データを `http://localhost:9411` で稼働している Zipkin インスタンスへ、Zipkin v2 JSON フォーマットでプッシュするには、次のようにします。

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

何らかのエラーが発生した場合、そのエラーが発生した部分のログデータは通知されることなく失われます。データが届かない場合は、エラーメッセージが出力されていないかサーバーログを確認してください。

<div id="related-content">
  ## 関連コンテンツ
</div>

* ブログ: [ClickHouseを使ったオブザーバビリティソリューションの構築 - 第2部 - トレース](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)