---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u76E3\u8996"
---

# 監視 {#monitoring}

監視できます:

-   ハードウェアリソースの利用。
-   ClickHouseサーバー指標。

## リソース使用率 {#resource-utilization}

ClickHouseは、ハードウェアリソースの状態を単独で監視しません。

監視をセットアップすることを強く推奨します:

-   プロセッサの負荷と温度。

    以下を使用できます [dmesg](https://en.wikipedia.org/wiki/Dmesg), [ターボスタット](https://www.linux.org/docs/man8/turbostat.html) または他の楽器。

-   ストレージシステム、RAM、ネットワークの利用。

## ClickHouseサーバー指標 {#clickhouse-server-metrics}

ClickHouse serverには、自己状態の監視のための計測器が組み込まれています。

追跡サーバのイベントサーバーを利用ます。 を参照。 [ロガー](server-configuration-parameters/settings.md#server_configuration_parameters-logger) 設定ファイルのセクション。

クリックハウス収集:

-   異なるメトリクスのサーバがどのように利用計算資源です。
-   クエリ処理に関する一般的な統計。

メトリックは、次のとおりです。 [システムメトリック](../operations/system-tables.md#system_tables-metrics), [システムイベント](../operations/system-tables.md#system_tables-events),and [システムasynchronous_metrics](../operations/system-tables.md#system_tables-asynchronous_metrics) テーブル

を設定することができClickHouse輸出の指標に [黒鉛](https://github.com/graphite-project). を参照。 [グラファイト部](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) ClickHouseサーバー設定ファイル内。 指標のエクスポートを設定する前に、公式に従ってGraphiteを設定する必要があります [ガイド](https://graphite.readthedocs.io/en/latest/install.html).

を設定することができClickHouse輸出の指標に [プロメテウス](https://prometheus.io). を参照。 [プロメテウス節](server-configuration-parameters/settings.md#server_configuration_parameters-prometheus) ClickHouseサーバー設定ファイル内。 メトリックのエクスポートを設定する前に、公式に従ってPrometheusを設定してください [ガイド](https://prometheus.io/docs/prometheus/latest/installation/).

さらに、HTTP APIを使用してサーバーの可用性を監視できます。 送信 `HTTP GET` リクエスト先 `/ping`. サーバーが利用可能な場合は、次のように応答します `200 OK`.

監視サーバーにクラスター構成設定してください [max_replica_delay_for_distributed_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries) パラメータとHTTPリソースの使用 `/replicas_status`. への要求 `/replicas_status` ﾂづｩﾂ。 `200 OK` レプリカが使用可能で、他のレプリカより遅れていない場合。 レプリカが遅延すると、次のようになります `503 HTTP_SERVICE_UNAVAILABLE` ギャップについての情報と。
