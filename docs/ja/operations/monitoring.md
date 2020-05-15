---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 45
toc_title: "\u76E3\u8996"
---

# 監視 {#monitoring}

監視できます:

-   ハードウェアリソースの利用。
-   クリックハウスサーバー指標。

## リソース使用率 {#resource-utilization}

ClickHouseは、ハードウェアリソースの状態を単独で監視しません。

それは強く推奨するセットアップ監視のための:

-   プロセッサの負荷と温度。

    を使用することができ [dmesg](https://en.wikipedia.org/wiki/Dmesg), [ターボスタット](https://www.linux.org/docs/man8/turbostat.html) または他の楽器。

-   ストレージシステムの利用,ramとネットワーク.

## Clickhouseサーバーメトリクス {#clickhouse-server-metrics}

ClickHouseサーバーは自己状態の監視のための器械を埋め込んだ。

追跡サーバのイベントサーバーを利用ます。 を見る [ロガー](server-configuration-parameters/settings.md#server_configuration_parameters-logger) 設定ファイルのセクション。

ClickHouseの収集:

-   異なるメトリクスのサーバがどのように利用計算資源です。
-   クエリ処理に関する一般的な統計。

メトリックは次の場所にあります [システム。指標](../operations/system-tables.md#system_tables-metrics), [システム。イベント](../operations/system-tables.md#system_tables-events)、と [システム。asynchronous\_metrics](../operations/system-tables.md#system_tables-asynchronous_metrics) テーブル。

を設定することができclickhouse輸出の指標に [黒鉛](https://github.com/graphite-project). を見る [グラファイト部](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) クリックハウスサーバー設定ファイルで。 を設定する前に輸出のメトリックトに設定する必要があります黒鉛は以下の公式 [ガイド](https://graphite.readthedocs.io/en/latest/install.html).

さらに、http apiを使用してサーバーの可用性を監視できます。 を送信 `HTTP GET` への要求 `/ping`. サーバーが使用可能な場合は、次のように応答します `200 OK`.

クラスター構成内のサーバーを監視するには、以下を設定します。 [max\_replica\_delay\_for\_distributed\_queries](settings/settings.md#settings-max_replica_delay_for_distributed_queries) HTTPリソースのパラメーターと使用 `/replicas_status`. を要求する `/replicas_status` を返します `200 OK` レプリカが使用可能で、他のレプリカの後ろに遅延されていない場合。 レプリカが遅延している場合は、 `503 HTTP_SERVICE_UNAVAILABLE` ギャップについての情報と。
