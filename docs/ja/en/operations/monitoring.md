---
description: 'ハードウェアリソースの利用状況と ClickHouse サーバーのメトリクスを監視できます。'
keywords: ['監視', 'オブザーバビリティ', '高度なダッシュボード', 'ダッシュボード', 'オブザーバビリティダッシュボード']
sidebar_label: '監視'
sidebar_position: 45
slug: /operations/monitoring
title: '監視'
doc_type: 'reference'
---

import Image from '@theme/IdealImage';

<div id="monitoring">
  # 監視
</div>

:::note
このガイドで説明する監視データは ClickHouse Cloud で利用できます。以下で説明する組み込みダッシュボードに加えて、基本的なものから高度なものまで、パフォーマンスメトリクスをサービスのメインコンソールで直接確認することもできます。
:::

以下を監視できます。

* ハードウェアリソースの使用状況。
* ClickHouse サーバー のメトリクス。

<div id="built-in-advanced-observability-dashboard">
  ## 組み込みの高度なオブザーバビリティダッシュボード
</div>

<Image img="https://github.com/ClickHouse/ClickHouse/assets/3936029/2bd10011-4a47-4b94-b836-d44557c7fdc1" alt="2023-11-12 午後6時08分58秒のスクリーンショット" size="md" />

ClickHouse には、`$HOST:$PORT/dashboard` からアクセスできる組み込みの高度なオブザーバビリティダッシュボード機能があります (ユーザー名とパスワードが必要です) 。このダッシュボードには、次のメトリクスが表示されます。

* クエリ/秒
* CPU 使用率 (コア)
* 実行中のクエリ
* 実行中のマージ
* 読み取りバイト/秒
* I/O 待機
* CPU 待機
* OS CPU 使用率 (ユーザー空間)
* OS CPU 使用率 (カーネル)
* ディスクからの読み取り
* ファイルシステムからの読み取り
* メモリ (追跡対象)
* 挿入行数/秒
* MergeTree パーツ総数
* パーティションごとの最大パーツ数

<div id="resource-utilization">
  ## リソース使用状況
</div>

ClickHouse は、次のようなハードウェアリソースの状態も自動的に監視します。

* プロセッサの負荷と温度。
* ストレージシステム、RAM、ネットワークの使用状況。

このデータは `system.asynchronous_metric_log` テーブルに収集されます。

<div id="clickhouse-server-metrics">
  ## ClickHouse サーバー のメトリクス
</div>

ClickHouse サーバー には、自身の状態を監視するための組み込みの計測機能があります。

サーバーイベントを追跡するには、サーバーログを使用します。設定ファイルの [logger](../operations/server-configuration-parameters/settings.md#logger) セクションを参照してください。

ClickHouse は以下を収集します。

* サーバーによる計算リソースの使用状況に関する各種メトリクス。
* クエリ処理に関する一般的な統計。

メトリクスは、[system.metrics](/ja/operations/system-tables/metrics)、[system.events](/ja/operations/system-tables/events)、および [system.asynchronous&#95;metrics](/ja/operations/system-tables/asynchronous_metrics) テーブルで確認できます。

ClickHouse はメトリクスを [Graphite](https://github.com/graphite-project) にエクスポートするよう設定できます。ClickHouse サーバー設定ファイルの [Graphite section](../operations/server-configuration-parameters/settings.md#graphite) を参照してください。メトリクスのエクスポートを設定する前に、公式の [guide](https://graphite.readthedocs.io/en/latest/install.html) に従って Graphite をセットアップしてください。

ClickHouse はメトリクスを [Prometheus](https://prometheus.io) にエクスポートするよう設定できます。ClickHouse サーバー設定ファイルの [Prometheus section](../operations/server-configuration-parameters/settings.md#prometheus) を参照してください。メトリクスのエクスポートを設定する前に、公式の [guide](https://prometheus.io/docs/prometheus/latest/installation/) に従って Prometheus をセットアップしてください。

さらに、HTTP API を通じてサーバーの可用性を監視できます。`HTTP GET` リクエストを `/ping` に送信してください。サーバーが利用可能であれば、`200 OK` を返します。

クラスター構成のサーバーを監視するには、[max&#95;replica&#95;delay&#95;for&#95;distributed&#95;queries](../operations/settings/settings.md#max_replica_delay_for_distributed_queries) パラメーターを設定し、HTTP リソース `/replicas_status` を使用する必要があります。`/replicas_status` へのリクエストは、レプリカが利用可能で、他のレプリカより遅延していない場合に `200 OK` を返します。レプリカが遅延している場合は、ギャップに関する情報とともに `503 HTTP_SERVICE_UNAVAILABLE` を返します。