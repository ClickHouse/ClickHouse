---
slug: /ja/integrations/prometheus
sidebar_label: Prometheus
title: Prometheus
description: ClickHouseのメトリクスをPrometheusにエクスポートします
keywords: [prometheus, grafana, 監視, メトリクス, エクスポーター] 
---

# Prometheus統合

この機能は、[Prometheus](https://prometheus.io/)を統合してClickHouse Cloudサービスを監視することをサポートします。Prometheusメトリクスへのアクセスは、[ClickHouse Cloud API](/ja/cloud/manage/api/api-overview) エンドポイントを介して公開され、ユーザーが安全に接続し、メトリクスをPrometheusメトリクスコレクターにエクスポートすることを可能にします。これらのメトリクスは、GrafanaやDatadogなどのダッシュボードに統合して可視化することができます。

開始するには、[APIキーを生成](/ja/cloud/manage/openapi)してください。

## PrometheusエンドポイントAPIでClickHouse Cloudメトリクスを取得する

### APIリファレンス

|Method|Path|
|---|---|
|GET|https://api.clickhouse.cloud/v1/organizations/:organizationId/services/:serviceId/prometheus|

**リクエストパラメータ**

|Name|Type|
|---|---|
|Organization ID|uuid|
|Service ID|uuid|

### 認証

基本認証にはClickHouse Cloud APIキーを使用します:

```bash
Username: <KEY_ID>
Password: <KEY_SECRET>

例:
export KEY_SECRET=<key_secret>
export KEY_ID=<key_id>
export ORG_ID=<org_id>
export SERVICE_ID=<service_id>
curl --silent --user $KEY_ID:$KEY_SECRET https://api.clickhouse.cloud/v1/organizations/$ORG_ID/services/$SERVICE_ID/prometheus 
```

### サンプルレスポンス

```
# HELP ClickHouse_ServiceInfo サービスに関する情報、クラスタステータスおよびClickHouseバージョンを含む
# TYPE ClickHouse_ServiceInfo untyped
ClickHouse_ServiceInfo{clickhouse_org="c2ba4799-a76e-456f-a71a-b021b1fafe60",clickhouse_service="12f4a114-9746-4a75-9ce5-161ec3a73c4c",clickhouse_service_name="test service",clickhouse_cluster_status="running",clickhouse_version="24.5",scrape="full"} 1

# HELP ClickHouseProfileEvents_Query 解釈されるクエリ数および潜在的に実行されるクエリ数。解析に失敗したクエリや、ASTサイズ制限、クォータ制限、または同時実行クエリ数の制限により拒否されたクエリは含まれません。ClickHouseによって内部的に開始されたクエリを含むことがあります。サブクエリはカウントしません。
# TYPE ClickHouseProfileEvents_Query counter
ClickHouseProfileEvents_Query{clickhouse_org="c2ba4799-a76e-456f-a71a-b021b1fafe60",clickhouse_service="12f4a114-9746-4a75-9ce5-161ec3a73c4c",clickhouse_service_name="test service",hostname="c-cream-ma-20-server-3vd2ehh-0",instance="c-cream-ma-20-server-3vd2ehh-0",table="system.events"} 6

# HELP ClickHouseProfileEvents_QueriesWithSubqueries すべてのサブクエリを含むクエリの数
# TYPE ClickHouseProfileEvents_QueriesWithSubqueries counter
ClickHouseProfileEvents_QueriesWithSubqueries{clickhouse_org="c2ba4799-a76e-456f-a71a-b021b1fafe60",clickhouse_service="12f4a114-9746-4a75-9ce5-161ec3a73c4c",clickhouse_service_name="test service",hostname="c-cream-ma-20-server-3vd2ehh-0",instance="c-cream-ma-20-server-3vd2ehh-0",table="system.events"} 230

# HELP ClickHouseProfileEvents_SelectQueriesWithSubqueries すべてのサブクエリを含むSELECTクエリの数
# TYPE ClickHouseProfileEvents_SelectQueriesWithSubqueries counter
ClickHouseProfileEvents_SelectQueriesWithSubqueries{clickhouse_org="c2ba4799-a76e-456f-a71a-b021b1fafe60",clickhouse_service="12f4a114-9746-4a75-9ce5-161ec3a73c4c",clickhouse_service_name="test service",hostname="c-cream-ma-20-server-3vd2ehh-0",instance="c-cream-ma-20-server-3vd2ehh-0",table="system.events"} 224

# HELP ClickHouseProfileEvents_FileOpen 開かれたファイルの数。
# TYPE ClickHouseProfileEvents_FileOpen counter
ClickHouseProfileEvents_FileOpen{clickhouse_org="c2ba4799-a76e-456f-a71a-b021b1fafe60",clickhouse_service="12f4a114-9746-4a75-9ce5-161ec3a73c4c",clickhouse_service_name="test service",hostname="c-cream-ma-20-server-3vd2ehh-0",instance="c-cream-ma-20-server-3vd2ehh-0",table="system.events"} 4157

# HELP ClickHouseProfileEvents_Seek 'lseek' 関数が呼び出された回数。
# TYPE ClickHouseProfileEvents_Seek counter
ClickHouseProfileEvents_Seek{clickhouse_org="c2ba4799-a76e-456f-a71a-b021b1fafe60",clickhouse_service="12f4a114-9746-4a75-9ce5-161ec3a73c4c",clickhouse_service_name="test service",hostname="c-cream-ma-20-server-3vd2ehh-0",instance="c-cream-ma-20-server-3vd2ehh-0",table="system.events"} 1840
```

### メトリクスラベル

すべてのメトリクスには以下のラベルがあります：

|Label|Description|
|---|---|
|clickhouse_org|組織ID|
|clickhouse_service|サービスID|
|clickhouse_service_name|サービス名|

### 情報メトリクス

ClickHouse Cloudは特別なメトリクス`ClickHouse_ServiceInfo`を提供しています。これは常に値`1`を持つ`gauge`です。このメトリクスにはすべての**メトリクスラベル**と以下のラベルが含まれます：

|Label|Description|
|---|---|
|clickhouse_cluster_status|サービスのステータス。以下のいずれかになります：[`awaking` \| `running` \| `degraded` \| `idle` \| `stopped`]|
|clickhouse_version|サービスが実行しているClickHouseサーバーのバージョン|
|scrape|最後のスクレイプのステータスを示します。`full`または`partial`のいずれかになります|
|full|最後のメトリクススクレイプ中にエラーがなかったことを示します|
|partial|最後のメトリクススクレイプ中にいくつかのエラーがあり、`ClickHouse_ServiceInfo`メトリクスのみが返されたことを示します|

メトリクスを取得するリクエストは、アイドル状態のサービスを再開させません。サービスが`idle`状態の場合、`ClickHouse_ServiceInfo`メトリクスのみが返されます。

### Prometheusの設定

Prometheusサーバーは、指定された間隔で設定されたターゲットからメトリクスを収集します。以下は、ClickHouse Cloud Prometheusエンドポイントを使用するためのPrometheusサーバーの設定例です：

```yaml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: "prometheus"
    static_configs:
    - targets: ["localhost:9090"]
  - job_name: "clickhouse"
    static_configs:
      - targets: ["api.clickhouse.cloud"]
    scheme: https
    metrics_path: "/v1/organizations/<ORG_ID>/services/<SERVICE_ID>/prometheus"
    basic_auth:
      username: <KEY_ID>
      password: <KEY_SECRET>
    honor_labels: true
```

`honor_labels`の設定パラメーターは`true`に設定する必要があり、インスタンスラベルが適切に設定されます。

## Grafanaとの統合

ユーザーは、Grafanaと統合するための主な方法が2つあります：

- **Metrics Endpoint** – この方法は、追加のコンポーネントやインフラを必要としない利点があります。この提供はGrafana Cloudに限定されており、ClickHouse Cloud PrometheusエンドポイントURLと認証情報のみが必要です。
- **Grafana Alloy** - Grafana Alloyは、Grafana Agentに代わるOpenTelemetry (OTel) Collectorのベンダー中立なディストリビューションです。これはスクレーパーとして使用可能で、独自のインフラストラクチャにデプロイ可能で、どのPrometheusエンドポイントとも互換性があります。

これらのオプションを使用する方法についての指示を以下に示します。ClickHouse Cloud Prometheusエンドポイントに特化した詳細に焦点を当てています。

### Grafana CloudとMetrics Endpoint

- Grafana Cloudアカウントにログイン
- **Metrics Endpoint**を選択して新しい接続を追加
- Prometheusエンドポイントを指すようにScrape URLを設定し、基本認証を使用してAPIキー/シークレットで接続を設定
- 接続ができることを確認するためにテスト

<img src={require('./images/prometheus-grafana-metrics-endpoint.png').default}    
  class='image'
  alt='Grafana Metrics Endpointを設定'
  style={{width: '600px'}} />

<br />

設定後、ダッシュボードを設定するメトリクスを選択するためのドロップダウンが表示されるはずです：

<img src={require('./images/prometheus-grafana-dropdown.png').default}    
  class='image'
  alt='Grafana Metrics Explorerドロップダウン'
  style={{width: '400px'}} />

<br />

<img src={require('./images/prometheus-grafana-chart.png').default}    
  class='image'
  alt='Grafana Metrics Explorerチャート'
  style={{width: '800px'}} />

### Grafana CloudとAlloy

Grafana Cloudを使用している場合、AlloyをGrafanaのAlloyメニューに移動して画面の指示に従うことでインストールできます：

<img src={require('./images/prometheus-grafana-alloy.png').default}    
  class='image'
  alt='Grafana Alloy'
  style={{width: '600px'}} />

<br />

これにより、認証トークンを用いてGrafana Cloudエンドポイントにデータを送信するための`prometheus.remote_write`コンポーネントを持つようにAlloyが設定されます。ユーザーはAlloyの設定（Linuxでは`/etc/alloy/config.alloy`にあります）を変更して、ClickHouse Cloud Prometheusエンドポイント用のスクレーパーを含める必要があります。

以下は、ClickHouse Cloudエンドポイントからメトリクスをスクレイプするための`prometheus.scrape`コンポーネントを含むAlloyの設定例です。また自動的に設定された`prometheus.remote_write`コンポーネントも含まれています。`basic_auth`設定コンポーネントには私たちのCloud APIキーIDおよびシークレットがそれぞれユーザー名とパスワードとして含まれていることに注意してください。

```yaml
prometheus.scrape "clickhouse_cloud" {
  // 通常のリッスンアドレスからメトリクスを収集します。
  targets = [{
	__address__ = "https://api.clickhouse.cloud/v1/organizations/:organizationId/services/:serviceId/prometheus",
// 例: https://api.clickhouse.cloud/v1/organizations/97a33bdb-4db3-4067-b14f-ce40f621aae1/services/f7fefb6e-41a5-48fa-9f5f-deaaa442d5d8/prometheus
  }]

  honor_labels = true

  basic_auth {
  	username = "KEY_ID"
  	password = "KEY_SECRET"
  }

  forward_to = [prometheus.remote_write.metrics_service.receiver]
  // follows to metrics_service below
}

prometheus.remote_write "metrics_service" {
  endpoint {
	url = "https://prometheus-prod-10-prod-us-central-0.grafana.net/api/prom/push"
	basic_auth {
  	  username = "<Grafana API username>"
  	  password = "<grafana API token>"
    }
  }
}
```

`honor_labels`の設定パラメーターは`true`に設定する必要があり、インスタンスラベルが適切に設定されます。

### セルフマネージドのGrafanaとAlloy

セルフマネージドのGrafanaユーザーは、[こちら](https://grafana.com/docs/alloy/latest/get-started/install/)でAlloyエージェントをインストールするための手順を見つけることができます。ユーザーがPrometheusメトリクスを送りたい目的の場所にAlloyを設定していることを前提としています。以下の`prometheus.scrape`コンポーネントは、AlloyがClickHouse Cloudエンドポイントをスクレイプする原因となります。`prometheus.remote_write`がスクレイプされたメトリクスを受け取ると想定しています。この設定が存在しない場合は、フォワード先を希望のデスティネーションに調整してください。

```yaml
prometheus.scrape "clickhouse_cloud" {
  // 通常のリッスンアドレスからメトリクスを収集します。
  targets = [{
	__address__ = "https://api.clickhouse.cloud/v1/organizations/:organizationId/services/:serviceId/prometheus",
// 例: https://api.clickhouse.cloud/v1/organizations/97a33bdb-4db3-4067-b14f-ce40f621aae1/services/f7fefb6e-41a5-48fa-9f5f-deaaa442d5d8/prometheus
  }]

  honor_labels = true

  basic_auth {
  	username = "KEY_ID"
  	password = "KEY_SECRET"
  }

  forward_to = [prometheus.remote_write.metrics_service.receiver]
  // preferred receiverに転送。修正してください。
}
```

設定すると、メトリクスエクスプローラーにClickHouse関連のメトリクスが表示されるはずです：

<img src={require('./images/prometheus-grafana-metrics-explorer.png').default}    
  class='image'
  alt='Grafana Metrics Explorer'
  style={{width: '800px'}} />

<br />

`honor_labels`の設定パラメーターは`true`に設定する必要があり、インスタンスラベルが適切に設定されます。

## Datadogとの統合

Datadog [Agent](https://docs.datadoghq.com/agent/?tab=Linux)および[OpenMetrics統合](https://docs.datadoghq.com/integrations/openmetrics/)を使用して、ClickHouse Cloudエンドポイントからメトリクスを収集することができます。以下は、このエージェントと統合のための簡単な設定例です。ただし、最も関心があるメトリクスのみを選択したいかもしれませんことに注意してください。以下のキャッチオールの例は、多くのメトリクスインスタンスの組み合わせをエクスポートし、Datadogはそれをカスタムメトリクスとして扱います。

```yaml
init_config:

instances:
   - openmetrics_endpoint: 'https://api.clickhouse.cloud/v1/organizations/97a33bdb-4db3-4067-b14f-ce40f621aae1/services/f7fefb6e-41a5-48fa-9f5f-deaaa442d5d8/prometheus'
     namespace: 'clickhouse'
     metrics:
         - '^ClickHouse.*'
     username: username
     password: password
```

<br />

<img src={require('./images/prometheus-datadog.png').default}    
  class='image'
  alt='Prometheus Datadog Integration'
  style={{width: '600px'}} />
