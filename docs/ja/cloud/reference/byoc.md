---
title: AWS用BYOC (Bring Your Own Cloud) - プライベートプレビュー
slug: /ja/cloud/reference/byoc
sidebar_label: BYOC (Bring Your Own Cloud)
keywords: [byoc, cloud, bring your own cloud]
description: 独自のクラウドインフラにClickHouseをデプロイ
---

## 概要

BYOC (Bring Your Own Cloud) を使用すると、自身のクラウドインフラにClickHouse Cloudをデプロイできます。これは、ClickHouse Cloudの管理サービスを使用することができない具体的な要件や制約がある場合に役立ちます。

**BYOCは現在プライベートプレビュー中です。アクセスを希望される場合は、[サポート](https://clickhouse.com/support/program)までお問い合わせください。** プライベートプレビューに関する追加情報は、[利用規約](https://clickhouse.com/legal/agreements/terms-of-service) を参照してください。

BYOCは現在、AWSのみサポートされており、GCPおよびMicrosoft Azureは開発中です。

:::note 
BYOCは大規模なデプロイメント専用に設計されています。
:::

## 用語集

- **ClickHouse VPC:**  ClickHouse Cloudが所有するVPC。 
- **Customer BYOC VPC:** ClickHouse Cloudによってプロビジョニングおよび管理され、ClickHouse Cloud BYOCのデプロイメントに専念する顧客クラウドアカウントのVPC。
- **Customer VPC** 顧客クラウドアカウントが所有する他のVPCで、Customer BYOC VPCに接続する必要があるアプリケーション用に使用されます。

## アーキテクチャ

メトリクスとログは顧客のBYOC VPC内に保存されます。ログは現在、ローカルのEBSに保存されています。次のアップデートで、ログはLogHouse（顧客のBYOC VPCにあるClickHouseサービス）に保存されるようになります。メトリクスはPrometheusとThanosスタックを介して顧客のBYOC VPC内にローカルに保存されます。

<br />

<img src={require('./images/byoc-1.png').default}
    alt='BYOC Architecture'
    class='image'
    style={{width: '800px'}}
/>

<br />

## オンボーディングプロセス

プライベートプレビュー中は、ClickHouse [サポート](https://clickhouse.com/support/program) に連絡することでオンボーディングプロセスを開始します。顧客は専用のAWSアカウントを持っている必要があり、使用するリージョンを知っている必要があります。現時点では、ClickHouse Cloudをサポートしている地域でのみBYOCサービスの起動を許可しています。

BYOCのセットアップはCloudFormationスタックを通じて管理されます。このCloudFormationスタックは、ClickHouse CloudのBYOCコントローラーがインフラを設定および管理するための役割を作成するのみです。ClickHouseを実行するために使用されるS3、VPC、およびコンピュートリソースはCloudFormationスタックの一部ではありません。

## アップグレードプロセス

定期的にソフトウェアをアップグレードします。これには、ClickHouseデータベースのバージョンアップグレード、ClickHouse Operator、EKS、およびその他のコンポーネントが含まれます。

アップグレードをできるだけシームレスにするよう努めています（例: ローリングアップグレードと再起動）が、ClickHouseバージョンの変更やEKSノードのアップグレードなど、一部のアップグレードはサービスに影響を与える可能性があります。その場合、顧客はメンテナンスウィンドウ（例: 毎週火曜日の午前1時PDT）を指定できます。そのようなアップグレードは、スケジュールされたメンテナンスウィンドウ中にのみ実行されることを保証します。

メンテナンスウィンドウは、セキュリティおよび脆弱性修正には適用されないことに注意してください。これらはオフサイクルアップグレードとして処理され、顧客と迅速にコミュニケーションを取り、必要なアクションを実行し、操作への影響を最小限に抑えるための適切な時間を調整します。

## CloudFormation IAM Roles

### Bootstrap IAMロール

bootstrap IAMロールにはこれらの権限があります：

- VPCとEKSクラスターのセットアップに必要なEC2およびVPC操作。
- バケットを作成するために必要な`s3:CreateBucket`などのS3操作が必要です。
- 外部DNSにおけるroute53のレコード設定のために`route53:*`が必要です。
- コントローラーが追加の役割を作成するために`iam:CreatePolicy`などのIAM関連の操作が必要です。詳細は次のセクションを参照してください。
- `ClickHouse-cloud`プリフィックスで始まるリソースに限定されたeks:xx操作。

### コントローラーによって作成される追加のIAMロール

CloudFormationを通じて作成された`ClickHouseManagementRole`に加えて、コントローラーは他にいくつかの役割を作成します。

これらの役割は、顧客のEKSクラスターで実行されるアプリケーションによって引き受けられることを意図しています。
- **State exporter role**
    - ClickHouseコンポーネントがサービスの健康情報をClickHouse Cloudに報告するもの。
    - ClickHouse Cloudが所有するSQSに書き込み権限が必要
- **Load-balancer-controller**
    - 標準的なAWSロードバランサーコントローラー
    - ClickHouseサービスに必要なボリュームを管理するためのEBS CSIコントローラー
- **External-dns**、route53にDNS設定を伝搬するため
- **Cert-manager**、BYOCサービスドメイン用にTLS証明書をプロビジョニングするため
- **Cluster autoscaler**、ノードグループを適宜スケールするため

**K8s-control-plane**と**k8s-worker**ロールはAWS EKSサービスによって引き受けられることを意図しています。

最後に、**data-plane-mgmt**はカスタムリソースの`ClickHouseCluster`やIstio Virtual Service/Gatewayなどを調整するためのClickHouseクラウドコントロールプレーンコンポーネントに許可するためのものです。

## ネットワーク境界

このセクションは、顧客のBYOC VPCへのおよびからのさまざまなネットワークトラフィックに焦点を当てています。

- **インバウンド**: 顧客のBYOC VPCに送られるトラフィック。
- **アウトバウンド**: 顧客のBYOC VPCから発信され、そのVPC外の宛先に送信されるトラフィック
- **パブリック**: 公共のインターネットに公開されているネットワークエンドポイントアドレス
- **プライベート**: VPCピアリング、VPCプライベートリンク、Tailscaleなどを通じてのみアクセス可能なプライベートなネットワークエンドポイントアドレス

**Istio ingressはAWS NLBの背後にデプロイされ、ClickHouseクライアントトラフィックを受け入れます**

*インバウンド、パブリック(プライベートにすることができます)*

Istio ingress gatewayはTLSを終了します。証明書はLet's EncryptでCertManagerによってプロビジョニングされ、EKSクラスター内にシークレットとして保存されます。IstioとClickHouse間のトラフィックは[AWSによって暗号化](https://docs.aws.amazon.com/whitepapers/latest/logical-separation/encrypting-data-at-rest-and--in-transit.html#:~:text=All%20network%20traffic%20between%20AWS,supported%20Amazon%20EC2%20instance%20types)されます。デフォルトでは、インテ入はIP許可リストフィルタリングでパブリックインターネットに利用可能です。顧客はVPCピアリングを設定してプライベートにしてパブリック接続を無効にするオプションがあります。アクセスを制限するに[IPフィルター](/ja/cloud/security/setting-ip-filters)を設定することを強くお勧めします。

**トラブルシューティングアクセス**

ClickHouseクラウドエンジニアはトラブルシューティングアクセスにTailscaleを必要とします。BYOCデプロイメントへの証明書ベースの認証を求め、タイムリーに提供されます。

*インバウンド、パブリック(プライベートにすることができます)*

**課金スクレイパー**

*アウトバウンド、プライベート*

課金スクレイパーは、ClickHouseから課金データを収集し、ClickHouseクラウドが所有するS3バケットに送信します。スクレイパーは、ClickHouseサーバーコンテナの横でサイドカートして働くコンポーネントです。定期的にClickHouseのCPUとメモリのメトリクスをスクレイプします。同じリージョンへのリクエストはVPCゲートウェイサービスのエンドポイントを介して行われます。

**アラート**

*アウトバウンド、パブリック*

AlertManagerは、顧客のClickHouseクラスターが健康ではないときにアラートをClickHouseクラウドに送信するように構成されています。メトリクスとログは顧客のBYOC VPC内に保存されます。ログは現在、ローカルのEBSに保存されています。次のアップデートで、ログはLogHouse（顧客のBYOC VPCにあるClickHouseサービス）に保存されるようになります。メトリクスはPrometheusとThanosスタックを介して顧客のBYOC VPC内にローカルに保存されます。

**サービス状態**

*アウトバウンド*

State ExporterはClickHouseのサービス状態情報をClickHouseクラウド所有のSQSに送信します。

## 機能

### サポートされている機能

- SharedMergeTree: ClickHouse CloudとBYOCは同じバイナリおよび設定を使用
- サービス状態を管理するためのコンソールアクセス
    - サポートされる操作には開始、停止、終了が含まれます
    - サービスと状態を表示
- バックアップとリストア
- 手動による垂直および水平スケーリング
- Falcoによるランタイムセキュリティ監視とアラート（falco-metrics）
- Tailscaleによるゼロトラストネットワーク
- 監視: クラウドコンソールにはサービスの健康を監視するための組み込みのダッシュボードがあります
- 中央ダッシュボードを使用したユーザーによる監視を選択するためのPrometheusのスクレイピング。今日、Prometheus、Grafana、Datadogをサポートしています。セットアップの詳細な手順については、[Prometheusドキュメント](/ja/integrations/prometheus) を参照してください
- VPCピアリング
- [このページ](/ja/integrations) にリストされている統合
- セキュアなS3

### 計画された機能（現在サポートされていません）

- [AWS PrivateLink](https://aws.amazon.com/privatelink/)
- [AWS KMS](https://aws.amazon.com/kms/) 別名CMEK（顧客管理型暗号化キー）
- インジェスト用のClickPipes
- 自動スケーリング
- イドル化
- MySQLインターフェース

## FAQ

### コンピュート

**この単一のEKSクラスターに複数のサービスを作成できますか？**

はい。インフラストラクチャは、AWSアカウントとリージョンの組み合わせごとに一度プロビジョニングするだけです。

**BYOCをサポートしている地域はどこですか？**

BYOCはClickHouse Cloudと同じ[リージョン](/ja/cloud/reference/supported-regions#aws-regions )セットをサポートしています。

**リソースオーバーヘッドはありますか？ClickHouseインスタンス以外のサービスを実行するために必要なリソースは何ですか？**

Clickhouseインスタンス（ClickHouseサーバーおよびClickHouse Keeper）の他に、clickhouse-operator、aws-cluster-autoscaler、Istioなどなどのサービスや監視スタックを実行しています。

現在、それらのワークロードを実行するために専用のノードグループに3つのm5.xlargeノード（各AZに1つずつ）があります。

### ネットワークとセキュリティ

**セットアップが完了した後にセットアップ中に設定された権限を取り消すことができますか？**

現時点では不可能です。

**ClickHouseエンジニアがトラブルシューティングのために顧客のインフラにアクセスする際の将来のセキュリティコントロールを検討していますか？**

はい。顧客がエンジニアのクラスターへのアクセスを承認できるようにする顧客制御のメカニズムの実装は、我々のロードマップに含まれています。現時点では、エンジニアは、クラスターへのタイムリーなアクセスを取得するために、内部のエスカレーションプロセスを経る必要があります。これは記録され、当社のセキュリティチームによって監査されます。

**VPCピアリングをどのようにセットアップしますか？**

VPCピアリングの作成と削除は、サポートエスカレーションを通じて行うことができます。前提条件として、ピアリングされたVPC間でCIDR範囲が重複していない必要があります。

ClickHouseサポートによってVPCピアリングの構成が完了したら、ユーザーが完了する必要があるいくつかの操作があります。

1. ピアリングされたVPCのAWSアカウントでVPCピアリングリクエストを受け取り、受け入れる必要があります。**VPC -> Peering connections -> Actions -> Accept request**に移動してください。


2. ピアリングされたVPCのルートテーブルを調整します。ClickHouseインスタンスに接続する必要があるピアリングされたVPCのサブネットを見つけます。このサブネットのルートテーブルを編集し、次の構成で1つのルートを追加します：
- 送信先: ClickHouse BYOC VPC CIDR (例: 10.0.0.0/16)
- ターゲット: ピアリング接続、pcx-12345678（ドロップダウンリストで実際のIDが表示される）

<br />

<img src={require('./images/byoc-2.png').default}
    alt='BYOC network configuration'
    class='image'
    style={{width: '600px'}}
/>

<br />

3. 既存のセキュリティグループを確認し、BYOC VPCへのアクセスをブロックしないルールがあることを確認してください。

ピアリングされたVPCからClickHouseサービスにアクセスできるようになります。

ClickHouseサービスにプライベートにアクセスするには、プライベートロードバランサーとエンドポイントがプロビジョニングされ、ユーザーのピアVPCからプライベートに接続できます。エンドポイントは`-private`のサフィックスで、パブリックエンドポイントと同様です。例：
もし公開エンドポイントが `h5ju65kv87.mhp0y4dmph.us-west-2.aws.byoc.clickhouse.cloud`であるなら、プライベートエンドポイントは`h5ju65kv87-private.mhp0y4dmph.us-west-2.aws.byoc.clickhouse.cloud`になります。

**EKSクラスター用に作成されるVPC IP範囲を選択できますか？**

VPC CIDR範囲を選択できます。これはVPCピアリング機能に影響を与えるためです。オンボーディング時にサポートチケットに記載してください。

**作成されるVPC IP範囲のサイズはどれくらいですか？**

将来の拡張を考慮して少なくとも/22を予約することをお勧めしますが、縮小を希望する場合は/23を使用して、30のサーバーポッドに制限される可能性がある場合は可能です。

**メンテナンスの頻度を決めることができますか？**

サポートに連絡してメンテナンスウィンドウをスケジュールしてください。少なくとも隔週のアップデートスケジュールの予定です。

## 観測可能性

### 組み込みの監視ツール

#### 観測可能性のダッシュボード

ClickHouse Cloudには、メモリ使用率、クエリレート、I/Oなどのメトリクスを表示する高度な観測可能性のダッシュボードが含まれています。これはClickHouse Cloudのウェブコンソールインターフェースの**監視**セクションでアクセスできます。

<br />

<img src={require('./images/byoc-3.png').default}
    alt='Observability dashboard'
    class='image'
    style={{width: '800px'}}
/>

<br />

#### 高度なダッシュボード

`system.metrics`、`system.events`、`system.asynchronous_metrics`などのシステムテーブルからのメトリクスを使用して、サーバーのパフォーマンスとリソース使用状況を詳細に監視するためのダッシュボードをカスタマイズすることができます。

<br />

<img src={require('./images/byoc-4.png').default}
    alt='Advanced dashboard'
    class='image'
    style={{width: '800px'}}
/>

<br />

#### Prometheus統合

ClickHouse Cloudは監視のためにメトリクスをスクレイプするために使用できるPrometheusエンドポイントを提供しています。これにより、GrafanaやDatadogなどのツールとの統合が可能です。

**/metrics_allのhttpsエンドポイントを介したサンプルリクエスト**
    
```bash
curl --user <username>:<password> https://i6ro4qarho.mhp0y4dmph.us-west-2.aws.byoc.clickhouse.cloud:8443/metrics_all
```

**サンプルレスポンス**

```bash
# HELP ClickHouse_CustomMetric_StorageSystemTablesS3DiskBytes The amount of bytes stored on disk `s3disk` in system database
# TYPE ClickHouse_CustomMetric_StorageSystemTablesS3DiskBytes gauge
ClickHouse_CustomMetric_StorageSystemTablesS3DiskBytes{hostname="c-jet-ax-16-server-43d5baj-0"} 62660929
# HELP ClickHouse_CustomMetric_NumberOfBrokenDetachedParts The number of broken detached parts
# TYPE ClickHouse_CustomMetric_NumberOfBrokenDetachedParts gauge
ClickHouse_CustomMetric_NumberOfBrokenDetachedParts{hostname="c-jet-ax-16-server-43d5baj-0"} 0
# HELP ClickHouse_CustomMetric_LostPartCount The age of the oldest mutation (in seconds)
# TYPE ClickHouse_CustomMetric_LostPartCount gauge
ClickHouse_CustomMetric_LostPartCount{hostname="c-jet-ax-16-server-43d5baj-0"} 0
# HELP ClickHouse_CustomMetric_NumberOfWarnings The number of warnings issued by the server. It usually indicates about possible misconfiguration
# TYPE ClickHouse_CustomMetric_NumberOfWarnings gauge
ClickHouse_CustomMetric_NumberOfWarnings{hostname="c-jet-ax-16-server-43d5baj-0"} 2
# HELP ClickHouseErrorMetric_FILE_DOESNT_EXIST FILE_DOESNT_EXIST
# TYPE ClickHouseErrorMetric_FILE_DOESNT_EXIST counter
ClickHouseErrorMetric_FILE_DOESNT_EXIST{hostname="c-jet-ax-16-server-43d5baj-0",table="system.errors"} 1
# HELP ClickHouseErrorMetric_UNKNOWN_ACCESS_TYPE UNKNOWN_ACCESS_TYPE
# TYPE ClickHouseErrorMetric_UNKNOWN_ACCESS_TYPE counter
ClickHouseErrorMetric_UNKNOWN_ACCESS_TYPE{hostname="c-jet-ax-16-server-43d5baj-0",table="system.errors"} 8
# HELP ClickHouse_CustomMetric_TotalNumberOfErrors The total number of errors on server since the last restart
# TYPE ClickHouse_CustomMetric_TotalNumberOfErrors gauge
ClickHouse_CustomMetric_TotalNumberOfErrors{hostname="c-jet-ax-16-server-43d5baj-0"} 9
```

**認証**

ClickHouseのユーザー名とパスワードのペアを使用して認証を行います。メトリクスをスクレイプするために最小限の権限で専用のユーザーを作成することをお勧めします。少なくとも、レプリカ間で`system.custom_metrics`テーブルへの`READ`権限が必要です。例：

```sql
GRANT REMOTE ON *.* TO scraping_user          
GRANT SELECT ON system.custom_metrics TO scraping_user
```

**Prometheusの設定**

以下は設定例です。`targets`エンドポイントはClickHouseサービスにアクセスするために使用されるものと同じです。

```bash
global:
 scrape_interval: 15s

scrape_configs:
 - job_name: "prometheus"
   static_configs:
   - targets: ["localhost:9090"]
 - job_name: "clickhouse"
   static_configs:
     - targets: ["<subdomain1>.<subdomain2>.aws.byoc.clickhouse.cloud:8443"]
   scheme: https
   metrics_path: "/metrics_all"
   basic_auth:
     username: <KEY_ID>
     password: <KEY_SECRET>
   honor_labels: true
```

また[このブログ記事](https://clickhouse.com/blog/clickhouse-cloud-now-supports-prometheus-monitoring) および [ClickHouse用のPrometheusセットアップドキュメント](/ja/integrations/prometheus)も参照してください。
