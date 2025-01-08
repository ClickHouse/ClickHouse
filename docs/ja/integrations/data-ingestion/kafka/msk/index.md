---
sidebar_label: Amazon MSK と Kafka Connector Sink
sidebar_position: 1
slug: /ja/integrations/kafka/cloud/amazon-msk/
description: ClickHouse 公式 Kafka コネクタと Amazon MSK の連携
keywords: [integration, kafka, amazon msk, sink, connector]
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# Amazon MSK と ClickHouse の統合

<div class='vimeo-container'>
  <iframe src="//www.youtube.com/embed/6lKI_WlQ3-s"
    width="640"
    height="360"
    frameborder="0"
    allow="autoplay;
    fullscreen;
    picture-in-picture"
    allowfullscreen>
  </iframe>
</div>

## 前提条件
以下を前提としています:
* あなたが [ClickHouse コネクタ Sink](../kafka-clickhouse-connect-sink.md)、Amazon MSK、MSK コネクタに精通していること。Amazon MSK [開始ガイド](https://docs.aws.amazon.com/msk/latest/developerguide/getting-started.html)と[MSK コネクトガイド](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect.html)を推奨します。
* MSK ブローカーが公開アクセス可能であること。開発者ガイドの[公開アクセス](https://docs.aws.amazon.com/msk/latest/developerguide/public-access.html)セクションを参照してください。

## ClickHouse の公式 Kafka コネクタと Amazon MSK

### 接続の詳細を集める

<ConnectionDetails />

### ステップ
1. [ClickHouse コネクタ Sink](../kafka-clickhouse-connect-sink.md)を理解していることを確認してください。
1. [MSK インスタンスを作成する](https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster.html)。
1. [IAM ロールを作成して割り当てる](https://docs.aws.amazon.com/msk/latest/developerguide/create-client-iam-role.html)。
1. ClickHouse Connect Sink の[リリースページ](https://github.com/ClickHouse/clickhouse-kafka-connect/releases)から `jar` ファイルをダウンロードします。
1. Amazon MSK コンソールの[カスタムプラグインページ](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-plugins.html)でダウンロードした `jar` ファイルをインストールします。
1. コネクタが公開 ClickHouse インスタンスと通信する場合は、[インターネットアクセスを有効にします](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-internet-access.html)。
1. 設定でトピック名、ClickHouse インスタンスホスト名、およびパスワードを指定します。
```yml
connector.class=com.clickhouse.kafka.connect.ClickHouseSinkConnector
tasks.max=1
topics=<topic_name>
ssl=true
security.protocol=SSL
hostname=<hostname>
database=<database_name>
password=<password>
ssl.truststore.location=/tmp/kafka.client.truststore.jks
port=8443
value.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
exactlyOnce=true
username=default
schemas.enable=false
```

## パフォーマンスチューニング
パフォーマンスを向上させるワンステップとして、**worker** 設定に以下を追加して、Kafka から取得するバッチサイズとレコード数を調整することができます:
```yml
consumer.max.poll.records=[NUMBER OF RECORDS]
consumer.max.partition.fetch.bytes=[NUMBER OF RECORDS * RECORD SIZE IN BYTES]
```

使用する具体的な値は、希望のレコード数やレコードサイズに基づいて異なります。例えば、デフォルト値は以下の通りです:

```yml
consumer.max.poll.records=500
consumer.max.partition.fetch.bytes=1048576
```

詳細情報（実装やその他の考慮事項について）は、公式の [Kafka](https://kafka.apache.org/documentation/#consumerconfigs) および 
[Amazon MSK](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-workers.html#msk-connect-create-custom-worker-config) ドキュメントで確認できます。

## MSK Connect のネットワーキングに関する注意事項

MSK Connect が ClickHouse に接続するために、MSK クラスターをプライベートサブネットに配置し、プライベート NAT を使用してインターネットアクセスを確保することを推奨します。これを設定する方法は以下に記載されています。パブリックサブネットもサポートされていますが、Elastic IP アドレスを ENI に常に割り当てる必要があるため、推奨されません。詳しくは [AWS の詳細](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-internet-access.html)をご覧ください。

1. **プライベートサブネットの作成:** VPC 内に新しいサブネットを作成し、プライベートサブネットとして指定します。このサブネットにはインターネットへの直接アクセスがありません。
1. **NAT ゲートウェイの作成:** VPC のパブリックサブネットに NAT ゲートウェイを作成します。NAT ゲートウェイは、プライベートサブネットのインスタンスがインターネットや他の AWS サービスと接続できるようにしますが、インターネットからこれらのインスタンスへの接続を防ぎます。
1. **ルートテーブルの更新:** インターネットに向かうトラフィックを NAT ゲートウェイに送るルートを追加します。
1. **セキュリティグループとネットワーク ACL の設定:** [セキュリティグループ](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-security-groups.html)と[ネットワーク ACL（アクセス制御リスト）](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-network-acls.html)を構成し、ClickHouse インスタンスへの関連するトラフィックを許可します。
   1. ClickHouse Cloud の場合、セキュリティグループを構成してポート 9440 および 8443 でのインバウンドトラフィックを許可します。
   2. セルフマネージドの ClickHouse の場合、セキュリティグループが設定ファイルのポート（デフォルトは 8123）でのインバウンドトラフィックを許可するように構成します。
2. **MSK にセキュリティグループをアタッチ:** これらの新しいセキュリティグループを NAT ゲートウェイにルートされた状態で MSK クラスターにアタッチします。
