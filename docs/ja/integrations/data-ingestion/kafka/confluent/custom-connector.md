---
sidebar_label: Confluent Platform上のKafka Connector Sink
sidebar_position: 2
slug: /ja/integrations/kafka/cloud/confluent/custom-connector
description: Kafka ConnectとClickHouseを使用したClickHouse Connector Sinkの利用
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# Confluent CloudとClickHouseの統合

<div class='vimeo-container'>
  <iframe src="//www.youtube.com/embed/SQAiPVbd3gg"
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
以下に精通していることを前提としています:
* [ClickHouse Connector Sink](../kafka-clickhouse-connect-sink.md)
* Confluent Cloudおよび[カスタムコネクタ](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/overview.html)。

## Confluent CloudでのClickHouse公式Kafkaコネクタ

### Confluent Cloudへのインストール
これはConfluent CloudでClickHouse Sink Connectorを使い始めるための簡易ガイドです。
詳細については、[公式Confluentドキュメント](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/custom-connector-qs.html#uploading-and-launching-the-connector)を参照してください。

#### トピックの作成
Confluent Cloudでのトピック作成は非常に簡単で、詳細な手順は[こちら](https://docs.confluent.io/cloud/current/client-apps/topics/manage.html)にあります。

#### 重要な注意事項
* Kafkaトピック名はClickHouseテーブル名と同じでなければなりません。これを調整する方法としてトランスフォーマー（例えば[ExtractTopic](https://docs.confluent.io/platform/current/connect/transforms/extracttopic.html)）を使用できます。
* より多くのパーティションが必ずしもより高いパフォーマンスを意味するわけではありません - 詳細とパフォーマンスのヒントは今後のガイドで説明します。

#### コネクタのインストール
コネクタを[こちらのリポジトリ](https://github.com/ClickHouse/clickhouse-kafka-connect/releases)からダウンロードできます。コメントや問題の提出も歓迎します！

「Connector Plugins」 -> 「Add plugin」へ移動し、以下の設定を使用してください:

```
'Connector Class' - 'com.clickhouse.kafka.connect.ClickHouseSinkConnector'
'Connector type' - Sink
'Sensitive properties' - 'password'。構成中にClickHouseパスワードの入力がマスクされることを保証します。
```
例:
<img src={require('./images/AddCustomConnectorPlugin.png').default} class="image" alt="カスタムコネクタを追加するための設定" style={{width: '50%'}}/>

#### 接続情報の収集
<ConnectionDetails />

#### コネクタの設定
「Connectors」 -> 「Add Connector」へ移動し、以下の設定を使用してください（値は例示のためだけです）:

```json
{
  "database": "<DATABASE_NAME>",
  "errors.retry.timeout": "30",
  "exactlyOnce": "false",
  "schemas.enable": "false",
  "hostname": "<CLICKHOUSE_HOSTNAME>",
  "password": "<SAMPLE_PASSWORD>",
  "port": "8443",
  "ssl": "true",
  "topics": "<TOPIC_NAME>",
  "username": "<SAMPLE_USERNAME>",
  "key.converter": "org.apache.kafka.connect.storage.StringConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter.schemas.enable": "false"
}
```

#### 接続エンドポイントの指定
コネクタがアクセスできるエンドポイントの許可リストを指定する必要があります。
ネットワークのイグレスエンドポイントを追加する際は、完全修飾ドメイン名（FQDN）を使用する必要があります。
例: `u57swl97we.eu-west-1.aws.clickhouse.com:8443`

:::note
HTTP(S)ポートを指定する必要があります。コネクタはまだネイティブプロトコルをサポートしていません。
:::

[ドキュメントを読む](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/custom-connector-qs.html#cc-byoc-endpoints)

これで準備は整いました！

#### 既知の制限事項
* カスタムコネクタはパブリックインターネットエンドポイントを使用する必要があります。静的IPアドレスはサポートされていません。
* 一部のカスタムコネクタプロパティは上書きできます。公式ドキュメントの[完全なリストを見る](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/custom-connector-manage.html#override-configuration-properties)。
* カスタムコネクタは[一部のAWSリージョンでのみ利用可能](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/custom-connector-fands.html#supported-aws-regions)です。
* 公式ドキュメントでの[カスタムコネクタの制限リストを見る](https://docs.confluent.io/cloud/current/connectors/bring-your-connector/custom-connector-fands.html#limitations)。
