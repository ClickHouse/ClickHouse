---
sidebar_label: ClickHouseとKafkaの統合
sidebar_position: 1
slug: /ja/integrations/kafka
description: ClickHouseとKafkaの紹介
---

# ClickHouseとKafkaの統合

[Apache Kafka](https://kafka.apache.org/)は、数千の企業が高性能データパイプライン、ストリーミング分析、データ統合、ミッションクリティカルなアプリケーションのために使用しているオープンソースの分散イベントストリーミングプラットフォームです。KafkaとClickHouseが関与するほとんどの場合、ユーザーはKafkaベースのデータをClickHouseに挿入したいと考えています。以下に、その両方のユースケースに対するいくつかのオプションを示し、それぞれのアプローチの利点と欠点を特定します。

## オプションの選択

KafkaをClickHouseと統合する際、高レベルのアプローチについて早期にアーキテクチャ上の決定を下す必要があります。以下に最も一般的な戦略を示します。

### Kafka用ClickPipes（ClickHouse Cloud）
* [**ClickPipes**](../clickpipes/kafka.md)は、ClickHouse Cloudにデータを取り込む最も簡単で直感的な方法を提供します。現在、Apache Kafka、Confluent Cloud、およびAmazon MSKをサポートしており、今後さらに多くのデータソースが追加される予定です。

### 3rd-Party CloudベースのKafka接続
* [**Confluent Cloud**](./confluent/index.md) - Confluentプラットフォームは、ClickHouse Connector SinkをConfluent Cloud上で[アップロードして稼働](./confluent/custom-connector.md)するオプションや、Apache KafkaをHTTPまたはHTTPS経由でAPIと統合する[Confluentプラットフォーム用のHTTP Sink Connector](./confluent/kafka-connect-http.md)を利用するオプションを提供します。

* [**Amazon MSK**](./msk/index.md) - Amazon MSK Connectフレームワークをサポートして、Apache KafkaクラスターからClickHouseなどの外部システムにデータを転送します。Amazon MSKにClickHouse Kafka Connectをインストールすることができます。

* [**Redpanda Cloud**](https://cloud.redpanda.com/) - Redpandaは、Kafka API互換のストリーミングデータプラットフォームで、ClickHouseのアップストリームデータソースとして使用できます。ホストされたクラウドプラットフォームであるRedpanda Cloudは、Kafkaプロトコル経由でClickHouseと統合され、ストリーミング分析ワークロードのリアルタイムデータ取り込みを可能にします。

### セルフマネージドKafka接続
* [**Kafka Connect**](./kafka-clickhouse-connect-sink.md) - Kafka Connectは、Apache Kafkaの無料のオープンソースコンポーネントで、Kafkaと他のデータシステム間でのシンプルなデータ統合用に設計された集中データハブとして機能します。コネクタは、Kafkaから他のデータストアへのデータのスケーラブルで信頼性の高いストリーミングをシンプルに提供します。Source Connectorsは他のシステムからKafkaトピックにデータを挿入し、Sink ConnectorsはKafkaトピックからClickHouseなどのデータストアにデータを配信します。

* [**Vector**](./kafka-vector.md) - Vectorはベンダーに依存しないデータパイプラインです。Kafkaからデータを読み込み、ClickHouseにイベントを送信する能力があり、堅牢な統合オプションを提供します。

* [**JDBC Connect Sink**](./kafka-connect-jdbc.md) - Kafka Connect JDBC Sinkコネクタは、Kafkaトピックから任意のJDBCドライバーを備えたリレーショナルデータベースにデータをエクスポートすることを可能にします。

* **カスタムコード** - KafkaとClickHouse用のそれぞれのクライアントライブラリを使用したカスタムコードは、イベントのカスタム処理が必要な場合に適切なケースがあります。この文書の範囲を超えています。

* [**Kafkaテーブルエンジン**](./kafka-table-engine.md)は、ネイティブClickHouse統合（ClickHouse Cloudでは利用不可）を提供します。このテーブルエンジンは**プル**してデータをソースシステムから取得します。これには、ClickHouseがKafkaに直接アクセスできることが必要です。

* [**名前付きコレクションを用いたKafkaテーブルエンジン**](./kafka-table-engine-named-collections.md) - 名前付きコレクションを使用することで、KafkaとのネイティブClickHouse統合が可能になります。このアプローチは、複数のKafkaクラスターへの安全な接続を許可し、構成管理を集中化し、スケーラビリティとセキュリティを向上させます。

### アプローチの選択
いくつかの決定ポイントに要約されます：

* **接続性** - Kafkaテーブルエンジンは、ClickHouseが目的地である場合、Kafkaからプルできる必要があります。これには双方向接続性が必要です。例えば、ClickHouseがクラウドにあり、Kafkaがセルフマネージドされている場合のように、ネットワークが分断されている場合、コンプライアンスやセキュリティ上の理由からこれを解除することをためらうかもしれません。（このアプローチは現在のClickHouse Cloudではサポートされていません。）Kafkaテーブルエンジンは、消費者用のスレッドを使用して、ClickHouse自身のリソースを利用します。このリソースの負担をClickHouseにかけることは、リソースの制約のために不可能かもしれず、また、アーキテクトは関心の分離を好むかもしれません。この場合、別のプロセスとして実行され、異なるハードウェア上にデプロイできるKafka Connectのようなツールが好ましいかもしれません。これにより、Kafkaデータのプルを担当するプロセスをClickHouseとは独立してスケールすることができます。

* **クラウドでのホスティング** - クラウドベンダは、プラットフォーム上で利用可能なKafkaコンポーネントに制限を設ける場合があります。各クラウドベンダの推奨オプションを探るために、ガイドに従ってください。

* **外部でのエンリッチメント** - メッセージは、Materialized Viewのselectステートメント内の関数を使用してClickHouseに挿入される前に操作できますが、ユーザーは複雑なエンリッチメントをClickHouse外部に移動することを好むかもしれません。

* **データフローの方向** - Vectorは、KafkaからClickHouseへのデータ転送のみをサポートしています。

## 前提条件 

リンクされたユーザーガイドは、以下を前提としています：

* Kafkaの基礎（プロデューサー、コンシューマー、トピックなど）に精通していること。
* これらの例のためにトピックを準備していること。すべてのデータがKafkaにJSONとして保存されていると想定していますが、Avroを使用している場合も原則は同じです。
* kcat（以前はkafkacatと呼ばれていました）が素晴らしい[こちら](https://github.com/edenhill/kcat)にあることを利用して、Kafkaデータを公開および消費します。
* サンプルデータをロードするためのいくつかのpythonスクリプトを参照していますが、例を自分のデータセットに適応させてください。
* ClickHouseのMaterialized Viewについて大まかに理解していること。
