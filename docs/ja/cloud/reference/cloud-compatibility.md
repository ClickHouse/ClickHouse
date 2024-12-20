---
slug: /ja/whats-new/cloud-compatibility
sidebar_label: Cloud Compatibility
title: クラウド互換性
---

# ClickHouse Cloud — 互換性ガイド

このガイドは、ClickHouse Cloudにおける機能的および操作上の期待を概観します。ClickHouse CloudはオープンソースのClickHouseディストリビューションに基づいていますが、アーキテクチャや実装にいくつかの違いがあるかもしれません。[ClickHouse Cloudをどのように構築したか](https://clickhouse.com/blog/building-clickhouse-cloud-from-scratch-in-a-year)についてのブログも背景として興味深いでしょう。

## ClickHouse Cloud アーキテクチャ
ClickHouse Cloudは、運用のオーバーヘッドを大幅に簡素化し、大規模にClickHouseを運用するコストを削減します。デプロイメントのサイズを事前に設定したり、高可用性のためにレプリケーションを設定したり、手動でデータをシャードしたり、ワークロードが増えたときにサーバーをスケールアップしたり、使用していないときにスケールダウンしたりする必要はありません。これらはすべて、私たちが処理します。

これらの利点は、ClickHouse Cloudの基盤となるアーキテクチャの選択によるものです：
- コンピュートとストレージが分離されており、異なる次元で自動的にスケーリングできるため、静的なインスタンス構成でストレージやコンピュートを過剰にプロビジョンする必要はありません。
- オブジェクトストアの上にある階層型ストレージと多層キャッシングは、事実上無制限のスケーリングと良好な価格/性能比を提供し、ストレージパーティションを事前に設定する必要や高いストレージコストについて心配する必要はありません。
- 高可用性はデフォルトでオンになっており、レプリケーションは透過的に管理されているため、アプリケーションの構築やデータの分析に集中できます。
- 変動する継続的なワークロードのための自動スケーリングはデフォルトでオンになっており、サービスのサイズを事前に設定したり、ワークロードが増えたときにサーバーをスケールアップしたり、アクティビティが減少したときに手動でサーバーをスケールダウンしたりする必要はありません。
- 断続的なワークロードのためのシームレスなハイバネーションがデフォルトでオンになっており、新しいクエリが到着したときに計算リソースを自動的に一時停止し、透過的に再開するため、アイドルリソースの支払いをする必要はありません。
- 高度なスケーリング制御を使用して、コスト制御のための自動スケーリング最大値を設定したり、専門的な性能要件を持つアプリケーションのためにコンピュートリソースを予約するための自動スケーリング最小値を設定できます。

## 機能
ClickHouse Cloudは、オープンソースディストリビューションのClickHouseの厳選された機能へのアクセスを提供します。以下のテーブルは、現時点でClickHouse Cloudで無効になっているいくつかの機能を説明しています。

### DDL構文
ClickHouse CloudのDDL構文は、大部分がセルフマネージドインストールで利用可能なものと一致するはずです。一部の顕著な例外:
  - 現在利用できない`CREATE AS SELECT`のサポート。代替策として、`CREATE ... EMPTY ... AS SELECT`を使用し、そのテーブルに挿入することをお勧めします（例については[このブログ](https://clickhouse.com/blog/getting-data-into-clickhouse-part-1)を参照してください）。
  - 一部のエクスペリメンタルな構文は無効にされる可能性があり、たとえば`ALTER TABLE … MODIFY QUERY`文です。
  - セキュリティ上の理由から一部のイントロスペクション機能が無効になることがあり、たとえば`addressToLine` SQL関数などです。
  - ClickHouse Cloudで`ON CLUSTER`パラメータを使用しないでください。これらは基本的に無効な関数ですが、[マクロ](https://clickhouse.com/docs/ja/operations/server-configuration-parameters/settings#macros)を使用しようとするとエラーが発生する可能性があります。マクロはClickHouse Cloudでは通常機能しないし、必要でもありません。

### データベースおよびテーブルエンジン

ClickHouse Cloudはデフォルトで高可用性のレプリケートされたサービスを提供します。その結果、すべてのデータベースおよびテーブルエンジンは「Replicated」となります。たとえば、`ReplicatedMergeTree`と`MergeTree`はClickHouse Cloudで使用される場合、同一です。

**サポートされているテーブルエンジン**

  - ReplicatedMergeTree (指定がない場合のデフォルト)
  - ReplicatedSummingMergeTree
  - ReplicatedAggregatingMergeTree
  - ReplicatedReplacingMergeTree
  - ReplicatedCollapsingMergeTree
  - ReplicatedVersionedCollapsingMergeTree
  - MergeTree (ReplicatedMergeTreeに変換)
  - SummingMergeTree (ReplicatedSummingMergeTreeに変換)
  - AggregatingMergeTree (ReplicatedAggregatingMergeTreeに変換)
  - ReplacingMergeTree (ReplicatedReplacingMergeTreeに変換)
  - CollapsingMergeTree (ReplicatedCollapsingMergeTreeに変換)
  - VersionedCollapsingMergeTree (ReplicatedVersionedCollapsingMergeTreeに変換)
  - URL
  - View
  - MaterializedView
  - GenerateRandom
  - Null
  - Buffer
  - Memory
  - Deltalake
  - Hudi
  - MySQL
  - MongoDB
  - NATS
  - RabbitMQ
  - PostgreSQL
  - S3

### インターフェース
ClickHouse CloudはHTTPS、ネイティブインターフェース、および[MySQLワイヤプロトコル](/docs/ja/interfaces/mysql)をサポートしています。Postgresのような他のインターフェースのサポートは近日中に予定されています。

### Dictionary
Dictionaryは、ClickHouseでルックアップを高速化するための一般的な方法です。ClickHouse Cloudは現在、PostgreSQL、MySQL、リモートおよびローカルClickHouseサーバー、Redis、MongoDB、およびHTTPソースからのDictionaryをサポートしています。

### 分散クエリ
クラウド内でのクロスクラスタ通信と外部セルフマネージドClickHouseクラスタとの通信のために、分散ClickHouseクエリをサポートします。ClickHouse Cloudは現在、以下の統合エンジンを使用して分散クエリをサポートしています：
  - Deltalake
  - Hudi
  - MySQL
  - MongoDB
  - NATS
  - RabbitMQ
  - PostgreSQL
  - S3

SQLite、ODBC、JDBC、Redis、HDFS、Hiveのような一部の外部データベースおよびテーブルエンジンとの分散クエリはまだサポートされていません。

### ユーザー定義関数

ユーザー定義関数は、ClickHouseの最近の機能です。ClickHouse Cloudは現在、SQL UDFのみをサポートしています。

### エクスペリメンタル機能

エクスペリメンタル機能は、サービスデプロイメントの安定性を確保するためにClickHouse Cloudサービスで無効化されています。

### Kafka

[Kafka テーブルエンジン](/docs/ja/integrations/data-ingestion/kafka/index.md)は、ClickHouse Cloudで一般に利用可能ではありません。代わりに、Kafka接続コンポーネントをClickHouseサービスから分離するアーキテクチャを利用することをお勧めします。Kafkaストリームからデータをプルするためには、[ClickPipes](https://clickhouse.com/cloud/clickpipes)をお勧めいたします。あるいは、プッシュベースの代替案として、[Kafkaユーザーガイド](/docs/ja/integrations/data-ingestion/kafka/index.md)にリストされているオプションを検討してください。

### Named collections

[Named collections](/ja/operations/named-collections)は現在、ClickHouse Cloudでサポートされていません。

## 運用上のデフォルトと考慮事項
以下はClickHouse Cloudサービスのデフォルト設定です。一部の設定はサービスの適切な運用を確保するために固定されており、他の設定は調整可能です。

### 運用の制限

#### `max_parts_in_total: 10,000`
MergeTreeテーブルの`max_parts_in_total`設定のデフォルト値は、100,000から10,000に引き下げられました。この変更の理由は、大量のデータパーツがクラウドでのサービスの起動時間を遅らせる可能性があることがわかったためです。多数のパーツは、通常はパーティションキーの選定が細かすぎることを示し、これは通常誤って行われるため避けるべきです。このデフォルトの変更により、これらのケースが早期に検出できるようになります。

#### `max_concurrent_queries: 1,000`
このサーバー単位の設定をデフォルトの100から1000に増加させ、より多くの同時性を許可しました。これにより、開発サービスでは2,000の同時クエリ、プロダクションでは3,000の同時クエリが可能になります。

#### `max_table_size_to_drop: 1,000,000,000,000`
この設定を50GBから1TBまでテーブル/パーティションの削除を許可するために増加しました。

### システム設定
ClickHouse Cloudは可変のワークロードに最適化されており、そのため大多数のシステム設定は現時点で構成可能ではありません。ほとんどのユーザーにとってシステム設定の調整は不要と考えていますが、高度なシステム調整に関する質問がある場合は、ClickHouse Cloudサポートにお問い合わせください。

### 高度なセキュリティ管理
ClickHouseサービスの作成の一環として、デフォルトのデータベースおよびこのデータベースに広範な権限を持つデフォルトユーザーが作成されます。この最初のユーザーは、他のユーザーを作成し、その権限をこのデータベースに割り当てることができます。これを超えて、Kerberos、LDAP、またはSSL X.509証明書認証を用いたデータベース内の以下のセキュリティ機能を有効にする機能は現時点ではサポートされていません。

## ロードマップ
以下のテーブルは、上記で説明したいくつかの機能を拡張するための取り組みを要約しています。フィードバックがある場合は、[こちらに提出](mailto:feedback@clickhouse.com)してください。

| 機能                                                                                      | 状況 |
|---------------------------------------------------------------------------------------------|:----------------------------------------|
|Dictionaryサポート：PostgreSQL、MySQL、リモートおよびローカルClickHouseサーバー、Redis、MongoDB、およびHTTPソース | **GAで追加済み** |
|SQLユーザー定義関数（UDF）                                                          | **GAで追加済み**                         |
|MySQLおよびPostgreSQLエンジン                                                | **GAで追加済み**                         |
|MySQLインターフェース                                                      | **GAで追加済み**                         |
|Postgresインターフェース                                                    | 近日公開                                      |
|SQLite、ODBC、Redis、HDFS、Hive向けエンジン                       | 近日公開                                      |
|Protobuf、Cap'n'Protoフォーマット                                   | 近日公開                                      |
|Kafkaテーブルエンジン                                                      | 推奨されません; ※上記の代替案を参照 |
|JDBCテーブルエンジン                                                       | 推奨されません                         |
|EmbeddedRocksDBエンジン                                            | 需要を評価中                       |
|実行可能なユーザー定義関数                                                | 需要を評価中                       |
