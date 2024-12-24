---
title: Named Collectionsを使用したClickHouseとKafkaの統合
description: named collectionsを使用してClickHouseをKafkaに接続する方法
keywords: [named collection, 方法, kafka]
---

# Named Collectionsを使用したClickHouseとKafkaの統合

## はじめに

このガイドでは、named collectionsを使用してClickHouseをKafkaに接続する方法を探ります。named collectionsのための設定ファイルを使用することには、以下のような利点があります：
- 設定の集中管理と容易な管理。
- SQLテーブル定義を変更せずに設定を変更可能。
- 単一の設定ファイルを調査することで、設定のレビューとトラブルシューティングが容易。

このガイドは、Apache Kafka 3.4.1とClickHouse 24.5.1でテストされています。

## 前提条件

このドキュメントは以下を前提としています：
1. 動作中のKafkaクラスター。
2. 設定済みで稼働中のClickHouseクラスター。
3. SQLの基本的な知識と、ClickHouseおよびKafkaの設定に関する基本理解。

## 必要条件

named collectionを作成するユーザーが必要なアクセス権を持っていることを確認してください：

```xml
<access_management>1</access_management>
<named_collection_control>1</named_collection_control>
<show_named_collections>1</show_named_collections>
<show_named_collections_secrets>1</show_named_collections_secrets>
```

アクセス制御を有効にする詳細については、[ユーザー管理ガイド](./../../../guides/sre/user-management/index.md)を参照してください。

## 設定

ClickHouseの`config.xml`ファイルに以下のセクションを追加してください：

```xml
<!-- Kafka統合のためのnamed collections -->
<named_collections>
    <cluster_1>
        <!-- ClickHouse Kafkaエンジンパラメータ -->
        <kafka_broker_list>c1-kafka-1:9094,c1-kafka-2:9094,c1-kafka-3:9094</kafka_broker_list>
        <kafka_topic_list>cluster_1_clickhouse_topic</kafka_topic_list>
        <kafka_group_name>cluster_1_clickhouse_consumer</kafka_group_name>
        <kafka_format>JSONEachRow</kafka_format>
        <kafka_commit_every_batch>0</kafka_commit_every_batch>
        <kafka_num_consumers>1</kafka_num_consumers>
        <kafka_thread_per_consumer>1</kafka_thread_per_consumer>

        <!-- Kafka拡張設定 -->
        <kafka>
            <security_protocol>SASL_SSL</security_protocol>
            <enable_ssl_certificate_verification>false</enable_ssl_certificate_verification>
            <sasl_mechanism>PLAIN</sasl_mechanism>
            <sasl_username>kafka-client</sasl_username>
            <sasl_password>kafkapassword1</sasl_password>
            <debug>all</debug>
            <auto_offset_reset>latest</auto_offset_reset>
        </kafka>
    </cluster_1>

    <cluster_2>
        <!-- ClickHouse Kafkaエンジンパラメータ -->
        <kafka_broker_list>c2-kafka-1:29094,c2-kafka-2:29094,c2-kafka-3:29094</kafka_broker_list>
        <kafka_topic_list>cluster_2_clickhouse_topic</kafka_topic_list>
        <kafka_group_name>cluster_2_clickhouse_consumer</kafka_group_name>
        <kafka_format>JSONEachRow</kafka_format>
        <kafka_commit_every_batch>0</kafka_commit_every_batch>
        <kafka_num_consumers>1</kafka_num_consumers>
        <kafka_thread_per_consumer>1</kafka_thread_per_consumer>

        <!-- Kafka拡張設定 -->
        <kafka>
            <security_protocol>SASL_SSL</security_protocol>
            <enable_ssl_certificate_verification>false</enable_ssl_certificate_verification>
            <sasl_mechanism>PLAIN</sasl_mechanism>
            <sasl_username>kafka-client</sasl_username>
            <sasl_password>kafkapassword2</sasl_password>
            <debug>all</debug>
            <auto_offset_reset>latest</auto_offset_reset>
        </kafka>
    </cluster_2>
</named_collections>
```

### 設定メモ

1. Kafkaアドレスや関連設定をお使いのKafkaクラスター設定に合わせて調整してください。
2. `<kafka>`の前のセクションには、ClickHouse Kafkaエンジンのパラメータが含まれます。すべてのパラメータについては、[Kafkaエンジンパラメータ](https://clickhouse.com/docs/ja/engines/table-engines/integrations/kafka)を参照してください。
3. `<kafka>`内のセクションには、Kafka拡張設定オプションが含まれます。より多くのオプションについては、[librdkafka設定](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)を参照してください。
4. この例では、`SASL_SSL`セキュリティプロトコルと`PLAIN`メカニズムを使用しています。これらの設定をあなたのKafkaクラスター設定に基づいて調整してください。

## テーブルとデータベースの作成

ClickHouseクラスター上に必要なデータベースとテーブルを作成します。ClickHouseを単一ノードとして実行している場合は、SQLコマンドのクラスタ部分を省略し、`ReplicatedMergeTree`の代わりに他のエンジンを使用してください。

### データベースの作成

```sql
CREATE DATABASE kafka_testing ON CLUSTER LAB_CLICKHOUSE_CLUSTER;
```

### Kafkaテーブルの作成

最初のKafkaクラスター用の最初のKafkaテーブルを作成：

```sql
CREATE TABLE kafka_testing.first_kafka_table ON CLUSTER LAB_CLICKHOUSE_CLUSTER
(
    `id` UInt32,
    `first_name` String,
    `last_name` String
)
ENGINE = Kafka(cluster_1);
```

2つ目のKafkaクラスター用の2つ目のKafkaテーブルを作成：

```sql
CREATE TABLE kafka_testing.second_kafka_table ON CLUSTER STAGE_CLICKHOUSE_CLUSTER
(
    `id` UInt32,
    `first_name` String,
    `last_name` String
)
ENGINE = Kafka(cluster_2);
```

### レプリケートテーブルの作成

最初のKafkaテーブル用のテーブルを作成：

```sql
CREATE TABLE kafka_testing.first_replicated_table ON CLUSTER STAGE_CLICKHOUSE_CLUSTER
(
    `id` UInt32,
    `first_name` String,
    `last_name` String
) ENGINE = ReplicatedMergeTree()
ORDER BY id;
```

2つ目のKafkaテーブル用のテーブルを作成：

```sql
CREATE TABLE kafka_testing.second_replicated_table ON CLUSTER STAGE_CLICKHOUSE_CLUSTER
(
    `id` UInt32,
    `first_name` String,
    `last_name` String
) ENGINE = ReplicatedMergeTree()
ORDER BY id;
```

### Materialized Viewの作成

最初のKafkaテーブルから最初のレプリケートテーブルにデータを挿入するためのmaterialized viewを作成：

```sql
CREATE MATERIALIZED VIEW kafka_testing.cluster_1_mv ON CLUSTER STAGE_CLICKHOUSE_CLUSTER TO first_replicated_table AS
SELECT 
    id,
    first_name,
    last_name
FROM first_kafka_table;
```

2つ目のKafkaテーブルから2つ目のレプリケートテーブルにデータを挿入するためのmaterialized viewを作成：

```sql
CREATE MATERIALIZED VIEW kafka_testing.cluster_2_mv ON CLUSTER STAGE_CLICKHOUSE_CLUSTER TO second_replicated_table AS
SELECT 
    id,
    first_name,
    last_name
FROM second_kafka_table;
```

## 設定の検証

Kafkaクラスター上で以下の消費者グループが見えるはずです：
- `cluster_1_clickhouse_consumer` on `cluster_1`
- `cluster_2_clickhouse_consumer` on `cluster_2`

ClickHouseノードのいずれかで以下のクエリを実行して、両方のテーブルのデータを確認してください：

```sql
SELECT * FROM first_replicated_table LIMIT 10;
```

```sql
SELECT * FROM second_replicated_table LIMIT 10;
```

### メモ

このガイドでは、両方のKafkaトピックに取り込まれるデータは同じですが、実際には異なるでしょう。必要に応じて多くのKafkaクラスターを追加することが可能です。

例の出力：

```sql
┌─id─┬─first_name─┬─last_name─┐
│  0 │ FirstName0 │ LastName0 │
│  1 │ FirstName1 │ LastName1 │
│  2 │ FirstName2 │ LastName2 │
└────┴────────────┴───────────┘
```

これで、Named Collectionsを使用したClickHouseとKafkaの統合のセットアップは完了です。ClickHouseの`config.xml`ファイルにKafka設定を集中させることで、設定の管理と調整がより簡単になり、効率的な統合が可能になります。
