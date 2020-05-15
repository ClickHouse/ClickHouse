---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 32
toc_title: "\u30AB\u30D5\u30ABname"
---

# カフカname {#kafka}

このエンジンは [アパッチ-カフカ](http://kafka.apache.org/).

カフカはあなたを可能にします:

-   データフローを公開または購読する。
-   整理-フォールトトレラント保管します。
-   プロセスストリームが使用可能になるとき。

## テーブルの作成 {#table_engine-kafka-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_row_delimiter = 'delimiter_symbol',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_skip_broken_messages = N]
```

必須パラメータ:

-   `kafka_broker_list` – A comma-separated list of brokers (for example, `localhost:9092`).
-   `kafka_topic_list` – A list of Kafka topics.
-   `kafka_group_name` – A group of Kafka consumers. Reading margins are tracked for each group separately. If you don’t want messages to be duplicated in the cluster, use the same group name everywhere.
-   `kafka_format` – Message format. Uses the same notation as the SQL `FORMAT` 機能、のような `JSONEachRow`. 詳細については、 [形式](../../../interfaces/formats.md) セクション。

任意変数:

-   `kafka_row_delimiter` – Delimiter character, which ends the message.
-   `kafka_schema` – Parameter that must be used if the format requires a schema definition. For example, [Cap’n Proto](https://capnproto.org/) スキーマファイルへのパスとルートの名前が必要です `schema.capnp:Message` オブジェクト。
-   `kafka_num_consumers` – The number of consumers per table. Default: `1`. 指定しこれからも、多くの消費者の場合、スループットの消費が不足しています。 の総数消費者を超えることはできませんパーティションの数の問題から一つだけの消費者割り当てることができた。
-   `kafka_skip_broken_messages` – Kafka message parser tolerance to schema-incompatible messages per block. Default: `0`. もし `kafka_skip_broken_messages = N` その後、エンジンはスキップ *N* 解析できないKafkaメッセージ(メッセージはデータの行と同じです)。

例:

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  SELECT * FROM queue LIMIT 5;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka SETTINGS kafka_broker_list = 'localhost:9092',
                            kafka_topic_list = 'topic',
                            kafka_group_name = 'group1',
                            kafka_format = 'JSONEachRow',
                            kafka_num_consumers = 4;

  CREATE TABLE queue2 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">

<summary>テーブルを作成する非推奨の方法</summary>

!!! attention "注意"
    用途では使用しないでください方法で新規プロジェクト. 可能であれば、古いプロジェクトを上記の方法に切り替えます。

``` sql
Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
      [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_skip_broken_messages])
```

</details>

## 説明 {#description}

届いたメッセージは自動的に追跡で、それぞれのメッセージグループでは数えます。 データを取得したい場合は、別のグループ名を持つテーブルのコピーを作成します。

グループは柔軟で、クラスター上で同期されます。 たとえば、クラスター内のテーブルの10のトピックと5つのコピーがある場合、各コピーは2つのトピックを取得します。 コピー数が変更されると、トピックは自動的にコピー全体に再配布されます。 これについての詳細を読むhttp://kafka.apache.org/intro。

`SELECT` は特に役立つメッセージを読む(以外のデバッグ)では、それぞれのメッセージでしか読み込むことができます。 ではより実践的な創出の実時間スレッドを実現します。 これを行うには:

1.  エンジンを使用してkafkaコンシューマーを作成し、データストリームとみなします。
2.  目的の構造を持つテーブルを作成します。
3.  を実現しュに変換するデータからのエンジンを入れていたとして作成されます。

とき `MATERIALIZED VIEW` 入、エンジンでデータを収集しみいただけます。 これにより、Kafkaからメッセージを継続的に受信し、必要な形式に変換することができます `SELECT`.
このようにして、異なる詳細レベル（grouping-aggregation and without）を持つ複数のテーブルに書き込むことができます。

例えば:

``` sql
  CREATE TABLE queue (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1', 'JSONEachRow');

  CREATE TABLE daily (
    day Date,
    level String,
    total UInt64
  ) ENGINE = SummingMergeTree(day, (day, level), 8192);

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() as total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

受信したメッ [max\_insert\_block\_size](../../../operations/server-configuration-parameters/settings.md#settings-max_insert_block_size). ブロックが内に形成されなかった場合 [stream\_flush\_interval\_ms](../../../operations/server-configuration-parameters/settings.md) ミリ秒では、データはブロックの完全性に関係なくテーブルにフラッシュされます。

リクエストを受けた話題のデータは変更に変換ロジック、切り離しを実現ビュー:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

ターゲットテーブルを次のように変更する場合 `ALTER` お勧めいたしま字の材質ビューを避ける異なるターゲットテーブルのデータからの眺め。

## 設定 {#configuration}

GraphiteMergeTreeと同様に、KafkaエンジンはClickHouse configファイルを使用した拡張構成をサポートしています。 使用できる設定キーは二つあります:global (`kafka`）とトピックレベル (`kafka_*`). グローバル構成が最初に適用され、トピックレベル構成が適用されます(存在する場合)。

``` xml
  <!-- Global configuration options for all tables of Kafka engine type -->
  <kafka>
    <debug>cgrp</debug>
    <auto_offset_reset>smallest</auto_offset_reset>
  </kafka>

  <!-- Configuration specific for topic "logs" -->
  <kafka_logs>
    <retry_backoff_ms>250</retry_backoff_ms>
    <fetch_min_bytes>100000</fetch_min_bytes>
  </kafka_logs>
```

可能な構成オプションの一覧については、 [librdkafka設定リファレンス](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md). アンダースコアを使う (`_`)ClickHouse構成のドットの代わりに。 例えば, `check.crcs=true` されます `<check_crcs>true</check_crcs>`.

## 仮想列 {#virtual-columns}

-   `_topic` — Kafka topic.
-   `_key` — Key of the message.
-   `_offset` — Offset of the message.
-   `_timestamp` — Timestamp of the message.
-   `_partition` — Partition of Kafka topic.

**また見なさい**

-   [仮想列](../index.md#table_engines-virtual_columns)

[元の記事](https://clickhouse.tech/docs/en/operations/table_engines/kafka/) <!--hide-->
