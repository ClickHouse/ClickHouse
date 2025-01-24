---
slug: /ja/engines/table-engines/integrations/kafka
sidebar_position: 110
sidebar_label: Kafka
---

# Kafka

このエンジンは [Apache Kafka](http://kafka.apache.org/) と連携します。

Kafkaの利点:

- データフローの発行や購読が可能です。
- フォールトトレラントなストレージを整理できます。
- ストリームを利用可能にした時点で処理します。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [ALIAS expr1],
    name2 [type2] [ALIAS expr2],
    ...
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'host:port',
    kafka_topic_list = 'topic1,topic2,...',
    kafka_group_name = 'group_name',
    kafka_format = 'data_format'[,]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0,]
    [kafka_client_id = '',]
    [kafka_poll_timeout_ms = 0,]
    [kafka_poll_max_batch_size = 0,]
    [kafka_flush_interval_ms = 0,]
    [kafka_thread_per_consumer = 0,]
    [kafka_handle_error_mode = 'default',]
    [kafka_commit_on_select = false,]
    [kafka_max_rows_per_message = 1];
```

必須パラメータ:

- `kafka_broker_list` — ブローカーのカンマ区切りリスト (例: `localhost:9092`)。
- `kafka_topic_list` — Kafka トピックのリスト。
- `kafka_group_name` — Kafka コンシューマのグループ。各グループの読み取り範囲は別々に追跡されます。クラスタ内でメッセージが重複しないようにする場合、すべての場所で同じグループ名を使用します。
- `kafka_format` — メッセージフォーマット。SQL の `FORMAT` 関数と同じ記法が使用され、例えば `JSONEachRow` が使用されます。詳細は [Formats](../../../interfaces/formats.md) を参照してください。

オプションパラメータ:

- `kafka_schema` — フォーマットがスキーマ定義を必要とする場合に使用されるパラメータ。例えば [Cap’n Proto](https://capnproto.org/) では、スキーマファイルへのパスとルート `schema.capnp:Message` オブジェクトの名前が必要です。
- `kafka_num_consumers` — テーブルあたりのコンシューマ数。1 つのコンシューマのスループットが不十分な場合は、さらにコンシューマを指定します。トピックのパーティション数を超えない数にする必要があります。1 つのパーティションには 1 つのコンシューマしか割り当てられず、ClickHouse がデプロイされたサーバー上の物理コア数を超えてはいけません。デフォルト: `1`。
- `kafka_max_block_size` — ポーリングの最大バッチサイズ(メッセージ単位)。デフォルト: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size)。
- `kafka_skip_broken_messages` — Kafka メッセージのパーサがブロックごとにスキーマと互換性のないメッセージを許容するかどうか。`kafka_skip_broken_messages = N` の場合、パーサで解析できない *N* の Kafka メッセージ（メッセージはデータの1行に相当）をスキップします。デフォルト: `0`。
- `kafka_commit_every_batch` — 全ブロックに書き込み後の単一コミットではなく、消費および処理された各バッチをコミットします。デフォルト: `0`。
- `kafka_client_id` — クライアント識別子。デフォルトは空です。
- `kafka_poll_timeout_ms` — Kafka からの単一ポールのタイムアウト。デフォルト: [stream_poll_timeout_ms](../../../operations/settings/settings.md#stream_poll_timeout_ms)。
- `kafka_poll_max_batch_size` — 単一の Kafka ポールでポーリングされる最大メッセージ量。デフォルト: [max_block_size](../../../operations/settings/settings.md#setting-max_block_size)。
- `kafka_flush_interval_ms` — Kafka からのデータをフラッシュするタイムアウト。デフォルト: [stream_flush_interval_ms](../../../operations/settings/settings.md#stream-flush-interval-ms)。
- `kafka_thread_per_consumer` — 各コンシューマに独立したスレッドを提供します。有効にすると、各コンシューマはデータを独立して、並行してフラッシュします（そうでない場合は、複数のコンシューマからの行が集約されて1ブロックを形成します）。デフォルト: `0`。
- `kafka_handle_error_mode` — Kafka エンジンのエラーをどのように対処するかを指定します。可能な値: デフォルト（メッセージの解析に失敗した場合に例外がスローされる）、ストリーム（例外メッセージと未加工のメッセージが仮想カラム `_error` 及び `_raw_message` に保存されます）。
- `kafka_commit_on_select` —  Select クエリが実行されたときにメッセージをコミットします。デフォルト: `false`。
- `kafka_max_rows_per_message` — 行ベースのフォーマット用の 1 つの Kafka メッセージに書き込まれる最大行数。デフォルト : `1`。

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

  CREATE TABLE queue3 (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = Kafka('localhost:9092', 'topic', 'group1')
              SETTINGS kafka_format = 'JSONEachRow',
                       kafka_num_consumers = 4;
```

<details markdown="1">

<summary>非推奨のテーブル作成メソッド</summary>

:::note
新しいプロジェクトではこの方法を使用しないでください。可能であれば、上で説明したメソッドに旧プロジェクトを切り替えてください。
:::

``` sql
Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
      [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_max_block_size,  kafka_skip_broken_messages, kafka_commit_every_batch, kafka_client_id, kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms, kafka_thread_per_consumer, kafka_handle_error_mode, kafka_commit_on_select, kafka_max_rows_per_message]);
```

</details>

:::info
Kafkaテーブルエンジンは[デフォルト値](../../../sql-reference/statements/create/table.md#default_value)を持つカラムをサポートしていません。デフォルト値を持つカラムが必要な場合、マテリアライズドビューのレベルで追加できます（下記参照）。
:::

## 説明 {#description}

提供されたメッセージは自動的に追跡され、グループ内の各メッセージは1回のみカウントされます。データを2回取得したい場合は、別のグループ名でテーブルのコピーを作成します。

グループは柔軟でクラスタ内で同期されます。たとえば、10のトピックとクラスタ内のテーブルのコピーが5つある場合、それぞれのコピーは2つのトピックを取得します。コピーの数が変わると、トピックは自動的にコピー間で再配信されます。詳細は http://kafka.apache.org/intro を参照してください。

`SELECT` はメッセージの読み取りには特に有用ではありません（デバッグを除く）なぜなら各メッセージは一度のみ読み取れるからです。実際には、マテリアライズドビューを使用してリアルタイムスレッドを作成する方が実用的です。以下の手順で行います:

1. エンジンを使用して Kafka コンシューマを作成し、データストリームと見なします。
2. 所望の構造でテーブルを作成します。
3. データをエンジンから変換し、以前に作成したテーブルに投入するマテリアライズドビューを作成します。

`MATERIALIZED VIEW` がエンジンに接続されると、バックグラウンドでデータの収集が始まります。これにより、Kafkaからメッセージを継続的に受信し、 `SELECT` を使用して必要なフォーマットに変換できます。
1つのKafkaテーブルには、好きなだけ多くのマテリアライズドビューを持つことができ、それらはkafkaテーブルから直接データを読み取るのではなく、新しいレコード（ブロック単位）を受信します。この方法で、異なる詳細レベル（集約あり・なし）の複数のテーブルに書き込むことができます。

例:

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
パフォーマンスを向上させるために、受信したメッセージは [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size) のサイズのブロックにまとめられます。ブロックが[stream_flush_interval_ms](../../../operations/settings/settings.md/#stream-flush-interval-ms)ミリ秒以内に形成されない場合、データはブロックが完全であるかどうかに関係なくテーブルにフラッシュされます。

トピックデータの受信を停止するか、変換ロジックを変更するには、マテリアライズドビューをデタッチします:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

`ALTER` を使用してターゲットテーブルを変更したい場合は、ターゲットテーブルとビューからのデータ間の不一致を避けるためにマテリアルビューを無効にすることをお勧めします。

## 設定 {#configuration}

GraphiteMergeTree と同様に、Kafka エンジンは ClickHouse の設定ファイルを使用した拡張設定をサポートしています。使用できる設定キーは2つあります: グローバル（`<kafka>` 以下）と トピックレベル（`<kafka><kafka_topic>` 以下）。最初にグローバル設定が適用され、その後にトピックレベルの設定が適用されます（存在する場合）。

``` xml
  <kafka>
    <!-- Kafka エンジンタイプのすべてのテーブルに関するグローバル設定オプション -->
    <debug>cgrp</debug>
    <statistics_interval_ms>3000</statistics_interval_ms>

    <kafka_topic>
        <name>logs</name>
        <statistics_interval_ms>4000</statistics_interval_ms>
    </kafka_topic>

    <!-- コンシューマの設定 -->
    <consumer>
        <auto_offset_reset>smallest</auto_offset_reset>
        <kafka_topic>
            <name>logs</name>
            <fetch_min_bytes>100000</fetch_min_bytes>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <fetch_min_bytes>50000</fetch_min_bytes>
        </kafka_topic>
    </consumer>

    <!-- プロデューサの設定 -->
    <producer>
        <kafka_topic>
            <name>logs</name>
            <retry_backoff_ms>250</retry_backoff_ms>
        </kafka_topic>

        <kafka_topic>
            <name>stats</name>
            <retry_backoff_ms>400</retry_backoff_ms>
        </kafka_topic>
    </producer>
  </kafka>
```

可能な設定オプションのリストについては、[librdkafka configuration reference](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) を参照してください。ClickHouse 設定では、ドット (`_`) の代わりにアンダースコア (`_`) を使用します。例えば、`check.crcs=true` は `<check_crcs>true</check_crcs>` となります。

### Kerberos サポート {#kafka-kerberos-support}

Kerberosを認識するKafkaを扱うには、`security_protocol` の子要素として `sasl_plaintext` の値を追加します。KerberosチケットがOSによって取得されキャッシュされている限り、それで十分です。
ClickHouse は keytab ファイルを使用してKerberos 資格情報を管理することができます。`sasl_kerberos_service_name`、`sasl_kerberos_keytab`、`sasl_kerberos_principal` の子要素を考慮してください。

例:

``` xml
  <!-- Kerberosを認識するKafka -->
  <kafka>
    <security_protocol>SASL_PLAINTEXT</security_protocol>
	<sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
	<sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
  </kafka>
```

## 仮想カラム {#virtual-columns}

- `_topic` — Kafka トピック。データ型: `LowCardinality(String)`。
- `_key` — メッセージのキー。データ型: `String`。
- `_offset` — メッセージのオフセット。データ型: `UInt64`。
- `_timestamp` — メッセージのタイムスタンプ。データ型: `Nullable(DateTime)`。
- `_timestamp_ms` — メッセージのミリ秒単位のタイムスタンプ。データ型: `Nullable(DateTime64(3))`。
- `_partition` — Kafka トピックのパーティション。データ型: `UInt64`。
- `_headers.name` — メッセージのヘッダキーの配列。データ型: `Array(String)`。
- `_headers.value` — メッセージのヘッダ値の配列。データ型: `Array(String)`。

`kafka_handle_error_mode='stream'` の場合の追加仮想カラム:

- `_raw_message` - 解析に失敗した生メッセージ。データ型: `String`。
- `_error` - 解析失敗時の例外メッセージ。データ型: `String`。

注意: `_raw_message` と `_error` の仮想カラムは、解析中の例外の場合にのみ入力され、メッセージが正常に解析された場合は常に空です。

## データフォーマットのサポート {#data-formats-support}

Kafka エンジンは、ClickHouse でサポートされるすべての [フォーマット](../../../interfaces/formats.md) をサポートします。
1つの Kafka メッセージ内の行数は、フォーマットが行ベースかブロックベースかによって異なります。

- 行ベースのフォーマットでは、1つの Kafka メッセージ内の行数は `kafka_max_rows_per_message` 設定によって制御できます。
- ブロックベースのフォーマットでは、ブロックを小さな部分に分割することはできませんが、1つのブロック内の行数は一般設定 [max_block_size](../../../operations/settings/settings.md#setting-max_block_size) によって制御できます。

## コミット済みオフセットをClickHouse Keeperに格納するエクスペリメンタルエンジン {#experimental-kafka-keeper}

`allow_experimental_kafka_offsets_storage_in_keeper` が有効になっている場合、以下の2つの設定を Kafka テーブルエンジンに指定できます:
 - `kafka_keeper_path` は ClickHouse Keeper のテーブルへのパスを指定します。
 - `kafka_replica_name` は ClickHouse Keeper 内のレプリカ名を指定します。

これらの設定はいずれも単独で指定することはできません。両方の設定が指定されている場合は、新しいエクスペリメンタルな Kafka エンジンが使用されます。この新しいエンジンは、設定済みのオフセットをKafkaに保存することには依存せず、ClickHouse Keeperにそれらを保存します。ただし、Kafkaへのオフセットコミットはまだ試みられますが、テーブルが作成される際にのみそれらのオフセットに依存します。その他の状況（テーブルの再起動またはエラー後の復旧）では、ClickHouse Keeperに保存されたオフセットがメッセージの消費を続けるために使用されます。コミット済みオフセットに加えて、最後のバッチで消費されたメッセージ数も保存され、挿入に失敗した場合、同じメッセージ数が消費されるため、必要に応じて重複除去が可能です。

例:

``` sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/experimental_kafka',
  kafka_replica_name = 'r1'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

または、`uuid` と `replica` マクロを ReplicatedMergeTree と同様に利用するには:

``` sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/{uuid}',
  kafka_replica_name = '{replica}'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

### 既知の制限

新しいエンジンはエクスペリメンタルであり、まだ本番使用には適していません。いくつかの既知の制限があります:
 - 最大の制限点は、エンジンが直接読み取りをサポートしていないことです。マテリアライズドビューを使用したエンジンからの読み取りとエンジンへの書き込みは動作しますが、直接読み取りは動作しません。その結果、すべての直接 `SELECT` クエリは失敗します。
 - テーブルの急速な削除と再作成、または異なるエンジンに同じ ClickHouse Keeper パスを指定することは、問題を引き起こす可能性があります。パスの衝突を避けるために、`kafka_keeper_path` に `{uuid}` を使用することをお勧めします。
 - 繰り返し可能な読み取りを行うためには、1 つのスレッドで複数のパーティションからメッセージを消費することはできません。 一方で、Kafka コンシューマは定期的にポーリングして生かしておく必要があります。これら2つの目的を満たすために、`kafka_thread_per_consumer` が有効になっている場合にのみ複数のコンシューマを作成することが可能で、そうでなければ定期的にコンシューマをポーリングすることに関して問題を回避するのが難しすぎます。
 - 新しいストレージエンジンによって作成されたコンシューマは、[`system.kafka_consumers`](../../../operations/system-tables/kafka_consumers.md) テーブルに表示されません。

**関連情報**

- [仮想カラム](../../../engines/table-engines/index.md#table_engines-virtual_columns)
- [background_message_broker_schedule_pool_size](../../../operations/server-configuration-parameters/settings.md#background_message_broker_schedule_pool_size)
- [system.kafka_consumers](../../../operations/system-tables/kafka_consumers.md)
