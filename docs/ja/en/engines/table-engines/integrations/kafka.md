---
description: 'Kafka テーブルエンジンは Apache Kafka と連携して使用でき、データフローのパブリッシュやサブスクライブ、耐障害性のあるストレージの構成、利用可能になったストリームの処理を行えます。'
sidebar_label: 'Kafka'
sidebar_position: 110
slug: /engines/table-engines/integrations/kafka
title: 'Kafka テーブルエンジン'
keywords: ['Kafka', 'テーブルエンジン']
doc_type: 'guide'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="kafka-table-engine">
  # Kafka テーブルエンジン
</div>

:::tip
ClickHouse Cloud をご利用の場合は、代わりに [ClickPipes](/ja/integrations/clickpipes) を使用することをお勧めします。ClickPipes は、プライベートネットワーク接続、インジェストとクラスターリソースの個別のスケーリング、さらに Kafka データを ClickHouse にストリーミングするための包括的な監視をネイティブでサポートしています。
:::

* データフローを公開またはサブスクライブできます。
* フォールトトレラントなストレージを構成できます。
* ストリームを利用可能になり次第処理できます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
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
    [kafka_security_protocol = '',]
    [kafka_sasl_mechanism = '',]
    [kafka_sasl_username = '',]
    [kafka_sasl_password = '',]
    [kafka_autodetect_client_rack = '',]
    [kafka_schema = '',]
    [kafka_num_consumers = N,]
    [kafka_max_block_size = 0,]
    [kafka_skip_broken_messages = N,]
    [kafka_commit_every_batch = 0,]
    [kafka_client_id = '',]
    [kafka_poll_timeout_ms = 0,]
    [kafka_poll_max_batch_size = 0,]
    [kafka_flush_interval_ms = 0,]
    [kafka_consumer_reschedule_ms = 0,]
    [kafka_thread_per_consumer = 0,]
    [kafka_handle_error_mode = 'default',]
    [kafka_commit_on_select = false,]
    [kafka_consumer_acquire_timeout_ms = 30000,]
    [kafka_max_rows_per_message = 1,]
    [kafka_compression_codec = '',]
    [kafka_compression_level = -1];
```

必須パラメータ:

* `kafka_broker_list` — ブローカーのカンマ区切りリストです (例: `localhost:9092`) 。
* `kafka_topic_list` — Kafkaトピックのリストです。
* `kafka_group_name` — Kafkaコンシューマーのグループです。読み取りオフセットは各グループごとに個別に追跡されます。クラスター内でメッセージが重複しないようにするには、すべてで同じグループ名を使用してください。
* `kafka_format` — メッセージのフォーマットです。`JSONEachRow` など、SQL `FORMAT` 関数と同じ表記を使用します。詳細については、[フォーマット](../../../interfaces/formats.md) セクションを参照してください。

オプションのパラメータ:

* `kafka_security_protocol` - broker との通信に使用するプロトコル。設定可能な値: `plaintext`, `ssl`, `sasl_plaintext`, `sasl_ssl`。
* `kafka_sasl_mechanism` - 認証に使用する SASL メカニズム。設定可能な値: `GSSAPI`, `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `AWS_MSK_IAM`。
* `kafka_aws_region` - MSK IAM 認証用の AWS リージョン。指定しない場合は broker アドレスから自動検出されます。PrivateLink の別名や、リージョン情報を含まないカスタム DNS ホスト名を使用する場合は、明示的に指定してください。デフォルト: 空 (自動検出) 。
* `kafka_sasl_username` - `PLAIN` および `SASL-SCRAM-..` メカニズムで使用する SASL ユーザー名。
* `kafka_sasl_password` - `PLAIN` および `SASL-SCRAM-..` メカニズムで使用する SASL パスワード。
* `kafka_schema` — フォーマットでスキーマ定義が必要な場合に必須のパラメーター。たとえば、[Cap&#39;n Proto](https://capnproto.org/) では、schema file へのパスとルート `schema.capnp:Message` オブジェクト名が必要です。
* `kafka_schema_registry_skip_bytes` — エンベロープヘッダー付きのスキーマレジストリを使用する場合 (例: 19 バイトのエンベロープを含む AWS Glue Schema Registry) に、各メッセージの先頭からスキップするバイト数。範囲: `[0, 255]`。デフォルト: `0`。
* `kafka_num_consumers` — テーブルごとのコンシューマー数。1 つのコンシューマーのスループットが不十分な場合は、より多くのコンシューマーを指定してください。コンシューマー総数はトピック内のパーティション数を超えてはいけません。各パーティションに割り当てられるコンシューマーは 1 つだけだからです。また、ClickHouse をデプロイしているサーバーの物理コア数を超えてはいけません。デフォルト: `1`。
* `kafka_max_block_size` — poll における最大バッチサイズ (メッセージ数) 。デフォルト: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size)。
* `kafka_skip_broken_messages` — block ごとに許容される、スキーマ非互換な Kafka メッセージ数。`kafka_skip_broken_messages = N` の場合、engine はパースできない Kafka メッセージを *N* 件スキップします (1 メッセージは 1 行のデータに相当します) 。デフォルト: `0`。
* `kafka_commit_every_batch` — ブロック全体を書き込んだ後に 1 回だけ commit する代わりに、消費および処理された各バッチごとに commit します。デフォルト: `0`。
* `kafka_client_id` — Client 識別子。デフォルトでは空です。
* `kafka_poll_timeout_ms` — Kafka から 1 回 poll する際のタイムアウト。デフォルト: [stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms)。
* `kafka_poll_max_batch_size` — 1 回の Kafka poll で取得するメッセージ数の最大値。デフォルト: [max&#95;block&#95;size](/ja/operations/settings/settings#max_block_size)。
* `kafka_flush_interval_ms` — Kafka からデータを flush する際のタイムアウト。デフォルト: [stream&#95;flush&#95;interval&#95;ms](/ja/operations/settings/settings#stream_flush_interval_ms)。
* `kafka_consumer_reschedule_ms` — Kafka の stream processing が停止している場合 (たとえば、消費可能なメッセージがない場合) の再スケジュール間隔。この設定は、コンシューマーが poll を再試行するまでの待機時間を制御します。`kafka_consumers_pool_ttl_ms` を超えてはいけません。デフォルト: `500` Milliseconds。
* `kafka_thread_per_consumer` — 各コンシューマーに独立した thread を割り当てます。有効にすると、各コンシューマーはデータを独立して並列に flush します (無効な場合は、複数のコンシューマーからの行がまとめられて 1 つの block が形成されます) 。デフォルト: `0`。
* `kafka_handle_error_mode` — Kafka エンジンでのエラー処理方法。設定可能な値: default (メッセージのパースに失敗した場合、例外がスローされます) 、stream (例外メッセージと生のメッセージが仮想カラム `_error` および `_raw_message` に保存されます) 、dead&#95;letter&#95;queue (エラー関連データが system.dead&#95;letter&#95;queue に保存されます) 。
* `kafka_commit_on_select` — `SELECT` クエリ実行時にメッセージを commit します。デフォルト: `false`。
* `kafka_consumer_acquire_timeout_ms` — `Kafka2` テーブルに対する直接 `SELECT` クエリ (Keeper ベースのオフセット保存あり) で Kafka コンシューマーを取得する際のタイムアウト (ミリ秒) 。同じテーブルに対して複数の同時実行の直接 `SELECT` クエリが実行されている場合、それぞれのクエリはコンシューマーが利用可能になるまで待機する必要があります。このタイムアウトは、クエリがそれぞれ異なるコンシューマーの部分集合を保持している場合のデッドロックを防ぎます。デフォルト: `30000`。
* `kafka_max_rows_per_message` — 行ベースのフォーマットで、1 つの Kafka メッセージに書き込まれる行の最大数です。デフォルト: `1`。
* `kafka_autodetect_client_rack` — 最寄りの Kafka レプリカを優先するために、`librdkafka` の `client.rack` パラメータを自動設定します。
  サポートされるソース:
  AWS IMDSv2 の availability zone ID には `AWS_ZONE_ID` (例: `euc1-az1`) ;
  AWS IMDSv2 の availability zone 名には `AWS_ZONE_NAME` (例: `eu-central-1a`) ;
  GCP metadata service の zone には `GCP_ZONE` (例: `europe-central2-a`) ;
  Cloud のメタデータまたは設定に基づく場合がある ClickHouse の内部検出を使用するには `CLICKHOUSE`;
  `AWS_ZONE_NAME` を試し、その後 `GCP_ZONE` を試すには `AWS_ZONE_NAME_THEN_GCP_ZONE`。
  デフォルト: 空文字列 (無効) 。
  ヒント: 環境によって availability zone のフォーマットは異なります。Amazon MSK では通常 zone ID が使われるため、`AWS_ZONE_ID` を優先してください。Confluent Cloud では通常 zone 名が使われるため、`AWS_ZONE_NAME` を優先してください。判断に迷う場合は、`AWS_ZONE_NAME_THEN_GCP_ZONE` を使用するか、クラスターの `broker.rack` の値を確認してください。
  注: Kafka broker では、`broker.rack` と `replica.selector.class=org.apache.kafka.common.replica.RackAwareReplicaSelector` を設定しておく必要があります。
* `kafka_compression_codec` — メッセージ生成に使用する圧縮 codec です。サポート対象: 空文字列、`none`、`gzip`、`snappy`、`lz4`、`zstd`。空文字列の場合、圧縮 codec は table では設定されず、設定ファイルの値または `librdkafka` のデフォルト値が使用されます。デフォルト: 空文字列。
* `kafka_compression_level` — kafka&#95;compression&#95;codec で選択したアルゴリズムの圧縮レベルパラメータです。値を大きくすると、CPU usage は増えますが、より高い圧縮率が得られます。使用可能な範囲はアルゴリズムによって異なります: `gzip` は `[0-9]`、`lz4` は `[0-12]`、`snappy` は `0` のみ、`zstd` は `[0-12]`、`-1` は codec 依存のデフォルト圧縮レベルです。デフォルト: `-1`。
* `kafka_map_virtual_columns_on_write` — 有効にすると、table schema 内の特別な名前 `_key`、`_timestamp`、`_headers.name`、`_headers.value` を持つカラムは、`INSERT` 時に対応する Kafka メッセージのメタデータにマッピングされ、payload から除外されます。[Mapping columns to Kafka message metadata](#mapping-columns-to-kafka-message-metadata) を参照してください。デフォルト: `false`。

例:

```sql
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
  <summary>テーブル作成の非推奨の方法</summary>

  :::note
  新規プロジェクトではこの方法を使用しないでください。可能であれば、既存のプロジェクトも上記の方法に切り替えてください。
  :::

  ```sql
  Kafka(kafka_broker_list, kafka_topic_list, kafka_group_name, kafka_format
        [, kafka_row_delimiter, kafka_schema, kafka_num_consumers, kafka_max_block_size,  kafka_skip_broken_messages, kafka_commit_every_batch, kafka_client_id, kafka_poll_timeout_ms, kafka_poll_max_batch_size, kafka_flush_interval_ms, kafka_consumer_reschedule_ms, kafka_thread_per_consumer, kafka_handle_error_mode, kafka_commit_on_select, kafka_max_rows_per_message]);
  ```
</details>

:::info
Kafka テーブルエンジン は、[デフォルト値](/ja/sql-reference/statements/create/table#default_values)を持つカラムをサポートしていません。デフォルト値を持つカラムが必要な場合は、materialized view レベルで追加できます (以下を参照) 。
:::

<div id="description">
  ## 説明
</div>

配信済みメッセージは自動的に追跡されるため、グループ内の各メッセージは 1 回だけカウントされます。データを 2 回取得したい場合は、別のグループ名でテーブルのコピーを作成してください。

グループは柔軟で、クラスター全体で同期されます。たとえば、10 個のトピックとクラスター内に 5 個のテーブルのコピーがある場合、各コピーは 2 個のトピックを受け取ります。コピー数が変わると、トピックは各コピーに自動的に再分配されます。詳細は http://kafka.apache.org/intro を参照してください。

各 Kafkaトピック には専用のコンシューマグループを割り当て、topic とグループが 1 対 1 の排他的な対応関係になるようにすることを推奨します。これは、特に topic が動的に作成・削除される可能性がある環境 (例: テストやステージング) で重要です。

メッセージの読み取りに `SELECT` はあまり適していません (デバッグを除く) 。これは、各メッセージを読み取れるのが 1 回だけだからです。より実用的なのは、materialized view を使ってリアルタイムの処理フローを作成することです。これを行うには、次の手順に従います。

1. engine を使用して Kafka コンシューマを作成し、それをデータストリームとして扱います。
2. 必要な構造を持つテーブルを作成します。
3. engine からデータを変換し、事前に作成したテーブルに格納する materialized view を作成します。

`MATERIALIZED VIEW` を engine に関連付けると、バックグラウンドでデータの収集を開始します。これにより、Kafka から継続的にメッセージを受信し、それらを `SELECT` を使って必要なフォーマットに変換できます。
1 つの Kafka table には、必要な数だけ materialized view を持たせることができます。これらは Kafka table から直接データを読み取るのではなく、新しいレコードを (block 単位で) 受け取ります。この仕組みにより、詳細度の異なる複数のテーブルに書き込むことができます (グループ化・aggregation あり／なし) 。

Example:

```sql
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
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM queue GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

パフォーマンス向上のため、受信したメッセージは [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size) のサイズごとにブロックにまとめられます。ブロックが [stream&#95;flush&#95;interval&#95;ms](/ja/operations/settings/settings#stream_flush_interval_ms) ミリ秒以内に形成されなかった場合は、ブロックが完全でなくても、データはテーブルにフラッシュされます。

トピックデータの受信を停止する場合や変換ロジックを変更する場合は、materialized view をデタッチします。

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

`ALTER` を使用してターゲットテーブルを変更する場合は、ターゲットテーブルとビューから書き込まれるデータとの不整合を避けるため、materialized view を無効にすることを推奨します。

<div id="configuration">
  ## 設定
</div>

GraphiteMergeTree と同様に、Kafka エンジンでは ClickHouse の設定ファイルを使って拡張設定を行えます。使用できる設定キーは 2 種類あり、グローバル (`<kafka>` 配下) とトピックレベル (`<kafka><kafka_topic>` 配下) です。まずグローバル設定が適用され、次にトピックレベルの設定が適用されます (存在する場合) 。

```xml
  <kafka>
    <!-- Global configuration options for all tables of Kafka engine type -->
    <debug>cgrp</debug>
    <statistics_interval_ms>3000</statistics_interval_ms>

    <kafka_topic>
        <name>logs</name>
        <statistics_interval_ms>4000</statistics_interval_ms>
    </kafka_topic>

    <!-- Settings for consumer -->
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

    <!-- Settings for producer -->
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

利用可能な設定オプションの一覧は、[librdkafka configuration reference](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)を参照してください。ClickHouse の設定では、ドット (`.`) の代わりにアンダースコア (`_`) を使用してください。たとえば、`check.crcs=true` は `<check_crcs>true</check_crcs>` になります。

<div id="kafka-aws-msk-iam">
  ### AWS MSK IAM 認証
</div>

:::note
AWS MSK IAM 認証を使用するには、ClickHouse が AWS S3 サポートを有効にしてビルドされている必要があります。
:::

AWS MSK は IAM ベースの認証をサポートしており、個別のユーザー名とパスワードを管理する代わりに、AWS 認証情報を使用して Kafka クラスターに接続できます。

**基本設定:**

テーブル設定で `kafka_sasl_mechanism = 'AWS_MSK_IAM'` を設定します。

```sql
CREATE TABLE msk_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'b-1.mycluster.kafka.us-east-1.amazonaws.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM';
```

AWSリージョンは、ブローカーのエンドポイントからパターンマッチングによって自動的に抽出されます。

* Provisioned MSK: `b-X.cluster.kafka.<region>.amazonaws.com:9098`
* Serverless MSK: `boot-X.kafka-serverless.<region>.amazonaws.com:9098`
* VPC Endpoint: `vpce-X.kafka.<region>.vpce.amazonaws.com:9098`

**AWS 認証情報:**

認証情報は、`~/.aws/credentials` と `~/.aws/config` (AWS プロファイルファイル) が存在する場合、常にそこから読み込まれます。さらに、EC2 インスタンスプロファイル、環境変数 (`AWS_ACCESS_KEY_ID` など) 、ECS タスクロール、およびその他の自動認証情報ソースも有効にするには、サーバー設定に次を追加します。

```xml
<kafka>
  <use_environment_credentials>true</use_environment_credentials>
</kafka>
```

この設定を行えるのはサーバー管理者のみです。デフォルト: `false`。

**PrivateLink とカスタム DNS:**

リージョン情報を含まない PrivateLink の別名またはカスタム DNS ホスト名を使用する場合は、AWS のリージョンを明示的に指定してください。

```sql
CREATE TABLE msk_privatelink_queue (
    timestamp UInt64,
    level String,
    message String
) ENGINE = Kafka()
SETTINGS
    kafka_broker_list = 'my-privatelink-alias.internal.example.com:9098',
    kafka_topic_list = 'my-topic',
    kafka_group_name = 'my-group',
    kafka_format = 'JSONEachRow',
    kafka_sasl_mechanism = 'AWS_MSK_IAM',
    kafka_aws_region = 'us-east-1';
```

**IAM権限:**

コンシューマー権限 (メッセージの読み取り用) :

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:ReadData",
      "kafka-cluster:AlterGroup",
      "kafka-cluster:DescribeGroup"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:group/CLUSTER_NAME/CONSUMER_GROUP/*"
    ]
  }]
}
```

プロデューサー権限 (メッセージ書き込み用) :

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
      "kafka-cluster:Connect",
      "kafka-cluster:DescribeTopic",
      "kafka-cluster:WriteData"
    ],
    "Resource": [
      "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/*",
      "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/TOPIC_NAME/*"
    ]
  }]
}
```

<div id="kafka-kerberos-support">
  ### Kerberos のサポート
</div>

Kerberos 対応の Kafka を扱うには、`security_protocol` 子要素を追加し、その値を `sasl_plaintext` に設定します。Kerberos のチケット認可チケットが OS の機能によって取得され、cache されていれば、それで十分です。
ClickHouse は、keytab ファイルを使用して Kerberos の認証情報を維持できます。`sasl_kerberos_service_name`、`sasl_kerberos_keytab`、`sasl_kerberos_principal` の各子要素を検討してください。

例:

```xml
<!-- Kerberos-aware Kafka -->
<kafka>
  <security_protocol>SASL_PLAINTEXT</security_protocol>
  <sasl_kerberos_keytab>/home/kafkauser/kafkauser.keytab</sasl_kerberos_keytab>
  <sasl_kerberos_principal>kafkauser/kafkahost@EXAMPLE.COM</sasl_kerberos_principal>
</kafka>
```

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_topic` — Kafkaトピック。データ型: `LowCardinality(String)`。
* `_key` — メッセージのキー。データ型: `String`。
* `_offset` — メッセージのオフセット。データ型: `UInt64`。
* `_timestamp` — メッセージのタイムスタンプ。データ型: `Nullable(DateTime)`。
* `_timestamp_ms` — メッセージのミリ秒単位のタイムスタンプ。データ型: `Nullable(DateTime64(3))`。
* `_partition` — Kafkaトピックのパーティション。データ型: `UInt64`。
* `_headers.name` — メッセージのヘッダーキーの配列。データ型: `Array(String)`。
* `_headers.value` — メッセージのヘッダー値の配列。データ型: `Array(String)`。

`kafka_handle_error_mode='stream'` の場合は、次の仮想カラムも追加されます:

* `_raw_message` - 正常にパースできなかった生のメッセージ。データ型: `String`。
* `_error` - パース失敗時に発生した例外メッセージ。データ型: `String`。

注: 仮想カラム `_raw_message` と `_error` が補完されるのは、パース中に例外が発生した場合のみです。メッセージが正常にパースされた場合、これらは常に空です。

<div id="mapping-columns-to-kafka-message-metadata">
  ## カラムを Kafka メッセージのメタデータにマッピングする
</div>

`INSERT INTO` でメッセージを生成すると、Kafka エンジンは、テーブルにそれらのカラムが存在する場合、常に `_key` という名前のカラム (型は `String`) を Kafka メッセージキーとして、`_timestamp` という名前のカラム (型は `DateTime`) を Kafka メッセージのタイムスタンプとして使用します。デフォルトでは、これらのカラムも他のカラムとともに、生成されるメッセージの payload に含まれます。

`kafka_map_virtual_columns_on_write = 1` を使用すると、動作が変わります。

* `_key` (型は `String`) — Kafka メッセージキーにマッピングされます。
* `_timestamp` (型は `DateTime`) — Kafka メッセージのタイムスタンプにマッピングされます。
* `_headers.name` (型は `Array(String)`) および `_headers.value` (型は `Array(String)`) — Kafka メッセージのヘッダーにマッピングされます。各ペア `(_headers.name[i], _headers.value[i])` は 1 つの Kafka ヘッダーになります。`_headers.name` と `_headers.value` は `_headers` という Nested のプレフィックスを共有しているため、ClickHouse では各行で両方の配列のサイズが同じである必要があります。

これらの名前を持つカラムは、型が上記のものと一致する場合にのみ **メッセージの payload から除外されます**。それ以外の場合は payload に残るため、たまたま無関係なデータにこれらの名前を使っているスキーマでも引き続き動作します。

例:

```sql
CREATE TABLE kafka_out
(
    event_json String,
    `_key` String,
    `_timestamp` DateTime,
    `_headers.name` Array(String),
    `_headers.value` Array(String)
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'broker:9092',
    kafka_topic_list = 'events',
    kafka_group_name = 'events-producer',
    kafka_format = 'JSONEachRow',
    kafka_map_virtual_columns_on_write = 1;

INSERT INTO kafka_out VALUES
    ('{"a":1}', 'session-42', now(), ['source', 'trace_id'], ['api', 'abc-123']);
```

生成された Kafka メッセージには、ペイロード `{"event_json":"{\"a\":1}"}`、キー `session-42`、現在のタイムスタンプ、そして 2 つのヘッダー `source=api` と `trace_id=abc-123` が含まれます。

<div id="data-formats-support">
  ## データフォーマットの対応
</div>

Kafka エンジンは、ClickHouse がサポートするすべての[フォーマット](../../../interfaces/formats.md)に対応しています。
1 つの Kafka メッセージに含まれる行数は、そのフォーマットが行ベースかブロックベースかによって異なります。

* 行ベースのフォーマットでは、1 つの Kafka メッセージに含まれる行数を `kafka_max_rows_per_message` 設定で制御できます。
* ブロックベースのフォーマットでは、ブロックをより小さなパーツに分割することはできませんが、1 つのブロックに含まれる行数は一般設定の [max&#95;block&#95;size](/ja/operations/settings/settings#max_block_size) で制御できます。

<div id="engine-to-store-committed-offsets-in-clickhouse-keeper">
  ## ClickHouse Keeper にコミット済みオフセットを保存するエンジン
</div>

<ExperimentalBadge />

`allow_experimental_kafka_offsets_storage_in_keeper` が有効な場合、Kafka テーブルエンジンに対してさらに 2 つの設定を指定できます。

* `kafka_keeper_path` は ClickHouse Keeper 内のテーブルへのパスを指定します
* `kafka_replica_name` は ClickHouse Keeper 内のレプリカ名を指定します

これらの設定は、両方とも指定するか、どちらも指定しないかのいずれかでなければなりません。両方を指定すると、新しい実験的な Kafka エンジンが使用されます。この新しいエンジンは、コミット済みオフセットを Kafka に保存する方式には依存せず、代わりに ClickHouse Keeper に保存します。オフセットの commit は引き続き Kafka に対して試みられますが、それらのオフセットに依存するのはテーブル作成時のみです。それ以外の状況 (テーブルの再起動時や、何らかの error からの復旧後など) では、ClickHouse Keeper に保存されたオフセットが、メッセージ消費を継続するためのオフセットとして使用されます。コミット済みオフセットに加えて、直前の Batch で消費したメッセージ数も保存されるため、insert が失敗した場合でも同じ数のメッセージが消費され、必要に応じて重複排除を有効にできます。

例:

```sql
CREATE TABLE experimental_kafka (key UInt64, value UInt64)
ENGINE = Kafka('localhost:19092', 'my-topic', 'my-consumer', 'JSONEachRow')
SETTINGS
  kafka_keeper_path = '/clickhouse/{database}/{uuid}',
  kafka_replica_name = '{replica}'
SETTINGS allow_experimental_kafka_offsets_storage_in_keeper=1;
```

<div id="known-limitations">
  ### 既知の制限事項
</div>

この新しいエンジンは実験段階のため、まだ本番環境で使用できる状態ではありません。現時点で、実装にはいくつかの既知の制限があります。

* テーブルを短時間のうちに削除して再作成したり、異なるエンジンに同じ ClickHouse Keeper パスを指定したりすると、問題が発生する可能性があります。ベストプラクティスとして、パスの競合を避けるために `kafka_keeper_path` で `{uuid}` を使用できます。
* 再現可能な読み取りを実現するには、1 つのスレッドで複数のパーティションからメッセージを消費することはできません。一方で、Kafka コンシューマーを生かしておくには、定期的にポーリングする必要があります。この 2 つの要件を満たすため、複数のコンシューマーを作成できるのは `kafka_thread_per_consumer` が有効な場合のみにしています。そうしないと、コンシューマーを定期的にポーリングする際の問題を避けるのが非常に複雑になるためです。

**関連項目**

* [仮想カラム](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* [background&#95;message&#95;broker&#95;schedule&#95;pool&#95;size](/ja/operations/server-configuration-parameters/settings#background_message_broker_schedule_pool_size)
* [system.kafka&#95;consumers](../../../operations/system-tables/kafka_consumers.md)