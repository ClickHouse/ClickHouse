---
description: 'このエンジンを使用すると、ClickHouse を NATS と統合し、メッセージ subject のパブリッシュやサブスクライブを行ったり、新しいメッセージが利用可能になるたびに処理したりできます。'
sidebar_label: 'NATS'
sidebar_position: 140
slug: /engines/table-engines/integrations/nats
title: 'NATS テーブルエンジン'
doc_type: 'guide'
---

このエンジンを使用すると、ClickHouse を [NATS](https://nats.io/) と統合できます。

`NATS` では、次のことができます。

* メッセージ subject をパブリッシュまたはサブスクライブする。
* 新しいメッセージが利用可能になるたびに処理する。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = NATS SETTINGS
    nats_url = 'host:port',
    nats_subjects = 'subject1,subject2,...',
    nats_format = 'data_format'[,]
    [nats_schema = '',]
    [nats_num_consumers = N,]
    [nats_queue_group = 'group_name',]
    [nats_secure = false,]
    [nats_max_reconnect = N,]
    [nats_reconnect_wait = N,]
    [nats_server_list = 'host1:port1,host2:port2,...',]
    [nats_skip_broken_messages = N,]
    [nats_max_block_size = N,]
    [nats_flush_interval_ms = N,]
    [nats_username = 'user',]
    [nats_password = 'password',]
    [nats_token = 'clickhouse',]
    [nats_credential_file = '/var/nats_credentials',]
    [nats_startup_connect_tries = 5,]
    [nats_max_rows_per_message = 1,]
    [nats_handle_error_mode = 'default']
```

必須パラメータ:

* `nats_url` – host:port (例: `localhost:4222`) .
* `nats_subjects` – NATS テーブルが subscribe/publish する subject の一覧です。`foo.*.bar` や `baz.>` のようなワイルドカード subject をサポートします。
* `nats_format` – メッセージのフォーマットです。`JSONEachRow` など、SQL の `FORMAT` 関数と同じ記法を使用します。詳しくは [Formats](../../../interfaces/formats.md) セクションを参照してください。

任意パラメータ:

* `nats_schema` – フォーマットでスキーマ定義が必要な場合に指定する必要があるパラメータです。たとえば、[Cap&#39;n Proto](https://capnproto.org/) ではスキーマファイルへのパスと、ルート `schema.capnp:Message` object の名前が必要です。
* `nats_stream` – NATS JetStream 内の既存の ストリーム の名前です。
* `nats_consumer_name` – NATS JetStream 内の既存の durable pull コンシューマー の名前です。
* `nats_num_consumers` – テーブルごとの コンシューマー 数です。デフォルト: `1`。NATS core のみで、1 つの コンシューマー の throughput では不足する場合は、コンシューマー 数を増やしてください。
* `nats_queue_group` – NATS subscriber の queue group 名です。デフォルトはテーブル名です。
* `nats_max_reconnect` – 非推奨であり、効果はありません。再接続は `nats_reconnect_wait` timeout で継続的に実行されます。
* `nats_reconnect_wait` – 再接続の各試行の間に sleep する時間 (ミリ秒) です。デフォルト: `2000`。
* `nats_server_list` - 接続先の server 一覧です。NATS クラスターへの接続時に指定できます。
* `nats_skip_broken_messages` - block ごとに許容される、スキーマに適合しない NATS メッセージ数です。デフォルト: `0`。`nats_skip_broken_messages = N` の場合、この engine は parse できない *N* 件の NATS メッセージをスキップします (1 つのメッセージは 1 行のデータに相当します) 。
* `nats_max_block_size` - NATS からデータを flush するために poll(s) で収集される行数です。デフォルト: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size)。
* `nats_flush_interval_ms` - NATS から読み取ったデータを flush するまでの timeout です。デフォルト: [stream&#95;flush&#95;interval&#95;ms](/ja/operations/settings/settings#stream_flush_interval_ms)。
* `nats_username` - NATS の username です。
* `nats_password` - NATS の password です。
* `nats_token` - NATS の auth token です。
* `nats_credential_file` - NATS credentials file へのパスです。
* `nats_startup_connect_tries` - 起動時の接続試行回数です。デフォルト: `5`。
* `nats_max_rows_per_message` — 行ベースのフォーマットで、1 つの NATS メッセージに書き込まれる最大行数です。 (デフォルト: `1`) 。
* `nats_handle_error_mode` — NATS engine での error の処理方法です。設定可能な値: default (メッセージの parse に失敗した場合は exception が throw されます) 、ストリーム (exception message と生メッセージが仮想カラム `_error` および `_raw_message` に保存されます) 。

SSL 接続:

安全な接続には、`nats_secure = 1` を使用します。
証明書の検証は `CLICKHOUSE_NATS_TLS_SECURE` 環境変数で制御されます。
証明書の有効期限が切れている、自己署名である、見つからない、またはその他の理由で無効な場合は、`CLICKHOUSE_NATS_TLS_SECURE=0` を設定して検証を無効にします。

NATS テーブルへの書き込み:

テーブルが 1 つの subject からのみ読み取る場合、INSERT はすべて同じ subject にパブリッシュされます。
ただし、テーブルが複数の subjects から読み取る場合は、どの subject にパブリッシュするかを指定する必要があります。
そのため、複数の subjects を持つテーブルに挿入する場合は常に、`stream_like_engine_insert_queue` の設定が必要です。
テーブルが読み取る subjects のうち 1 つを選択し、そこにデータをパブリッシュできます。例:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1,subject2',
             nats_format = 'JSONEachRow';

  INSERT INTO queue
  SETTINGS stream_like_engine_insert_queue = 'subject2'
  VALUES (1, 1);
```

また、NATS 関連の設定に加えて、フォーマット設定を追加することもできます。

例:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';
```

NATSサーバーの設定は、ClickHouseの設定ファイルを使用して追加できます。
具体的には、NATSエンジンのパスワードを追加できます:

```xml
<nats>
    <user>click</user>
    <password>house</password>
    <token>clickhouse</token>
</nats>
```

<div id="description">
  ## 説明
</div>

メッセージの読み取りに `SELECT` はあまり適していません (デバッグ時を除く) 。これは、各メッセージを読み取れるのが1回だけだからです。より実用的なのは、[materialized view](../../../sql-reference/statements/create/view.md) を使ってリアルタイム処理の流れを作ることです。そのためには、次のようにします。

1. エンジンを使って NATS コンシューマーを作成し、それをデータストリームとして扱います。
2. 必要な構造を持つテーブルを作成します。
3. エンジンからのデータを変換し、あらかじめ作成したテーブルに格納する materialized view を作成します。

`MATERIALIZED VIEW` がエンジンに接続されると、バックグラウンドでデータの収集を開始します。これにより、NATS からメッセージを継続的に受信し、`SELECT` を使って必要なフォーマットに変換できます。
1つの NATS テーブルには、必要な数だけ materialized view を作成できます。これらはテーブルから直接データを読み取るのではなく、新しいレコードをブロック単位で受け取ります。そのため、詳細度の異なる複数のテーブルに書き込めます (グループ化・集約する場合としない場合の両方) 。

例:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = NATS
    SETTINGS nats_url = 'localhost:4444',
             nats_subjects = 'subject1',
             nats_format = 'JSONEachRow',
             date_time_input_format = 'best_effort';

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

ストリームデータの受信を停止するか、変換ロジックを変更するには、materialized view を detach します:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

`ALTER` を使用してターゲットテーブルを変更する場合は、ターゲットテーブルとビューからのデータとの不整合を避けるため、マテリアライズドビューを無効にすることを推奨します。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_subject` - NATS メッセージの subject。データ型: `String`。

`nats_handle_error_mode='stream'` の場合は、次の仮想カラムも追加されます。

* `_raw_message` - 正常にパースできなかった生のメッセージ。データ型: `Nullable(String)`。
* `_error` - パース失敗時に発生した Exception メッセージ。データ型: `Nullable(String)`。

注意: `_raw_message` と `_error` の仮想カラムに値が入るのは、パース中に例外が発生した場合のみです。メッセージが正常にパースされた場合、これらは常に `NULL` です。

<div id="data-formats-support">
  ## データフォーマットのサポート
</div>

NATS engine は、ClickHouse でサポートされているすべての[フォーマット](../../../interfaces/formats.md)に対応しています。
1 つの NATS メッセージに含まれる行数は、そのフォーマットが行ベースかブロックベースかによって異なります。

* 行ベースのフォーマットでは、1 つの NATS メッセージに含まれる行数は `nats_max_rows_per_message` の設定で制御できます。
* ブロックベースのフォーマットでは、ブロックをより小さなパーツに分割することはできませんが、1 つのブロックに含まれる行数は一般設定の [max&#95;block&#95;size](/ja/operations/settings/settings#max_block_size) で制御できます。

<div id="using-jetstream">
  ## JetStream の使用
</div>

NATS JetStream で NATS engine を使用する前に、NATS のストリームと durable pull コンシューマー を作成する必要があります。これには、たとえば [NATS CLI](https://github.com/nats-io/natscli) パッケージの `nats` ユーティリティを使用できます。

<details>
  <summary>ストリームの作成</summary>

  ```bash
  $ nats stream add
  ? Stream Name stream_name
  ? Subjects stream_subject
  ? Storage file
  ? Replication 1
  ? Retention Policy Limits
  ? Discard Policy Old
  ? Stream Messages Limit -1
  ? Per Subject Messages Limit -1
  ? Total Stream Size -1
  ? Message TTL -1
  ? Max Message Size -1
  ? Duplicate tracking time window 2m0s
  ? Allow message Roll-ups No
  ? Allow message deletion Yes
  ? Allow purging subjects or the entire stream Yes
  Stream stream_name was created

  Information for Stream stream_name created 2025-10-03 14:12:51

                  Subjects: stream_subject
                  Replicas: 1
                   Storage: File

  Options:

                 Retention: Limits
           Acknowledgments: true
            Discard Policy: Old
          Duplicate Window: 2m0s
                Direct Get: true
         Allows Msg Delete: true
              Allows Purge: true
    Allows Per-Message TTL: false
            Allows Rollups: false

  Limits:

          Maximum Messages: unlimited
       Maximum Per Subject: unlimited
             Maximum Bytes: unlimited
               Maximum Age: unlimited
      Maximum Message Size: unlimited
         Maximum Consumers: unlimited

  State:

                  Messages: 0
                     Bytes: 0 B
            First Sequence: 0
             Last Sequence: 0
          Active Consumers: 0
  ```
</details>

<details>
  <summary>durable pull コンシューマー の作成</summary>

  ```bash
  $ nats consumer add
  ? Select a Stream stream_name
  ? Consumer name consumer_name
  ? Delivery target (empty for Pull Consumers) 
  ? Start policy (all, new, last, subject, 1h, msg sequence) all
  ? Acknowledgment policy explicit
  ? Replay policy instant
  ? Filter Stream by subjects (blank for all) 
  ? Maximum Allowed Deliveries -1
  ? Maximum Acknowledgments Pending 0
  ? Deliver headers only without bodies No
  ? Add a Retry Backoff Policy No
  Information for Consumer stream_name > consumer_name created 2025-10-03T14:13:51+03:00

  Configuration:

                      Name: consumer_name
                 Pull Mode: true
            Deliver Policy: All
                Ack Policy: Explicit
                  Ack Wait: 30.00s
             Replay Policy: Instant
           Max Ack Pending: 1,000
         Max Waiting Pulls: 512

  State:

    Last Delivered Message: Consumer sequence: 0 Stream sequence: 0
      Acknowledgment Floor: Consumer sequence: 0 Stream sequence: 0
          Outstanding Acks: 0 out of maximum 1,000
      Redelivered Messages: 0
      Unprocessed Messages: 0
             Waiting Pulls: 0 of maximum 512
  ```
</details>

ストリームと durable pull コンシューマー を作成したら、NATS engine を使用してテーブルを作成できます。これを行うには、`nats&#95;stream`、`nats&#95;consumer&#95;name`、`nats&#95;subjects` を設定する必要があります:

```SQL
CREATE TABLE nats_jet_stream (
    key UInt64,
    value UInt64
  ) ENGINE NATS 
    SETTINGS  nats_url = 'localhost:4222',
              nats_stream = 'stream_name',
              nats_consumer_name = 'consumer_name',
              nats_subjects = 'stream_subject',
              nats_format = 'JSONEachRow';
```