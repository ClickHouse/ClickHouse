---
description: 'このエンジンを使用すると、ClickHouse を RabbitMQ と連携できます。'
sidebar_label: 'RabbitMQ'
sidebar_position: 170
slug: /engines/table-engines/integrations/rabbitmq
title: 'RabbitMQ テーブルエンジン'
doc_type: 'guide'
---

このエンジンを使用すると、ClickHouse を [RabbitMQ](https://www.rabbitmq.com) と連携できます。

`RabbitMQ` では、次のことができます。

* データフローをパブリッシュまたはサブスクライブする。
* ストリームを利用可能になり次第処理する。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1],
    name2 [type2],
    ...
) ENGINE = RabbitMQ SETTINGS
    rabbitmq_host_port = 'host:port' [or rabbitmq_address = 'amqp(s)://guest:guest@localhost/vhost'],
    rabbitmq_exchange_name = 'exchange_name',
    rabbitmq_format = 'data_format'[,]
    [rabbitmq_exchange_type = 'exchange_type',]
    [rabbitmq_routing_key_list = 'key1,key2,...',]
    [rabbitmq_secure = 0,]
    [rabbitmq_schema = '',]
    [rabbitmq_num_consumers = N,]
    [rabbitmq_num_queues = N,]
    [rabbitmq_queue_base = 'queue',]
    [rabbitmq_persistent = 0,]
    [rabbitmq_skip_broken_messages = N,]
    [rabbitmq_max_block_size = N,]
    [rabbitmq_flush_interval_ms = N,]
    [rabbitmq_queue_settings_list = 'x-dead-letter-exchange=my-dlx,x-max-length=10,x-overflow=reject-publish',]
    [rabbitmq_queue_consume = false,]
    [rabbitmq_address = '',]
    [rabbitmq_vhost = '/',]
    [rabbitmq_username = '',]
    [rabbitmq_password = '',]
    [rabbitmq_commit_on_select = false,]
    [rabbitmq_max_rows_per_message = 1,]
    [rabbitmq_handle_error_mode = 'default']
```

パラメータ:

* `rabbitmq_host_port` – ホスト:ポート (例: `localhost:5672`) 。
* `rabbitmq_exchange_name` – RabbitMQ exchange の名前。
* `rabbitmq_format` – メッセージのフォーマット。`JSONEachRow` など、SQL の `FORMAT` 関数と同じ表記を使用します。詳しくは、[フォーマット](../../../interfaces/formats.md) セクションを参照してください。

パラメータ:

* `rabbitmq_exchange_type` – RabbitMQ exchange の種類: `direct`、`fanout`、`topic`、`headers`、`consistent_hash`。デフォルト: `fanout`。
* `rabbitmq_routing_key_list` – ルーティングキーをカンマ区切りで指定したリスト。
* `rabbitmq_schema` – フォーマットでスキーマ定義が必要な場合に使用必須のパラメーターです。たとえば、[Cap&#39;n Proto](https://capnproto.org/) では、スキーマファイルへのパスと、ルート `schema.capnp:Message` オブジェクトの名前が必要です。
* `rabbitmq_num_consumers` – テーブルごとのコンシューマー数。1 つのコンシューマーのスループットが不十分な場合は、コンシューマー数を増やしてください。デフォルト: `1`
* `rabbitmq_num_queues` – キューの総数。この数を増やすと、パフォーマンスが大幅に向上する可能性があります。デフォルト: `1`。
* `rabbitmq_queue_base` - キュー名のヒントを指定します。この設定のユースケースについては以下で説明します。
* `rabbitmq_persistent` - 1 (`true`) に設定すると、INSERT クエリの配信モードは 2 に設定されます (メッセージは「永続」としてマークされます) 。デフォルト: `0`。
* `rabbitmq_skip_broken_messages` – block ごとに、スキーマと互換性のないメッセージに対して RabbitMQ メッセージパーサーが許容する数です。`rabbitmq_skip_broken_messages = N` の場合、この engine は parse できない *N* 件の RabbitMQ メッセージをスキップします (1 メッセージは 1 行のデータに相当します) 。デフォルト: `0`。
* `rabbitmq_max_block_size` - RabbitMQ からデータを flush する前に収集する行数。デフォルト: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size)。
* `rabbitmq_flush_interval_ms` - RabbitMQ からデータを flush するまでの timeout。デフォルト: [stream&#95;flush&#95;interval&#95;ms](/ja/operations/settings/settings#stream_flush_interval_ms)。
* `rabbitmq_queue_settings_list` - キュー作成時に RabbitMQ の設定を指定できます。使用可能な設定: `x-max-length`、`x-max-length-bytes`、`x-message-ttl`、`x-expires`、`x-priority`、`x-max-priority`、`x-overflow`、`x-dead-letter-exchange`、`x-queue-type`。`durable` 設定はキューに対して自動的に有効になります。
* `rabbitmq_address` - 接続先アドレスです。この設定または `rabbitmq_host_port` のいずれかを使用してください。
* `rabbitmq_vhost` - RabbitMQ vhost。デフォルト: `'/'`。
* `rabbitmq_queue_consume` - ユーザー定義のキューを使用し、exchange、queues、bindings の宣言を含む RabbitMQ のセットアップを一切行いません。デフォルト: `false`。
* `rabbitmq_username` - RabbitMQ の username。
* `rabbitmq_password` - RabbitMQ の password。
* `reject_unhandled_messages` - エラー時にメッセージを reject します (RabbitMQ に negative acknowledgement を送信します) 。この設定は、`rabbitmq_queue_settings_list` で `x-dead-letter-exchange` が定義されている場合、自動的に有効になります。
* `rabbitmq_commit_on_select` - select クエリ実行時にメッセージを commit します。デフォルト: `false`。
* `rabbitmq_max_rows_per_message` — 行ベースのフォーマットで、1 つの RabbitMQ メッセージに書き込める最大行数。デフォルト: `1`。
* `rabbitmq_empty_queue_backoff_start_ms` — RabbitMQ キュー が空の場合に read を再スケジュールするための backoff の開始点。
* `rabbitmq_empty_queue_backoff_end_ms` — RabbitMQ キュー が空の場合に read を再スケジュールするための backoff の終了点。
* `rabbitmq_empty_queue_backoff_step_ms` — RabbitMQ キュー が空の場合に read を再スケジュールするための backoff の刻み幅。
* `rabbitmq_handle_error_mode` — RabbitMQ エンジンのエラー処理方法。設定可能な値: default (メッセージの parse に失敗した場合は例外が throw されます) 、stream (例外メッセージと生メッセージは仮想カラム `_error` および `_raw_message` に保存されます) 、dead&#95;letter&#95;queue (エラー関連データは system.dead&#95;letter&#95;queue に保存されます) 。

<div id="ssl-connection">
  ### SSL 接続
</div>

接続アドレスには、`rabbitmq_secure = 1` または `amqps` のいずれかを使用します: `rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`。
使用されるライブラリのデフォルト動作では、作成された TLS 接続が十分に安全かどうかは確認されません。証明書が期限切れ、自己署名、欠落、または無効であっても、接続はそのまま許可されます。より厳格な証明書の検証は、今後実装される可能性があります。

rabbitmq 関連の設定に加えて、フォーマット設定を追加することもできます。

例:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64,
    date DateTime
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5,
                            date_time_input_format = 'best_effort';
```

RabbitMQ のサーバー設定は、ClickHouse の設定ファイルに追加する必要があります。

必要な設定:

```xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

追加設定:

```xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

<div id="description">
  ## 説明
</div>

各メッセージは 1 回しか読めないため、メッセージの読み取りに `SELECT` はあまり適していません (デバッグ用途を除く) 。より実用的なのは、[materialized view](../../../sql-reference/statements/create/view.md) を使ってリアルタイムのストリームを作成する方法です。手順は次のとおりです。

1. engine を使って RabbitMQ コンシューマーを作成し、それをデータストリームとして扱います。
2. 必要な structure を持つ table を作成します。
3. engine から data を変換し、事前に作成した table に格納する materialized view を作成します。

`MATERIALIZED VIEW` を engine に関連付けると、バックグラウンドで data の収集が開始されます。これにより、RabbitMQ から継続的にメッセージを受信し、`SELECT` を使って必要なフォーマットに変換できます。
1 つの RabbitMQ table には、必要なだけ materialized view を作成できます。

data は `rabbitmq_exchange_type` と指定した `rabbitmq_routing_key_list` に基づいて振り分けることができます。
1 つの table に設定できる exchange は 1 つだけです。1 つの exchange は複数の table で共有できるため、同時に複数の table へルーティングできます。

Exchange type のオプション:

* `direct` - ルーティングはオプションの完全一致に基づきます。Example の table key list: `key1,key2,key3,key4,key5`。message key はこのいずれかと一致できます。
* `fanout` - オプションに関係なく、すべての table (exchange 名が同じもの) へルーティングします。
* `topic` - ルーティングは、ドット区切りのオプションを持つ pattern に基づきます。Examples: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`。
* `headers` - ルーティングは `key=value` の一致に基づき、設定 `x-match=all` または `x-match=any` を使用します。Example の table key list: `x-match=all,format=logs,type=report,year=2020`。
* `consistent_hash` - data はすべてのバインド済み table 間に均等に分散されます (exchange 名が同じもの) 。この exchange type を使用するには、RabbitMQ plugin `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange` を有効にする必要がある点に注意してください。

設定 `rabbitmq_queue_base` は、次のような場合に使用できます。

* 異なる table 間で キュー を共有し、同じ キュー に対して複数の コンシューマー を登録できるようにするためです。これにより、パフォーマンスが向上します。`rabbitmq_num_consumers` や `rabbitmq_num_queues` の設定を使用する場合、これらのパラメーターが同じであれば、キュー を完全に一致させられます。
* すべてのメッセージを正常に消費できなかった場合に、特定の durable キュー からの読み取りを restore できるようにするためです。特定の 1 つの キュー から consumption を再開するには、`rabbitmq_queue_base` 設定にその名前を指定し、`rabbitmq_num_consumers` と `rabbitmq_num_queues` は指定しないでください (デフォルトは 1) 。特定の table 用に宣言されたすべての キュー から consumption を再開するには、同じ設定 `rabbitmq_queue_base`、`rabbitmq_num_consumers`、`rabbitmq_num_queues` を指定するだけです。デフォルトでは、キュー 名は table ごとに一意になります。
* キュー は durable で auto-delete されないよう宣言されているため、それらを再利用するためです。 (削除する場合は、RabbitMQ CLI tools のいずれかを使用できます。)

パフォーマンス向上のため、受信したメッセージは [max&#95;insert&#95;block&#95;size](/ja/operations/settings/settings#max_insert_block_size) のサイズの blocks にまとめられます。block が [stream&#95;flush&#95;interval&#95;ms](../../../operations/server-configuration-parameters/settings.md) Milliseconds 以内に形成されなかった場合、block が完全でなくても data は table に flush されます。

`rabbitmq_num_consumers` および / または `rabbitmq_num_queues` の設定が `rabbitmq_exchange_type` とともに指定されている場合、次が必要です。

* `rabbitmq-consistent-hash-exchange` plugin を有効にする必要があります。
* 公開されるメッセージの `message_id` プロパティ を指定する必要があります (各 message/batch で一意) 。

INSERT クエリ にはメッセージの メタデータ があり、公開された各メッセージに対して `messageID` と `republished` フラグ (複数回公開された場合は true) が追加されます。これらはメッセージ headers から参照できます。

inserts と materialized view に同じ table を使用しないでください。

Example:

```sql
  CREATE TABLE queue (
    key UInt64,
    value UInt64
  ) ENGINE = RabbitMQ SETTINGS rabbitmq_host_port = 'localhost:5672',
                            rabbitmq_exchange_name = 'exchange1',
                            rabbitmq_exchange_type = 'headers',
                            rabbitmq_routing_key_list = 'format=logs,type=report,year=2020',
                            rabbitmq_format = 'JSONEachRow',
                            rabbitmq_num_consumers = 5;

  CREATE TABLE daily (key UInt64, value UInt64)
    ENGINE = MergeTree() ORDER BY key;

  CREATE MATERIALIZED VIEW consumer TO daily
    AS SELECT key, value FROM queue;

  SELECT key, value FROM daily ORDER BY key;
```

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_exchange_name` - RabbitMQ exchange の名前。Data type: `String`。
* `_channel_id` - メッセージを受信した コンシューマー が宣言されていた ChannelID。Data type: `String`。
* `_delivery_tag` - 受信したメッセージの DeliveryTag。チャネルごとのスコープです。Data type: `UInt64`。
* `_redelivered` - メッセージの `redelivered` flag。Data type: `UInt8`。
* `_message_id` - 受信したメッセージの messageID。メッセージの公開時に設定されていた場合は空になりません。Data type: `String`。
* `_timestamp` - 受信したメッセージの timestamp。メッセージの公開時に設定されていた場合は空になりません。Data type: `UInt64`。

`rabbitmq_handle_error_mode='stream'` の場合は、次の仮想カラムも追加されます:

* `_raw_message` - 正常にパースできなかった生のメッセージ。Data type: `Nullable(String)`。
* `_error` - パース失敗時に発生した Exception メッセージ。Data type: `Nullable(String)`。

Note: `_raw_message` と `_error` の仮想カラムに値が入るのは、パース中に例外が発生した場合のみです。メッセージが正常にパースされた場合、これらは常に `NULL` です。

<div id="caveats">
  ## 注意事項
</div>

テーブル定義では[デフォルトカラム式](/ja/sql-reference/statements/create/table.md/#default_values) (`DEFAULT`、`MATERIALIZED`、`ALIAS` など) を指定できますが、これらは無視されます。代わりに、各カラムにはその型に応じたデフォルト値が設定されます。

<div id="data-formats-support">
  ## データフォーマットのサポート
</div>

RabbitMQ エンジンは、ClickHouse でサポートされているすべての[フォーマット](../../../interfaces/formats.md)に対応しています。
1 つの RabbitMQ メッセージに含められる行数は、そのフォーマットが行ベースかブロックベースかによって異なります。

* 行ベースのフォーマットでは、1 つの RabbitMQ メッセージに含める行数を `rabbitmq_max_rows_per_message` の設定で制御できます。
* ブロックベースのフォーマットでは、ブロックをより小さなパーツに分割することはできませんが、1 つのブロックに含まれる行数は一般設定の [max&#95;block&#95;size](/ja/operations/settings/settings#max_block_size) で制御できます。