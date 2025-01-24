---
slug: /ja/engines/table-engines/integrations/rabbitmq
sidebar_position: 170
sidebar_label: RabbitMQ
---

# RabbitMQ エンジン

このエンジンを使用すると、ClickHouse を [RabbitMQ](https://www.rabbitmq.com) と統合することができます。

`RabbitMQ` を使用すると、以下のことが可能になります：

- データフローのパブリッシュまたはサブスクライブ
- ストリームが利用可能になると処理

## テーブルの作成 {#creating-a-table}

``` sql
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
    [rabbitmq_deadletter_exchange = 'dl-exchange',]
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

必須パラメータ:

- `rabbitmq_host_port` – ホスト:ポート（例: `localhost:5672`）。
- `rabbitmq_exchange_name` – RabbitMQ エクスチェンジ名。
- `rabbitmq_format` – メッセージ形式。SQL の `FORMAT` 関数の記法と同じ形式を使用し、例えば `JSONEachRow`。詳細は、[フォーマット](../../../interfaces/formats.md)セクションを参照してください。

任意パラメータ:

- `rabbitmq_exchange_type` – RabbitMQ エクスチェンジの種類: `direct`, `fanout`, `topic`, `headers`, `consistent_hash`。デフォルト: `fanout`。
- `rabbitmq_routing_key_list` – 経路指定キーのカンマ区切りリスト。
- `rabbitmq_schema` – 形式がスキーマ定義を必要とする場合に使用するパラメータ。例えば、[Cap’n Proto](https://capnproto.org/) ではスキーマファイルへのパスとルート `schema.capnp:Message` オブジェクトの名前が必要です。
- `rabbitmq_num_consumers` – テーブルあたりのコンシューマ数。1つのコンシューマのスループットが不十分な場合は、コンシューマ数を増やします。デフォルト: `1`
- `rabbitmq_num_queues` – キューの合計数。この数を増やすと、パフォーマンスが大幅に向上する可能性があります。デフォルト: `1`。
- `rabbitmq_queue_base` - キュー名のヒントを指定。以下に設定の使用例を説明します。
- `rabbitmq_deadletter_exchange` - [デッドレターエクスチェンジ](https://www.rabbitmq.com/dlx.html)の名前を指定。デッドレターエクスチェンジに再公開された場合にメッセージを収集するために、このエクスチェンジ名を持つ別のテーブルを作成できます。デフォルトではデッドレターエクスチェンジは指定されていません。
- `rabbitmq_persistent` - 値が1（true）の場合、インサートクエリ配信モードが2に設定され（メッセージが「永続的」とマークされます）。デフォルト: `0`。
- `rabbitmq_skip_broken_messages` – ブロックごとにスキーマ不適合のメッセージを RabbitMQ メッセージパーサが許容する数。`rabbitmq_skip_broken_messages = N` の場合、エンジンは解析できない *N* RabbitMQ メッセージをスキップします（メッセージはデータの行に相当）。デフォルト: `0`。
- `rabbitmq_max_block_size` - RabbitMQ からデータをフラッシュする前に収集する行数。デフォルト: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size)。
- `rabbitmq_flush_interval_ms` - RabbitMQ からデータをフラッシュするタイムアウト。デフォルト: [stream_flush_interval_ms](../../../operations/settings/settings.md#stream-flush-interval-ms)。
- `rabbitmq_queue_settings_list` - キューを作成する際に RabbitMQ 設定をセットします。利用可能な設定: `x-max-length`, `x-max-length-bytes`, `x-message-ttl`, `x-expires`, `x-priority`, `x-max-priority`, `x-overflow`, `x-dead-letter-exchange`, `x-queue-type`。キューに対して `durable` 設定が自動的に有効になります。
- `rabbitmq_address` - 接続用アドレス。この設定か `rabbitmq_host_port` を使用します。
- `rabbitmq_vhost` - RabbitMQ vhost。デフォルト: `'/'`。
- `rabbitmq_queue_consume` - ユーザー定義のキューを使用し、RabbitMQのセットアップを行わない: エクスチェンジ、キュー、バインディングの宣言など。デフォルト: `false`。
- `rabbitmq_username` - RabbitMQ ユーザー名。
- `rabbitmq_password` - RabbitMQ パスワード。
- `reject_unhandled_messages` - エラーの場合にメッセージを拒否（RabbitMQ に否定的な承認を送信）。この設定は `rabbitmq_queue_settings_list` に `x-dead-letter-exchange` が定義されている場合に自動的に有効になります。
- `rabbitmq_commit_on_select` - select クエリが実行された際にメッセージをコミット。デフォルト: `false`。
- `rabbitmq_max_rows_per_message` — 行ベースの形式に対して、一つの RabbitMQ メッセージに書き込まれる最大行数。デフォルト: `1`。
- `rabbitmq_empty_queue_backoff_start` — RabbitMQ キューが空の場合の読み取り再スケジュールの開始バックオフポイント。
- `rabbitmq_empty_queue_backoff_end` — RabbitMQ キューが空の場合の読み取り再スケジュールの終了バックオフポイント。
- `rabbitmq_handle_error_mode` — RabbitMQエンジンのエラーハンドリング方法。選択可能な値: default（メッセージのパースに失敗すると例外が発生する）、stream（例外メッセージと生のメッセージが仮想カラム `_error` と `_raw_message` に保存される）。

  * [ ] SSL 接続:

`rabbitmq_secure = 1` または接続アドレスに `amqps` を使用: `rabbitmq_address = 'amqps://guest:guest@localhost/vhost'`。
使用されるライブラリのデフォルト動作は、作成された TLS 接続が十分に安全であるかどうかをチェックしないことです。証明書が期限切れ、自分で署名、欠落、無効であっても、接続が許可されます。証明書のより厳格な確認は将来的に実装される可能性があります。

rabbitmq関連の設定と共にフォーマット設定も追加できます。

例：

``` sql
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

RabbitMQ サーバー設定は ClickHouse のコンフィグファイルを使用して追加されるべきです。

必須コンフィグレーション：

``` xml
 <rabbitmq>
    <username>root</username>
    <password>clickhouse</password>
 </rabbitmq>
```

追加コンフィグレーション：

``` xml
 <rabbitmq>
    <vhost>clickhouse</vhost>
 </rabbitmq>
```

## 説明 {#description}

`SELECT` はメッセージの読み取りに特に有用ではありません（デバッグを除く）、なぜならメッセージは一度だけしか読み取ることができないからです。より実用的には、[マテリアライズドビュー](../../../sql-reference/statements/create/view.md)を使用してリアルタイムスレッドを作成します。これを行うには：

1. エンジンを使用して RabbitMQ コンシューマを作成し、それをデータストリームとみなします。
2. 希望の構造を持つテーブルを作成します。
3. エンジンからデータを変換し、前述のテーブルに入れるマテリアライズドビューを作成します。

`MATERIALIZED VIEW` がエンジンに接続されると、バックグラウンドでデータの収集を開始します。これにより、RabbitMQ からのメッセージを継続的に受信し、`SELECT` を使用して必要な形式に変換することができます。
1つの RabbitMQ テーブルには、いくつでもマテリアライズドビューを作ることができます。

データは `rabbitmq_exchange_type` と指定された `rabbitmq_routing_key_list` に基づいてチャネル化できます。
1テーブルあたりのエクスチェンジは1つまでで、1つのエクスチェンジは複数のテーブル間で共有できます - これにより、同時に複数のテーブルへのルーティングが可能になります。

エクスチェンジタイプオプション：

- `direct` - キーの正確な一致に基づくルーティング。例：テーブルのキーリスト `key1,key2,key3,key4,key5`、メッセージキーはそのいずれかに等しい可能性があります。
- `fanout` - キーに関係なくすべてのテーブル（エクスチェンジ名が同じ）へのルーティング。
- `topic` - ドットで区切られたキーを使用したパターンに基づくルーティング。例: `*.logs`, `records.*.*.2020`, `*.2018,*.2019,*.2020`。
- `headers` - `key=value` の一致に基づくルーティング(`x-match=all` または `x-match=any` 設定を使用) 。例：テーブルキーリスト `x-match=all,format=logs,type=report,year=2020`。
- `consistent_hash` - データがすべてのバインドされたテーブル間で均等に分配される（エクスチェンジ名が同じ）。注意、このエクスチェンジタイプは RabbitMQ プラグインで有効にする必要があります: `rabbitmq-plugins enable rabbitmq_consistent_hash_exchange`.

設定 `rabbitmq_queue_base` は次の用途に使用できます:

- 異なるテーブルにキューを共有させることで、一つのキューに複数のコンシューマを登録可能となり、より良いパフォーマンスを実現します。`rabbitmq_num_consumers` および/または `rabbitmq_num_queues` 設定を使用する場合、これらのパラメータが同じであると正確なキューの一致が達成されます。
- すべてのメッセージが正常に消費されなかった場合に特定の永続キューから読み取りを復元するため。一つの特定のキューから消費を再開するには、その名前を `rabbitmq_queue_base` 設定にセットし、`rabbitmq_num_consumers` および `rabbitmq_num_queues` を指定しないでください（デフォルトは 1）。特定のテーブルに対して宣言されたすべてのキューからの消費を再開するには、同じ設定を指定するだけです: `rabbitmq_queue_base`, `rabbitmq_num_consumers`, `rabbitmq_num_queues`。デフォルトでは、キュー名はテーブルに対してユニークになります。
- キューは耐久性があり、自動削除されないため再利用可能です。（RabbitMQ CLI ツールのいずれかで削除可能）

パフォーマンス向上のために、受信したメッセージは [max_insert_block_size](../../../operations/server-configuration-parameters/settings.md#settings-max_insert_block_size) サイズのブロックにグループ化されます。ブロックが [stream_flush_interval_ms](../../../operations/server-configuration-parameters/settings.md) ミリ秒以内に形成されなかった場合、データはブロックの完全性に関係なくテーブルにフラッシュされます。

`rabbitmq_num_consumers` および/または `rabbitmq_num_queues` 設定が `rabbitmq_exchange_type` と共に指定された場合:

- `rabbitmq-consistent-hash-exchange` プラグインを有効にする必要があります。
- 公開されるメッセージの `message_id` プロパティが指定されている必要があります（各メッセージ/バッチに対してユニーク）。

挿入クエリには、各公開されたメッセージに追加されるメッセージメタデータがあります: `messageID` および `republished` フラグ（真の場合、再公開された場合） - メッセージヘッダーを通じてアクセス可能です。

挿入とマテリアライズドビューに同じテーブルを使用しないでください。

例：

``` sql
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

## 仮想カラム {#virtual-columns}

- `_exchange_name` - RabbitMQ エクスチェンジ名。データ型: `String`。
- `_channel_id` - メッセージを受信した消費者が宣言された ChannelID。データ型: `String`。
- `_delivery_tag` - 受信したメッセージの DeliveryTag。チャネルごとにスコープされる。データ型: `UInt64`。
- `_redelivered` - メッセージの `redelivered` フラグ。データ型: `UInt8`。
- `_message_id` - 受信したメッセージの messageID。設定された場合は非空。データ型: `String`。
- `_timestamp` - 受信したメッセージのタイムスタンプ。設定された場合は非空。データ型: `UInt64`。

`kafka_handle_error_mode='stream'` の場合の追加仮想カラム:

- `_raw_message` - 正しく解析されなかった生メッセージ。データ型: `Nullable(String)`。
- `_error` - 解析の失敗時に発生した例外メッセージ。データ型: `Nullable(String)`。

注: `_raw_message` と `_error` 仮想カラムは、解析中の例外時にのみ埋められます。正常に解析された場合は常に `NULL` です。

## 注意点 {#caveats}

[デフォルトカラム式](/docs/ja/sql-reference/statements/create/table.md/#default_values)（`DEFAULT`, `MATERIALIZED`, `ALIAS` など）をテーブル定義に指定することはできますが、それらは無視されます。代わりに、カラムはその型のデフォルト値で埋められます。

## データ形式サポート {#data-formats-support}

RabbitMQ エンジンは、ClickHouse でサポートされているすべての[形式](../../../interfaces/formats.md)をサポートします。
1つの RabbitMQ メッセージ内の行数は、形式が行ベースかブロックベースかによります：

- 行ベースの形式の場合、1つの RabbitMQ メッセージ内の行数は `rabbitmq_max_rows_per_message` を設定することで制御できます。
- ブロックベースの形式の場合、ブロックを小さな部分に分けることはできませんが、1ブロック内の行数は一般設定 [max_block_size](../../../operations/settings/settings.md#setting-max_block_size) で制御できます。
