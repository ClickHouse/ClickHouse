---
slug: /ja/engines/table-engines/integrations/nats
sidebar_position: 140
sidebar_label: NATS
---

# NATS エンジン {#redisstreams-engine}

このエンジンを使用すると、ClickHouseを[NATS](https://nats.io/)と統合できます。

`NATS` は以下を可能にします:

- メッセージのサブジェクトを発行したり、購読したりします。
- 新しいメッセージを利用可能になると処理します。

## テーブルの作成 {#creating-a-table}

``` sql
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
    [nats_startup_connect_tries = '5']
    [nats_max_rows_per_message = 1,]
    [nats_handle_error_mode = 'default']
```

必須パラメータ:

- `nats_url` – ホストとポート (例: `localhost:5672`).
- `nats_subjects` – NATS テーブルがサブスクライブ/発行するサブジェクトのリスト。`foo.*.bar`や`baz.>`のようなワイルドカードサブジェクトをサポートします。
- `nats_format` – メッセージフォーマット。SQL の `FORMAT` 関数の同じ表記を使用し、`JSONEachRow` などがあります。詳細については、[フォーマット](../../../interfaces/formats.md)セクションを参照してください。

オプションのパラメータ:

- `nats_schema` – フォーマットがスキーマの定義を必要とする場合に使用するパラメータ。例えば、[Cap’n Proto](https://capnproto.org/) はスキーマファイルのパスとルート `schema.capnp:Message` オブジェクトの名前を必要とします。
- `nats_num_consumers` – テーブルごとのコンシューマーの数。デフォルト: `1`。1つのコンシューマーのスループットが不十分な場合は、より多くのコンシューマーを指定します。
- `nats_queue_group` – NATS サブスクライバーのキューグループ名。デフォルトはテーブル名です。
- `nats_max_reconnect` – NATSへの接続1回あたりの再接続試行の最大回数。デフォルト: `5`。
- `nats_reconnect_wait` – 再接続試行間に睡眠する時間 (ミリ秒)。デフォルト: `5000`。
- `nats_server_list` - 接続するためのサーバーリスト。NATSクラスターへの接続に指定することができます。
- `nats_skip_broken_messages` - シャード内のスキーマ非互換メッセージに対する NATS メッセージパーサーの許容度。デフォルト: `0`。`nats_skip_broken_messages = N`の場合、解析できない*N*件のNATSメッセージをスキップします (メッセージはデータの行に相当します)。
- `nats_max_block_size` - NATSからデータをフラッシュするためのポール(s)で収集された行の数。デフォルト: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size)。
- `nats_flush_interval_ms` - NATSから読み取ったデータをフラッシュするためのタイムアウト。デフォルト: [stream_flush_interval_ms](../../../operations/settings/settings.md#stream-flush-interval-ms)。
- `nats_username` - NATS ユーザー名。
- `nats_password` - NATS パスワード。
- `nats_token` - NATS認証トークン。
- `nats_credential_file` - NATS認証情報ファイルのパス。
- `nats_startup_connect_tries` - 起動時の接続試行回数。デフォルト: `5`。
- `nats_max_rows_per_message` — 行ベースのフォーマットで1つのNATSメッセージに書き込む最大行数。(デフォルト : `1`)。
- `nats_handle_error_mode` — NATSエンジンのエラー処理方法。可能な値: default (メッセージの解析に失敗した場合に例外がスローされます)、stream (例外メッセージと生のメッセージが仮想カラム `_error` と `_raw_message` に保存されます)。

SSL接続:

安全な接続には `nats_secure = 1` を使用します。
使用されるライブラリのデフォルトの動作では、作成されたTLS接続が十分に安全であるかどうかをチェックしません。期限切れ、自署名、欠落、または無効な証明書であっても、接続は単に許可されます。証明書のより厳密なチェックは、将来的に実装できる可能性があります。

NATSテーブルへの書き込み:

テーブルが1つのサブジェクトからしか読み取らない場合、任意の挿入は同じサブジェクトに公開されます。
ただし、テーブルが複数のサブジェクトから読み取る場合、どのサブジェクトに公開するかを指定する必要があります。
そのため、複数のサブジェクトを持つテーブルに挿入する際は、`stream_like_engine_insert_queue`の設定が必要です。
テーブルが読み取るサブジェクトの1つを選択し、そこにデータを公開できます。例えば：

``` sql
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

また、nats関連の設定と共にフォーマット設定を追加することも可能です。

例:

``` sql
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
特に、NATSエンジン用のRedisパスワードを追加できます:

``` xml
<nats>
    <user>click</user>
    <password>house</password>
    <token>clickhouse</token>
</nats>
```

## 説明 {#description}

`SELECT` はメッセージを読み取るために特に有用ではありません（デバッグを除く）、なぜなら各メッセージは一度だけ読み取れます。実際には、[Materialized View](../../../sql-reference/statements/create/view.md)を使用してリアルタイムのスレッドを作成する方が実用的です。これを行うには:

1. エンジンを使用してNATSコンシューマーを作成し、データストリームと見なします。
2. 望む構造を持つテーブルを作成します。
3. エンジンからデータを変換し、事前に作成したテーブルに格納するMaterialized Viewを作成します。

`MATERIALIZED VIEW` がエンジンに接続されると、バックグラウンドでデータを収集し始めます。これにより、NATSからメッセージを継続的に受信し、`SELECT` を使用して要求されるフォーマットに変換できます。
1つのNATS テーブルには好きなだけ多くのMaterialized Viewを持たせることができ、これらはテーブルから直接データを読み取るのではなく、新しいレコード（ブロック単位）を受信します。このようにして異なる詳細レベルで（集約を伴う集計や集計なしで）複数のテーブルに書き込むことができます。

例:

``` sql
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

ストリームデータの受信を止めたり、変換ロジックを変更したりするには、Materialized Viewをデタッチします:

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

ターゲットテーブルを`ALTER`を使って変更したい場合は、ターゲットテーブルとビューからのデータ間の不一致を避けるために、マテリアルビューを無効にすることをお勧めします。

## 仮想カラム {#virtual-columns}

- `_subject` - NATS メッセージのサブジェクト。データ型: `String`.

`nats_handle_error_mode='stream'`の場合の追加仮想カラム:

- `_raw_message` - 解析に失敗した生メッセージ。データ型: `Nullable(String)`。
- `_error` - 解析に失敗したときの例外メッセージ。データ型: `Nullable(String)`。

注意: `_raw_message`と`_error`仮想カラムは、解析中に例外が発生した場合のみ埋められ、メッセージが正常に解析された場合は常に`NULL`です。

## データフォーマットのサポート {#data-formats-support}

NATS エンジンは、ClickHouseでサポートされているすべての[フォーマット](../../../interfaces/formats.md)をサポートしています。
1つのNATSメッセージ内の行数は、フォーマットが行ベースかブロックベースかによって異なります:

- 行ベースのフォーマットの場合、1つのNATSメッセージ内の行数は、`nats_max_rows_per_message`設定で制御できます。
- ブロックベースのフォーマットの場合、ブロックを小さな部分に分割することはできませんが、1つのブロック内の行数は、一般設定の[max_block_size](../../../operations/settings/settings.md#setting-max_block_size)で制御できます。
