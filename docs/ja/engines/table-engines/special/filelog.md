---
slug: /ja/engines/table-engines/special/filelog
sidebar_position: 160
sidebar_label: FileLog
---

# FileLog エンジン {#filelog-engine}

このエンジンは、アプリケーションログファイルをレコードのストリームとして処理することができます。

`FileLog`を使用すると次のことができます:

- ログファイルを購読する。
- 購読したログファイルに新しいレコードが追加されると、それを処理する。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = FileLog('path_to_logs', 'format_name') SETTINGS
    [poll_timeout_ms = 0,]
    [poll_max_batch_size = 0,]
    [max_block_size = 0,]
    [max_threads = 0,]
    [poll_directory_watch_events_backoff_init = 500,]
    [poll_directory_watch_events_backoff_max = 32000,]
    [poll_directory_watch_events_backoff_factor = 2,]
    [handle_error_mode = 'default']
```

エンジン引数:

- `path_to_logs` –  購読するログファイルへのパス。ログファイルがあるディレクトリまたは単一のログファイルへのパスにすることができます。ClickHouseは`user_files`ディレクトリ内のパスのみを許可することに注意してください。
- `format_name` - レコードフォーマット。FileLog はファイル内の各行を個別のレコードとして処理し、すべてのデータフォーマットが適しているわけではありません。

オプションのパラメータ:

- `poll_timeout_ms` - ログファイルからの単一ポールのタイムアウト。デフォルト: [stream_poll_timeout_ms](../../../operations/settings/settings.md#stream_poll_timeout_ms)。
- `poll_max_batch_size` — 単一ポールでポールされるレコードの最大数。デフォルト: [max_block_size](../../../operations/settings/settings.md#setting-max_block_size)。
- `max_block_size` — ポールの最大バッチサイズ（レコード数）。デフォルト: [max_insert_block_size](../../../operations/settings/settings.md#max_insert_block_size)。
- `max_threads` - ファイルを解析するための最大スレッド数、デフォルトは0で、これは max(1, physical_cpu_cores / 4) となります。
- `poll_directory_watch_events_backoff_init` - ディレクトリを監視するスレッドの初期スリープ値。デフォルト: `500`。
- `poll_directory_watch_events_backoff_max` - ディレクトリ監視スレッドの最大スリープ値。デフォルト: `32000`。
- `poll_directory_watch_events_backoff_factor` - バックオフの速度、デフォルトで指数的。デフォルト: `2`。
- `handle_error_mode` — FileLog エンジンでのエラーハンドリング方法。可能な値: デフォルト（メッセージの解析に失敗したときに例外がスローされる）、ストリーム（例外メッセージと生メッセージが仮想カラム`_error`と`_raw_message`に保存される）。

## 説明 {#description}

配信されたレコードは自動的に追跡されるため、ログファイル内の各レコードは一度だけカウントされます。

`SELECT`はレコードを読み取るためには特に有用ではありません（デバッグ以外では）、というのも各レコードは一度だけしか読めないからです。より実用的なのは、[materialized views](../../../sql-reference/statements/create/view.md) を用いてリアルタイムのスレッドを作成することです。手順は以下の通りです：

1.  エンジンを用いて FileLog テーブルを作成し、それをデータストリームと見なす。
2.  目的の構造を持つテーブルを作成する。
3.  エンジンからデータを変換し、事前に作成したテーブルにデータを格納する materialized view を作成する。

`MATERIALIZED VIEW`がエンジンに結合されると、バックグラウンドでデータを収集し始めます。これにより、ログファイルからレコードを継続的に受け取り、`SELECT`を使って必要なフォーマットに変換できます。一つの FileLog テーブルは、好きなだけ materialized views を持つことができます。これらはテーブルから直接データを読み取ることはせず、新しいレコード（ブロック単位）を受け取ることで、異なる詳細レベル（集計 - グループ化ありとなし）の複数のテーブルに書き込むことができます。

例:

``` sql
  CREATE TABLE logs (
    timestamp UInt64,
    level String,
    message String
  ) ENGINE = FileLog('user_files/my_app/app.log', 'JSONEachRow');

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

ストリームデータの受信を停止したい場合や変換ロジックを変更したい場合、materialized viewをデタッチします：

``` sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

`ALTER`を使ってターゲットテーブルを変更したい場合は、ターゲットテーブルとビューからのデータの間に不整合が生じるのを避けるために、material viewを無効化することをお勧めします。

## 仮想カラム {#virtual-columns}

- `_filename` - ログファイルの名前。データ型: `LowCardinality(String)`。
- `_offset` - ログファイル内のオフセット。データ型: `UInt64`。

`handle_error_mode='stream'`の際の追加の仮想カラム:

- `_raw_record` - 正しく解析できなかった生レコード。データ型: `Nullable(String)`。
- `_error` - 解析失敗時の例外メッセージ。データ型: `Nullable(String)`。

注意: `_raw_record`および`_error`の仮想カラムは、解析中に例外が発生した場合のみ埋められ、メッセージが正常に解析された場合は常に`NULL`です。
