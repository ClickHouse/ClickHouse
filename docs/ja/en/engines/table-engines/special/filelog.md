---
description: 'このエンジンは、アプリケーションのログファイルをレコードのストリームとして処理できます。'
sidebar_label: 'FileLog'
sidebar_position: 160
slug: /engines/table-engines/special/filelog
title: 'FileLog テーブルエンジン'
doc_type: 'reference'
---

このエンジンは、アプリケーションのログファイルをレコードのストリームとして処理できます。

`FileLog` では、次のことができます。

* ログファイルを購読する。
* 購読したログファイルに追記された新しいレコードを処理する。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
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

* `path_to_logs` – サブスクライブするログファイルへのパス。ログファイルを含むディレクトリへのパス、または単一のログファイルへのパスを指定できます。ClickHouse では `user_files` ディレクトリ内のパスのみ指定できる点に注意してください。
* `format_name` - レコードのフォーマット。FileLog はファイル内の各行を個別のレコードとして処理するため、すべてのデータフォーマットがこれに適しているわけではありません。

パラメータ:

* `poll_timeout_ms` - ログファイルから 1 回 poll する際のタイムアウト。デフォルト: [stream&#95;poll&#95;timeout&#95;ms](../../../operations/settings/settings.md#stream_poll_timeout_ms)。
* `poll_max_batch_size` — 1 回の poll で取得するレコードの最大数。デフォルト: [max&#95;block&#95;size](/ja/operations/settings/settings#max_block_size)。
* `max_block_size` — poll の最大バッチサイズ (レコード数) 。デフォルト: [max&#95;insert&#95;block&#95;size](../../../operations/settings/settings.md#max_insert_block_size)。
* `max_threads` - ファイルを解析する最大スレッド数。デフォルトは 0 で、この場合は max(1, physical&#95;cpu&#95;cores / 4) になります。
* `poll_directory_watch_events_backoff_init` - ディレクトリ監視スレッドの初期 sleep 値。デフォルト: `500`。
* `poll_directory_watch_events_backoff_max` - ディレクトリ監視スレッドの最大 sleep 値。デフォルト: `32000`。
* `poll_directory_watch_events_backoff_factor` - バックオフの速度。デフォルトでは指数的です。デフォルト: `2`。
* `handle_error_mode` — FileLog エンジンでエラーをどのように処理するか。設定可能な値: default (メッセージの解析に失敗した場合は例外がスローされます) 、stream (例外メッセージと生のメッセージが仮想カラム `_error` および `_raw_message` に保存されます) 。

<div id="description">
  ## 説明
</div>

配信されたレコードは自動的に追跡されるため、ログファイル内の各レコードがカウントされるのは 1 回だけです。

レコードの読み取りに `SELECT` はあまり適していません (デバッグ用途を除く) 。各レコードは 1 回しか読み取れないためです。代わりに、[materialized view](../../../sql-reference/statements/create/view.md) を使用してリアルタイムの処理フローを作成するほうが実用的です。手順は次のとおりです。

1. engine を使用して FileLog table を作成し、それをデータストリームとして扱います。
2. 必要な structure を持つ table を作成します。
3. engine からの data を変換し、事前に作成した table に格納する materialized view を作成します。

`MATERIALIZED VIEW` が engine に接続されると、バックグラウンドで data の収集を開始します。これにより、ログファイルから継続的にレコードを受信し、`SELECT` を使って必要なフォーマットに変換できます。
1 つの FileLog table には、必要な数だけ materialized view を作成できます。これらは table から直接 data を読み取るのではなく、新しいレコードを (block 単位で) 受け取ります。これにより、異なる粒度の複数の table に書き込むことができます (グループ化と aggregation を行う場合／行わない場合) 。

例:

```sql
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
    AS SELECT toDate(toDateTime(timestamp)) AS day, level, count() AS total
    FROM logs GROUP BY day, level;

  SELECT level, sum(total) FROM daily GROUP BY level;
```

ストリームデータの受信を停止するか、変換ロジックを変更するには、materialized view をデタッチします:

```sql
  DETACH TABLE consumer;
  ATTACH TABLE consumer;
```

`ALTER` を使用してターゲットテーブルを変更する場合は、ターゲットテーブルとビュー経由のデータとの不整合を避けるため、materialized view を無効にすることをおすすめします。

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_filename` - ログファイル名。データ型: `LowCardinality(String)`.
* `_offset` - ログファイル内のオフセット。データ型: `UInt64`.

`handle_error_mode='stream'` の場合、以下の仮想カラムも追加されます。

* `_raw_record` - 正常にパースできなかった生のレコード。データ型: `Nullable(String)`.
* `_error` - パース失敗時に発生した例外メッセージ。データ型: `Nullable(String)`.

注: `_raw_record` と `_error` の仮想カラムに値が入るのは、パース中に例外が発生した場合のみです。メッセージが正常にパースされた場合は、常に `NULL` になります。