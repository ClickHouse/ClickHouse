---
description: '`QueryRunner` テーブルに挿入されたレコードは、エンジンがローカルまたはリモートのクラスター上で "fire and forget" モードにより実行するクエリを表します。'
sidebar_label: 'QueryRunner'
sidebar_position: 55
slug: /engines/table-engines/special/query-runner
title: 'QueryRunner テーブルエンジン'
doc_type: 'reference'
---

<div id="queryrunner-table-engine">
  # QueryRunner テーブルエンジン
</div>

`QueryRunner` テーブルに挿入されたレコードは、このエンジンによって実行されるクエリを表します。
このエンジンは、非同期クエリ実行、生成されたクエリのバッチ実行、
リモートクラスターへのクエリの転送、ベンチマーク、ファジング、シャドウトラフィックを用いたテストに使用できます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE runner
(
    query String,
    database String,
    settings Map(LowCardinality(String), String)
)
ENGINE = QueryRunner
SETTINGS
    cluster = 'cluster_name',
    shard = '1',
    mode = 'asynchronous',
    threads = 4,
    max_queue_size = 1000
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }];
```

テーブルは、許可されているカラム `query`、`database`、`settings` の一部を使って作成する必要があります。
カラム `query` は必須で、他のカラムは任意です。

| Column     | Type                  | Meaning                                        |
| ---------- | --------------------- | ---------------------------------------------- |
| `query`    | `String`              | 実行するクエリ。                                       |
| `database` | `String`              | クエリのデフォルトデータベース。空の場合は、サーバーのデフォルトデータベースが使用されます。 |
| `settings` | `Map(String, String)` | クエリに適用される設定。                                   |

<div id="engine-settings">
  ## エンジン設定
</div>

| Setting          | Default          | Meaning                                                                                                         |
| ---------------- | ---------------- | --------------------------------------------------------------------------------------------------------------- |
| `cluster`        | `''`             | クエリの送信先となるクラスター名。空の場合、クエリはローカルで実行されます。                                                                          |
| `shard`          | `'1'`            | クエリの送信先となるクラスターの分片の1始まりの索引、またはクエリごとにランダムな分片を選択する `'random'`、もしくは各クエリをすべての分片で実行する `'all'`。`cluster` 設定が必要です。     |
| `mode`           | `'asynchronous'` | `synchronous` モードでは、挿入された Batch 内のすべてのクエリの完了後に INSERT が返ります。`asynchronous` モードでは、クエリがキューに追加された時点で INSERT が返ります。 |
| `threads`        | `4`              | クエリを実行するバックグラウンドスレッド数。                                                                                          |
| `max_queue_size` | `1000`           | キューに入れられるクエリの最大数。キューがいっぱいになると、新たに挿入されたクエリは破棄され、エラーが記録されます。                                                      |

<div id="details">
  ## 詳細
</div>

このテーブルで許可されるのは `INSERT` クエリのみです。
クエリは「fire and forget」モードで実行されるため、例外が発生しても再試行は行われず、
`SELECT` クエリの結果は破棄されます (結果を保持する唯一の方法は `INSERT SELECT` のみです) 。
各クエリが成功したかどうかは `system.query_log` テーブルで確認できます。この
エンジンによって開始されたクエリには、開始元のサーバーで `is_internal = 1` が設定されます。

キューに入れられたクエリはメモリ内に保持されるため、サーバーを再起動すると失われます。サーバーの停止時
 (またはテーブルの `DROP`/`DETACH` 時) には、まだ開始されていないクエリは破棄されます。すでに
実行中のクエリについては、クラスターに送出されたものはキャンセルされ、ローカルで実行中のものは
完了するまで待機します。

実行するクエリ自体が `INSERT` の場合、そのデータはインラインで指定する必要があります。つまり `INSERT ... VALUES (...)`、
`INSERT ... SELECT ...`、またはクエリテキスト内にデータを含む `INSERT ... FORMAT ...` です。データを
別のストリームから受け取る `INSERT` には対応していません。

<div id="local-mode-and-sql-security">
  ## Local mode と SQL SECURITY
</div>

`cluster` 設定がない場合、クエリはローカルサーバーで実行されます。
どのユーザーとして実行されるかは、`SQL SECURITY` 句によって決まります。

* `INVOKER` (デフォルト) : クエリは、`INSERT` を実行したユーザーの権限で実行されます。
* `DEFINER`: クエリは、指定された `DEFINER` ユーザーの権限で実行されます。挿入されるクエリは任意であるため、このようなテーブルに対して `INSERT` を付与すると、definer のすべての権限が委譲されます。
* `NONE`: クエリはユーザーなしで完全なアクセス権を持って実行されます。テーブルの作成時には `ALLOW_SQL_SECURITY_NONE` 権限が必要です。

<div id="cluster-mode">
  ## クラスターモード
</div>

`cluster` 設定が指定されている場合、クエリは指定されたクラスターに送信されます。

対象の分片は `shard` で選択されます。指定できるのは、固定の 1 始まりのインデックス (デフォルトは `'1'`) 、クエリごとに
ランダムな分片を選択する `'random'`、またはクラスター内のすべての分片で各クエリを実行する `'all'` です。分片内のレプリカは、
サーバーの `load_balancing` 設定に従って選択されます。

`database` カラムは、リモートサーバーへの接続のデフォルトデータベースを設定します。デフォルトデータベースは
接続ごとに一度だけ設定されるため、`database` の値が異なるごとに専用の
接続プールが使用されます。この接続プールは最初の使用時に作成され、テーブルの存続期間中は再利用されます。

`DEFINER` と `SQL SECURITY` が有効なのはローカルモードの場合のみであり、これらを
`cluster` 設定と組み合わせるとエラーになります。リモートサーバーでは、クエリは
クラスター設定の認証情報で認証され、通常の初期クエリとして実行されます。これらは
`system.query_log` に `is_initial_query = 1` および独自の `query_id` とともに記録されます (それらを生成した INSERT とは
関連付けられません) 。開始元のサーバーでは、送出されるクエリは `system.query_log`
に `is_internal = 1` として記録されます。

このエンジンはクエリ結果を破棄するため、送出されるクエリは常に
`discard_query_data = 1` を付けて実行されます。したがって、SELECT クエリの結果データはネットワーク経由で転送されません
 (これは `settings` カラムで設定された `discard_query_data` の値を上書きします) 。

<div id="waiting-for-queries-to-finish">
  ## クエリの完了を待機する
</div>

非同期モードでは、これまでにそのテーブルに送信されたすべてのクエリの完了を待機するために、次のクエリを使用できます。

```sql
SYSTEM WAIT QUERY RUNNER runner;
```

<div id="example">
  ## 例
</div>

クエリログに記録された最近の`SELECT`クエリを再実行します：

```sql
INSERT INTO runner (query, database, settings)
SELECT query, current_database, Settings
FROM system.query_log
WHERE type = 'QueryFinish' AND is_initial_query AND NOT is_internal AND query_kind = 'Select'
  AND event_time > now() - INTERVAL 1 HOUR;
```