---
description: 'KILLのドキュメント'
sidebar_label: 'KILL'
sidebar_position: 46
slug: /sql-reference/statements/kill
title: 'KILLステートメント'
doc_type: 'reference'
---

KILLステートメントには、クエリを停止するものと、ミューテーションを停止するものの2種類があります

<div id="kill-query">
  ## KILL QUERY
</div>

```sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

現在実行中のクエリの強制終了を試みます。
終了対象のクエリは、`KILL`クエリの`WHERE`句で指定した条件に基づいて、system.processesテーブルから選択されます。

例:

まず、未完了のクエリの一覧を取得する必要があります。このSQLクエリでは、実行時間が長い順に一覧を取得できます。

単一のClickHouseノードからの一覧:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM system.processes
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

ClickHouseクラスターの一覧:

```sql
SELECT
  initial_query_id,
  query_id,
  formatReadableTimeDelta(elapsed) AS time_delta,
  query,
  *
  FROM clusterAllReplicas(default, system.processes)
  WHERE query ILIKE 'SELECT%'
  ORDER BY time_delta DESC;
```

クエリを強制終了します:

```sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

:::tip
ClickHouse Cloud またはセルフマネージドのクラスターでクエリを停止する場合は、すべてのレプリカで確実にクエリを停止するために、必ず `ON CLUSTER [cluster-name]` オプションを使用してください
:::

読み取り専用ユーザーは、自分自身のクエリしか停止できません。

デフォルトでは、クエリの停止が確認されるのを待たない非同期バージョン (`ASYNC`) が使用されます。

同期バージョン (`SYNC`) は、すべてのクエリが停止するまで待機し、各プロセスの停止時にその情報を表示します。
レスポンスには `kill_status` カラムが含まれ、次のいずれかの値を取ります。

1. `finished` – クエリは正常に停止されました。
2. `waiting` – 停止シグナルの送信後、クエリの終了を待機しています。
3. その他の値は、クエリを停止できない理由を示します。

テストクエリ (`TEST`) は、ユーザーの権限を確認し、停止対象となるクエリの一覧を表示するだけです。

<div id="kill-mutation">
  ## KILL MUTATION
</div>

長時間実行されている ミューテーション や未完了の ミューテーション がある場合、多くは ClickHouseサービス の動作状況が良くないことを示しています。ミューテーション は非同期で実行されるため、システムで利用可能なリソースをすべて消費してしまうことがあります。必要に応じて、次のいずれかを行ってください。

* 新しい ミューテーション、`INSERT`、`SELECT` をすべて一時停止し、ミューテーション の queue が処理し終わるのを待ちます。
* または、`KILL` コマンドを送信して、これらの ミューテーション の一部を手動で停止します。

```sql
KILL MUTATION
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

現在実行中の[ミューテーション](/ja/sql-reference/statements/alter#mutations)をキャンセルして削除しようとします。キャンセル対象のミューテーションは、`KILL` クエリの `WHERE` 句で指定したフィルタを使用して [`system.mutations`](/ja/operations/system-tables/mutations) テーブルから選択されます。

テストクエリ (`TEST`) は、ユーザーの権限を確認し、停止対象のミューテーションの一覧を表示するだけです。

例:

未完了のミューテーション数を `count()` で取得します:

単一の ClickHouse ノードにおけるミューテーション数:

```sql
SELECT count(*)
FROM system.mutations
WHERE is_done = 0;
```

レプリカで構成されるClickHouseクラスターのミューテーション数:

```sql
SELECT count(*)
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

未完了のミューテーションの一覧をクエリします:

単一のClickHouseノードのミューテーション一覧:

```sql
SELECT mutation_id, *
FROM system.mutations
WHERE is_done = 0;
```

ClickHouse クラスターのミューテーション一覧:

```sql
SELECT mutation_id, *
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

必要に応じてミューテーションを停止します:

```sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

ミューテーション が停止して完了できなくなった場合、このクエリが役立ちます (たとえば、ミューテーション クエリ内のいずれかの関数が、テーブルに含まれるデータに適用された際に例外をスローする場合) 。

ミューテーション によってすでに行われた変更はロールバックされません。

:::note
[system.mutations](/ja/operations/system-tables/mutations) テーブルの `is_killed=1` カラム (ClickHouse Cloud のみ) は、ミューテーション が完全に終了したことを必ずしも意味しません。ミューテーション が `is_killed=1` かつ `is_done=0` の状態のまま、長期間残ることがあります。これは、別の長時間実行中の ミューテーション が kill された ミューテーション をブロックしている場合に発生します。これは正常な状況です。
:::