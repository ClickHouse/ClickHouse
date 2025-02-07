---
slug: /ja/sql-reference/statements/kill
sidebar_position: 46
sidebar_label: KILL
title: "KILLステートメント"
---

KILLステートメントには2種類あります: クエリを停止するものと、ミューテーションを停止するものです。

## KILL QUERY

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <system.processesテーブルからSELECTするための条件式>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

現在実行中のクエリを強制的に停止しようとします。停止するクエリは、`KILL`クエリの`WHERE`句で定義された条件に基づき、system.processesテーブルから選択されます。

例:

まず、未完了のクエリのリストを取得する必要があります。このSQLクエリは、最も長く実行されているものに基づき、これを提供します。

単一のClickHouseノードからのリスト:
``` sql
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

ClickHouseクラスタからのリスト:
``` sql
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

クエリの停止:
``` sql
-- 指定されたquery_idを持つすべてのクエリを強制終了します:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- 'username'によって実行されているすべてのクエリを同期的に終了します:
KILL QUERY WHERE user='username' SYNC
```

:::tip 
ClickHouse Cloudやセルフマネージドのクラスタでクエリを停止する場合は、`ON CLUSTER [cluster-name]`オプションを使用して、すべてのレプリカでクエリが確実に停止されるようにしてください。
:::

読み取り専用ユーザーは自分のクエリしか停止できません。

デフォルトでは、非同期バージョン（`ASYNC`）が使用され、クエリが停止したことの確認を待たずに進行します。

同期バージョン（`SYNC`）は、すべてのクエリが停止するのを待機し、各プロセスが停止するたびに情報を表示します。応答には、以下の値を持つ`kill_status`カラムが含まれます:

1. `finished` – クエリが正常に終了しました。
2. `waiting` – クエリの終了信号を送信した後、終了を待機しています。
3. その他の値は、クエリが停止できない理由を説明します。

テストクエリ（`TEST`）は、ユーザーの権限をチェックし、停止するクエリのリストを表示します。

## KILL MUTATION

長時間実行中のミューテーションや未完了のミューテーションがある場合、ClickHouseサービスが正常に動作していないことが示されることがよくあります。ミューテーションの非同期性により、システムの利用可能なすべてのリソースを消費してしまうことがあります。以下のいずれかの対策が必要な場合があります:

- すべての新しいミューテーション、`INSERT`、および`SELECT`を一時停止し、ミューテーションのキューを完了させる。
- または、`KILL`コマンドを送信して、これらのミューテーションの一部を手動で停止する。

``` sql
KILL MUTATION
  WHERE <system.mutationsテーブルからSELECTするための条件式>
  [TEST]
  [FORMAT format]
```

現在実行中の[ミューテーション](../../sql-reference/statements/alter/index.md#alter-mutations)をキャンセルして削除しようとします。キャンセルするミューテーションは、`KILL`クエリの`WHERE`句で指定されたフィルタを使用して[`system.mutations`](../../operations/system-tables/mutations.md#system_tables-mutations)テーブルから選択されます。

テストクエリ（`TEST`）は、ユーザーの権限を確認し、停止するミューテーションのリストを表示します。

例:

未完了ミューテーションの`count()`を取得:

単一のClickHouseノードからのミューテーション数:
``` sql
SELECT count(*)
FROM system.mutations
WHERE is_done = 0;
```

レプリカのClickHouseクラスタからのミューテーション数:
``` sql
SELECT count(*)
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

未完了のミューテーションのリストを照会:

単一のClickHouseノードからのミューテーションのリスト:
``` sql
SELECT mutation_id, *
FROM system.mutations
WHERE is_done = 0;
```

ClickHouseクラスタからのミューテーションのリスト:
``` sql
SELECT mutation_id, *
FROM clusterAllReplicas('default', system.mutations)
WHERE is_done = 0;
```

必要に応じてミューテーションを停止:
``` sql
-- 単一のテーブル内のすべてのミューテーションをキャンセルして削除:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- 特定のミューテーションをキャンセル:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

このクエリは、ミューテーションが停止できず完了しない状態（例えば、ミューテーションクエリ内の何らかの関数がテーブル内のデータに適用された際に例外を投げる場合）に役立ちます。

ミューテーションによってすでに行われた変更はロールバックされません。
