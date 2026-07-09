---
description: 'TRUNCATE ステートメントのドキュメント'
sidebar_label: 'TRUNCATE'
sidebar_position: 52
slug: /sql-reference/statements/truncate
title: 'TRUNCATE ステートメント'
doc_type: 'リファレンス'
---

ClickHouse の `TRUNCATE` ステートメントは、テーブルまたはデータベースの構造を保持したまま、すべてのデータをすばやく削除するために使用されます。

<div id="truncate-table">
  ## テーブルをTRUNCATEする
</div>

```sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster] [SYNC]
```

<br />

| パラメーター               | 説明                                                                               |
| -------------------- | -------------------------------------------------------------------------------- |
| `IF EXISTS`          | テーブルが存在しない場合にエラーを防ぎます。省略すると、クエリはエラーを返します。                                        |
| `db.name`            | 省略可能なデータベース名です。                                                                  |
| `ON CLUSTER cluster` | 指定したクラスター全体でコマンドを実行します。                                                          |
| `SYNC`               | レプリケートテーブルを使用している場合、レプリカ間で TRUNCATE を同期的に実行します。省略すると、TRUNCATE はデフォルトで非同期に実行されます。 |

[alter&#95;sync](/ja/operations/settings/settings#alter_sync) 設定を使用すると、レプリカ上で処理が実行されるのを待機するよう設定できます。

[replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/ja/operations/settings/settings#replication_wait_for_inactive_replica_timeout) 設定を使用すると、非アクティブなレプリカが `TRUNCATE` クエリを実行するまで待機する時間 (秒単位) を指定できます。

:::note
`alter_sync` が `2` に設定されていて、一部のレプリカが `replication_wait_for_inactive_replica_timeout` 設定で指定された時間を超えて非アクティブな場合、例外 `UNFINISHED` がスローされます。
:::

`テーブルをTRUNCATEする` クエリは、次のテーブルエンジンでは**サポートされていません**。

* [`View`](../../engines/table-engines/special/view.md)
* [`File`](../../engines/table-engines/special/file.md)
* [`URL`](../../engines/table-engines/special/url.md)
* [`Buffer`](../../engines/table-engines/special/buffer.md)
* [`Null`](../../engines/table-engines/special/null.md)

<div id="truncate-all-tables">
  ## すべてのテーブルをTRUNCATEする
</div>

```sql
TRUNCATE [ALL] TABLES FROM [IF EXISTS] db [LIKE | ILIKE | NOT LIKE '<pattern>'] [ON CLUSTER cluster]
```

<br />

| パラメータ                                   | 説明                           |
| --------------------------------------- | ---------------------------- |
| `ALL`                                   | データベース内のすべてのテーブルからデータを削除します。 |
| `IF EXISTS`                             | データベースが存在しない場合にエラーを防ぎます。     |
| `db`                                    | データベース名です。                   |
| `LIKE \| ILIKE \| NOT LIKE '<pattern>'` | パターンでテーブルを絞り込みます。            |
| `ON CLUSTER cluster`                    | クラスター全体でコマンドを実行します。          |

データベース内のすべてのテーブルからすべてのデータを削除します。

<div id="truncate-database">
  ## TRUNCATE DATABASE
</div>

```sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

<br />

| Parameter            | Description              |
| -------------------- | ------------------------ |
| `IF EXISTS`          | データベースが存在しない場合のエラーを防ぎます。 |
| `db`                 | データベース名。                 |
| `ON CLUSTER cluster` | 指定したクラスター全体でコマンドを実行します。  |

データベース自体は残したまま、そのデータベース内のすべてのテーブルを削除します。句 `IF EXISTS` を省略すると、データベースが存在しない場合、クエリはエラーを返します。

:::note
`TRUNCATE DATABASE` は `Replicated` データベースではサポートされていません。代わりに、データベースを `DROP` してから `CREATE` してください。
:::