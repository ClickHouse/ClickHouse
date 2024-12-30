---
slug: /ja/sql-reference/statements/truncate
sidebar_position: 52
sidebar_label: TRUNCATE
---

# TRUNCATE ステートメント

## TRUNCATE TABLE
``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

テーブルからすべてのデータを削除します。`IF EXISTS` の句を省略した場合、テーブルが存在しないとクエリはエラーを返します。

`TRUNCATE` クエリは、[View](../../engines/table-engines/special/view.md)、[File](../../engines/table-engines/special/file.md)、[URL](../../engines/table-engines/special/url.md)、[Buffer](../../engines/table-engines/special/buffer.md)、[Null](../../engines/table-engines/special/null.md) テーブルエンジンではサポートされていません。

レプリカでの実行待機をセットアップするには、[alter_sync](../../operations/settings/settings.md#alter-sync) 設定を使用できます。

非アクティブなレプリカが `TRUNCATE` クエリを実行するのを待つ時間（秒単位）を指定するには、[replication_wait_for_inactive_replica_timeout](../../operations/settings/settings.md#replication-wait-for-inactive-replica-timeout) 設定を使用できます。

:::note    
`alter_sync` が `2` に設定され、`replication_wait_for_inactive_replica_timeout` 設定で指定された時間を超えてアクティブでないレプリカがある場合、`UNFINISHED` 例外がスローされます。
:::

## TRUNCATE ALL TABLES
``` sql
TRUNCATE ALL TABLES FROM [IF EXISTS] db [ON CLUSTER cluster]
```

データベース内のすべてのテーブルからすべてのデータを削除します。

## TRUNCATE DATABASE
``` sql
TRUNCATE DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

データベースからすべてのテーブルを削除しますが、データベースそのものは保持します。`IF EXISTS` の句を省略した場合、データベースが存在しないとクエリはエラーを返します。
