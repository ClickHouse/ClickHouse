---
description: '`Atomic` エンジンは、非ブロッキングの `DROP TABLE` および `RENAME TABLE`
  クエリと、アトミックな `EXCHANGE TABLES` クエリをサポートします。`Atomic` データベースエンジンは
  デフォルトで使用されます。'
sidebar_label: 'Atomic'
sidebar_position: 10
slug: /engines/database-engines/atomic
title: 'Atomic'
doc_type: 'reference'
---

`Atomic` エンジンは、非ブロッキングの [`DROP TABLE`](#drop-detach-table) クエリと [`RENAME TABLE`](#rename-table) クエリ、およびアトミックな [`EXCHANGE TABLES`](#exchange-tables) クエリをサポートします。オープンソース版 ClickHouse では、`Atomic` データベースエンジンがデフォルトで使用されます。

:::note
ClickHouse Cloud では、[`Shared` データベースエンジン](/ja/cloud/reference/shared-catalog#shared-database-engine) がデフォルトで使用されており、
上記の操作にも対応しています。
:::

<div id="creating-a-database">
  ## データベースの作成
</div>

```sql
CREATE DATABASE test [ENGINE = Atomic] [SETTINGS disk=...];
```

<div id="specifics-and-recommendations">
  ## 留意点と推奨事項
</div>

<div id="table-uuid">
  ### テーブル UUID
</div>

`Atomic` データベース内の各テーブルには永続的な [UUID](../../sql-reference/data-types/uuid.md) があり、そのデータは次のディレクトリに保存されます。

```text
/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/
```

ここで、`xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` はテーブルのUUIDです。

UUIDはデフォルトで自動的に生成されます。ただし、推奨はされませんが、テーブル作成時にユーザーがUUIDを明示的に指定することもできます。

たとえば:

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
[show&#95;table&#95;uuid&#95;in&#95;table&#95;create&#95;query&#95;if&#95;not&#95;nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil) 設定を使用すると、`SHOW CREATE` クエリで UUID を表示できます。
:::

<div id="rename-table">
  ### RENAME TABLE
</div>

[`RENAME`](../../sql-reference/statements/rename.md) クエリでは、UUID は変更されず、テーブルデータも移動されません。これらのクエリは即座に実行され、テーブルを使用中の他のクエリの完了を待機しません。

<div id="drop-detach-table">
  ### DROP/DETACH TABLE
</div>

`DROP TABLE` を使用しても、データは削除されません。`Atomic` engine は、メタデータを `/clickhouse_path/metadata_dropped/` に移動してテーブルを drop 済みとしてマークし、バックグラウンドスレッドに通知するだけです。最終的にテーブルデータが削除されるまでの遅延は、[`database_atomic_delay_before_drop_table_sec`](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec) 設定で指定します。
`SYNC` 修飾子を使用すると、同期モードを指定できます。これを行うには、[`database_atomic_wait_for_drop_and_detach_synchronously`](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously) 設定を使用します。この場合、`DROP` は、テーブルを使用している実行中の `SELECT`、`INSERT`、その他のクエリが終了するまで待機します。テーブルは、使用されなくなった時点で削除されます。

<div id="exchange-tables">
  ### EXCHANGE TABLES/DICTIONARIES
</div>

[`EXCHANGE`](../../sql-reference/statements/exchange.md)クエリは、テーブルまたはディクショナリをアトミックに入れ替えます。たとえば、次のような非アトミックな操作を行う代わりに：

```sql title="Non-atomic"
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```

atomic データベースを使用できます:

```sql title="Atomic"
EXCHANGE TABLES new_table AND old_table;
```

<div id="replicatedmergetree-in-atomic-database">
  ### atomic データベースの ReplicatedMergeTree
</div>

[`ReplicatedMergeTree`](/ja/engines/table-engines/mergetree-family/replication) テーブルでは、ZooKeeper 内のパスとレプリカ名のエンジンパラメータは指定しないことを推奨します。この場合は、設定パラメーター [`default_replica_path`](../../operations/server-configuration-parameters/settings.md#default_replica_path) と [`default_replica_name`](../../operations/server-configuration-parameters/settings.md#default_replica_name) が使用されます。エンジンパラメータを明示的に指定する場合は、`{uuid}` マクロを使用することを推奨します。これにより、ZooKeeper では各テーブルに対して一意のパスが自動的に生成されます。

<div id="metadata-disk">
  ### メタデータディスク
</div>

`SETTINGS` で `disk` を指定すると、そのディスクはテーブルのメタデータファイルの保存に使用されます。
例:

```sql
CREATE TABLE db (n UInt64) ENGINE = Atomic SETTINGS disk=disk(type='local', path='/var/lib/clickhouse-disks/db_disk');
```

未指定の場合、`database_disk.disk` で定義されたディスクがデフォルトで使用されます。

<div id="see-also">
  ## 関連項目
</div>

* [system.databases](../../operations/system-tables/databases.md) システムテーブル