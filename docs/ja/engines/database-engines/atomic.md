---
slug: /ja/engines/database-engines/atomic
sidebar_label: Atomic
sidebar_position: 10
---

# Atomic 

非ブロッキングな[DROP TABLE](#drop-detach-table)および[RENAME TABLE](#rename-table)クエリや、アトミックな[EXCHANGE TABLES](#exchange-tables)クエリに対応しています。`Atomic` データベースエンジンはデフォルトで使用されます。

## データベースの作成 {#creating-a-database}

``` sql
CREATE DATABASE test [ENGINE = Atomic];
```

## 特徴と推奨事項 {#specifics-and-recommendations}

### テーブル UUID {#table-uuid}

`Atomic` データベース内のすべてのテーブルには永続的な[UUID](../../sql-reference/data-types/uuid.md)があり、データはディレクトリ `/clickhouse_path/store/xxx/xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy/` に保存されます。ここで `xxxyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy` はテーブルのUUIDです。通常、UUIDは自動的に生成されますが、テーブル作成時にユーザーが明示的にUUIDを指定することも可能です（推奨されません）。

例：

```sql
CREATE TABLE name UUID '28f1c61c-2970-457a-bffe-454156ddcfef' (n UInt64) ENGINE = ...;
```

:::note
`SHOW CREATE` クエリでUUIDを表示するには、[show_table_uuid_in_table_create_query_if_not_nil](../../operations/settings/settings.md#show_table_uuid_in_table_create_query_if_not_nil) 設定を使用できます。
:::

### RENAME TABLE {#rename-table}

[RENAME](../../sql-reference/statements/rename.md) クエリはUUIDを変更したり、テーブルデータを移動することなく実行されます。これらのクエリは、テーブルを使用しているクエリの完了を待つことなく瞬時に実行されます。

### DROP/DETACH TABLE {#drop-detach-table}

`DROP TABLE` の際、データは削除されず、`Atomic` データベースはメタデータを `/clickhouse_path/metadata_dropped/` に移動してテーブルを削除としてマークし、バックグラウンドスレッドに通知します。最終的なテーブルデータ削除までの遅延は、[database_atomic_delay_before_drop_table_sec](../../operations/server-configuration-parameters/settings.md#database_atomic_delay_before_drop_table_sec) 設定で指定されます。`SYNC` 修飾子を使用して同期モードを指定できます。この設定は[database_atomic_wait_for_drop_and_detach_synchronously](../../operations/settings/settings.md#database_atomic_wait_for_drop_and_detach_synchronously)を用いて行います。この場合、`DROP`はテーブルを使用している`SELECT`、`INSERT`およびその他のクエリの終了を待ちます。テーブルは使用されていないときに実際に削除されます。

### EXCHANGE TABLES/DICTIONARIES {#exchange-tables}

[EXCHANGE](../../sql-reference/statements/exchange.md) クエリは、テーブルまたはディクショナリーをアトミックに交換します。例えば、この非アトミックな操作を行う代わりに：

```sql
RENAME TABLE new_table TO tmp, old_table TO new_table, tmp TO old_table;
```

単一のアトミックなクエリを使用できます：

``` sql
EXCHANGE TABLES new_table AND old_table;
```

### Atomic データベース内の ReplicatedMergeTree {#replicatedmergetree-in-atomic-database}

[ReplicatedMergeTree](../table-engines/mergetree-family/replication.md#table_engines-replication) テーブルに対しては、エンジンパラメータ（ZooKeeperのパスとレプリカ名）を指定しないことをお勧めします。この場合、設定パラメータ [default_replica_path](../../operations/server-configuration-parameters/settings.md#default_replica_path) と [default_replica_name](../../operations/server-configuration-parameters/settings.md#default_replica_name) が使用されます。エンジンパラメータを明示的に指定したい場合は、`{uuid}` マクロを使用することをお勧めします。これにより、ZooKeeper内の各テーブルに対して自動的に一意のパスが生成されます。

## 関連項目

- [system.databases](../../operations/system-tables/databases.md) システムテーブル

