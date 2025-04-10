---
slug: /ja/operations/system-tables/databases
---
# databases

現在のユーザーが利用可能なデータベースに関する情報を含みます。

カラム:

- `name` ([String](../../sql-reference/data-types/string.md)) — データベース名。
- `engine` ([String](../../sql-reference/data-types/string.md)) — [データベースエンジン](../../engines/database-engines/index.md)。
- `data_path` ([String](../../sql-reference/data-types/string.md)) — データパス。
- `metadata_path` ([String](../../sql-reference/data-types/enum.md)) — メタデータパス。
- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — データベースUUID。
- `comment` ([String](../../sql-reference/data-types/enum.md)) — データベースコメント。
- `engine_full` ([String](../../sql-reference/data-types/enum.md)) — データベースエンジンのパラメータ。
- `database` ([String](../../sql-reference/data-types/string.md)) – `name`のエイリアス。

このシステムテーブルの`name`カラムは、`SHOW DATABASES`クエリの実装に使用されます。

**例**

データベースの作成。

``` sql
CREATE DATABASE test;
```

ユーザーが利用可能なすべてのデータベースを確認します。

``` sql
SELECT * FROM system.databases;
```

``` text
┌─name────────────────┬─engine─────┬─data_path────────────────────┬─metadata_path─────────────────────────────────────────────────────────┬─uuid─────────────────────────────────┬─engine_full────────────────────────────────────────────┬─comment─┐
│ INFORMATION_SCHEMA  │ Memory     │ /data/clickhouse_data/       │                                                                       │ 00000000-0000-0000-0000-000000000000 │ Memory                                                 │         │
│ default             │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/f97/f97a3ceb-2e8a-4912-a043-c536e826a4d4/ │ f97a3ceb-2e8a-4912-a043-c536e826a4d4 │ Atomic                                                 │         │
│ information_schema  │ Memory     │ /data/clickhouse_data/       │                                                                       │ 00000000-0000-0000-0000-000000000000 │ Memory                                                 │         │
│ replicated_database │ Replicated │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/da8/da85bb71-102b-4f69-9aad-f8d6c403905e/ │ da85bb71-102b-4f69-9aad-f8d6c403905e │ Replicated('some/path/database', 'shard1', 'replica1') │         │
│ system              │ Atomic     │ /data/clickhouse_data/store/ │ /data/clickhouse_data/store/b57/b5770419-ac7a-4b67-8229-524122024076/ │ b5770419-ac7a-4b67-8229-524122024076 │ Atomic                                                 │         │
└─────────────────────┴────────────┴──────────────────────────────┴───────────────────────────────────────────────────────────────────────┴──────────────────────────────────────┴────────────────────────────────────────────────────────┴─────────┘

```
