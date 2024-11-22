---
slug: /ja/operations/system-tables/view_refreshes
---
# view_refreshes

[更新可能なMaterialized View](../../sql-reference/statements/create/view.md#refreshable-materialized-view)に関する情報を提供します。サーバー起動時やテーブル作成時以降、更新が進行中かどうかに関わらず、すべての更新可能なMaterialized Viewが含まれています。

カラム:

- `database` ([String](../../sql-reference/data-types/string.md)) — テーブルが所在するデータベースの名前。
- `view` ([String](../../sql-reference/data-types/string.md)) — テーブル名。
- `uuid` ([UUID](../../sql-reference/data-types/uuid.md)) — テーブルのUUID (Atomicデータベース)。
- `status` ([String](../../sql-reference/data-types/string.md)) — 現在の更新の状態。
- `last_success_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md))) — 最新の成功した更新が開始された時間。サーバー起動以降またはテーブル作成以降成功した更新がなければNULL。
- `last_success_duration_ms` ([Nullable](../../sql-reference/data-types/nullable.md)([UInt64](../../sql-reference/data-types/int-uint.md))) — 最新の更新にどれくらい時間がかかったか。
- `last_refresh_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md))) — 最新の更新の試行が終了した（既知の場合）か開始された（不明な場合または実行中の場合）時間。サーバー起動以降またはテーブル作成以降更新の試行がなければNULL。
- `last_refresh_replica` ([String](../../sql-reference/data-types/string.md)) — 調整が有効化されている場合、現在の（実行中の場合）または前回の（実行中でない場合の）更新の試行を行ったレプリカの名前。
- `next_refresh_time` ([Nullable](../../sql-reference/data-types/nullable.md)([DateTime](../../sql-reference/data-types/datetime.md))) — statusがScheduledの場合に、次の更新が開始される予定の時間。
- `exception` ([String](../../sql-reference/data-types/string.md)) — 前回の試行が失敗した場合のエラーメッセージ。
- `retry` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 現在の更新においてこれまでに失敗した試行の回数。
- `progress` ([Float64](../../sql-reference/data-types/float.md)) — 現在の更新の進行状況（0から1の間）。statusが`RunningOnAnotherReplica`の場合は利用不可。
- `read_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 現在の更新でこれまでに読み取られた行数。statusが`RunningOnAnotherReplica`の場合は利用不可。
- `read_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 現在の更新中に読み取られたバイト数。statusが`RunningOnAnotherReplica`の場合は利用不可。
- `total_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 現在の更新に必要な行の推定総数。statusが`RunningOnAnotherReplica`の場合は利用不可。
- `written_rows` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 現在の更新中に書き込まれた行数。statusが`RunningOnAnotherReplica`の場合は利用不可。
- `written_bytes` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 現在の更新中に書き込まれたバイト数。statusが`RunningOnAnotherReplica`の場合は利用不可。

**例**

```sql
SELECT
    database,
    view,
    status,
    last_refresh_result,
    last_refresh_time,
    next_refresh_time
FROM system.view_refreshes

┌─database─┬─view───────────────────────┬─status────┬─last_refresh_result─┬───last_refresh_time─┬───next_refresh_time─┐
│ default  │ hello_documentation_reader │ Scheduled │ Finished            │ 2023-12-01 01:24:00 │ 2023-12-01 01:25:00 │
└──────────┴────────────────────────────┴───────────┴─────────────────────┴─────────────────────┴─────────────────────┘
```

