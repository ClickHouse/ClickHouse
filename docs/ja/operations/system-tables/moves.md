---
slug: /ja/operations/system-tables/moves
---
# moves

このテーブルには、[MergeTree](/docs/ja/engines/table-engines/mergetree-family/mergetree.md) テーブルの進行中の[データパート移動](/docs/ja/sql-reference/statements/alter/partition#move-partitionpart)に関する情報が格納されています。各データパートの移動は単一の行で表現されます。

カラム:

- `database` ([String](/docs/ja/sql-reference/data-types/string.md)) — データベースの名前。

- `table` ([String](/docs/ja/sql-reference/data-types/string.md)) — データパートが移動中のテーブルの名前。

- `elapsed` ([Float64](../../sql-reference/data-types/float.md)) — データパートの移動が始まってから経過した時間（秒）。

- `target_disk_name` ([String](disks.md)) — データパートが移動する[ディスク](/docs/ja/operations/system-tables/disks/)の名前。

- `target_disk_path` ([String](disks.md)) — ファイルシステム内の[ディスク](/docs/ja/operations/system-tables/disks/)のマウントポイントへのパス。

- `part_name` ([String](/docs/ja/sql-reference/data-types/string.md)) — 移動中のデータパートの名前。

- `part_size` ([UInt64](../../sql-reference/data-types/int-uint.md)) — データパートのサイズ。

- `thread_id` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 移動を行っているスレッドの識別子。

**例**

```sql
SELECT * FROM system.moves
```

```response
┌─database─┬─table─┬─────elapsed─┬─target_disk_name─┬─target_disk_path─┬─part_name─┬─part_size─┬─thread_id─┐
│ default  │ test2 │ 1.668056039 │ s3               │ ./disks/s3/      │ all_3_3_0 │       136 │    296146 │
└──────────┴───────┴─────────────┴──────────────────┴──────────────────┴───────────┴───────────┴───────────┘
```

**関連項目**

- [MergeTree](/docs/ja/engines/table-engines/mergetree-family/mergetree.md) テーブルエンジン
- [データストレージのための複数のブロックデバイスの使用](/docs/ja/engines/table-engines/mergetree-family/mergetree#table_engine-mergetree-multiple-volumes)
- [ALTER TABLE ... MOVE PART](/docs/ja/sql-reference/statements/alter/partition#move-partitionpart) コマンド
