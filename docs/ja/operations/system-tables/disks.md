---
slug: /ja/operations/system-tables/disks
---
# disk

[サーバー設定](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes_configure)で定義されたディスクの情報を含みます。

カラム:

- `name` ([String](../../sql-reference/data-types/string.md)) — サーバー設定内のディスクの名前。
- `path` ([String](../../sql-reference/data-types/string.md)) — ファイルシステム内のマウントポイントへのパス。
- `free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ディスク上の空き容量（バイト単位）。
- `total_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ディスク容量（バイト単位）。
- `unreserved_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 予約されていない空き容量。（`free_space`から、現在実行中のマージやインサート及びその他のディスク書き込み操作によって取られている予約サイズを引いた値。）
- `keep_free_space` ([UInt64](../../sql-reference/data-types/int-uint.md)) — ディスク上で常に空けておくべきディスク容量（バイト単位）。ディスク設定の`keep_free_space_bytes`パラメーターで定義。

**例**

```sql
SELECT * FROM system.disks;
```

```response
┌─name────┬─path─────────────────┬───free_space─┬──total_space─┬─keep_free_space─┐
│ default │ /var/lib/clickhouse/ │ 276392587264 │ 490652508160 │               0 │
└─────────┴──────────────────────┴──────────────┴──────────────┴─────────────────┘

1 rows in set. Elapsed: 0.001 sec.
```
