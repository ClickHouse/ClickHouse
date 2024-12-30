---
slug: /ja/operations/system-tables/events
---
# events

システムで発生したイベントの数に関する情報を含んでいます。例えば、このテーブルでは、ClickHouseサーバーが起動してから処理された`SELECT`クエリの数を確認できます。

カラム:

- `event` ([String](../../sql-reference/data-types/string.md)) — イベント名。
- `value` ([UInt64](../../sql-reference/data-types/int-uint.md)) — 発生したイベントの数。
- `description` ([String](../../sql-reference/data-types/string.md)) — イベントの説明。
- `name` ([String](../../sql-reference/data-types/string.md)) — `event`の別名。

サポートされているすべてのイベントはソースファイル [src/Common/ProfileEvents.cpp](https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ProfileEvents.cpp) で確認できます。

**例**

``` sql
SELECT * FROM system.events LIMIT 5
```

``` text
┌─event─────────────────────────────────┬─value─┬─description────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Query                                 │    12 │ クエリを解釈し、実行される可能性のあるクエリの数。パースに失敗したクエリや、ASTサイズ制限、クォータ制限、同時実行クエリ数の制限により拒否されたクエリは含まれません。ClickHouse自身によって開始された内部クエリを含む場合があります。サブクエリはカウントされません。             │
│ SelectQuery                           │     8 │ Queryと同じですが、SELECTクエリのみです。                                                                                                                                                                                                                    │
│ FileOpen                              │    73 │ 開かれたファイルの数。                                                                                                                                                                                                                                    │
│ ReadBufferFromFileDescriptorRead      │   155 │ ファイルディスクリプタからの読み込み（read/pread）の数。ソケットは含まれません。                                                                                                                                                                           │
│ ReadBufferFromFileDescriptorReadBytes │  9931 │ ファイルディスクリプタから読み取られたバイト数。ファイルが圧縮されている場合、これは圧縮データサイズを示します。                                                                                                    │
└───────────────────────────────────────┴───────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

**関連項目**

- [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) — 定期的に計算されるメトリクスを含んでいます。
- [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) — 瞬時に計算されるメトリクスを含んでいます。
- [system.metric_log](../../operations/system-tables/metric_log.md#system_tables-metric_log) — テーブル `system.metrics` と `system.events` のメトリクス値の履歴を含んでいます。
- [Monitoring](../../operations/monitoring.md) — ClickHouseモニタリングの基本概念。
