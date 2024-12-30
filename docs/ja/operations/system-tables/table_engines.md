---
slug: /ja/operations/system-tables/table_engines
---
# table_engines

サーバーでサポートされているテーブルエンジンの説明と、その機能サポート情報を含んでいます。

このテーブルには次のカラム（カラムタイプが括弧内に示されています）が含まれています:

- `name` (String) — テーブルエンジンの名前。
- `supports_settings` (UInt8) — テーブルエンジンが`SETTINGS`句をサポートするかを示すフラグ。
- `supports_skipping_indices` (UInt8) — テーブルエンジンが[スキッピングインデックス](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes)をサポートするかを示すフラグ。
- `supports_ttl` (UInt8) — テーブルエンジンが[有効期限 (TTL)](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl)をサポートするかを示すフラグ。
- `supports_sort_order` (UInt8) — テーブルエンジンが`PARTITION_BY`、`PRIMARY_KEY`、`ORDER_BY`、および`SAMPLE_BY`句をサポートするかを示すフラグ。
- `supports_replication` (UInt8) — テーブルエンジンが[データレプリケーション](../../engines/table-engines/mergetree-family/replication.md)をサポートするかを示すフラグ。
- `supports_deduplication` (UInt8) — テーブルエンジンがデータの重複除去をサポートするかを示すフラグ。
- `supports_parallel_insert` (UInt8) — テーブルエンジンが並列挿入をサポートするかを示すフラグ（[`max_insert_threads`](../../operations/settings/settings.md#max-insert-threads)設定を参照）。

例:

``` sql
SELECT *
FROM system.table_engines
WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

``` text
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┬─supports_parallel_insert─┐
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │                        1 │
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │                        0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │                        1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┴──────────────────────────┘
```

**関連項目**

- MergeTree ファミリーの[クエリ句](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
- Kafkaの[設定](../../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table)
- Joinの[設定](../../engines/table-engines/special/join.md#join-limitations-and-settings)
