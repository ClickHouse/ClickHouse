---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# 系统。表\_engines {#system-table-engines}

包含服务器支持的表引擎的描述及其功能支持信息。

此表包含以下列（列类型显示在括号中):

-   `name` (String) — The name of table engine.
-   `supports_settings` (UInt8) — Flag that indicates if table engine supports `SETTINGS` 条款
-   `supports_skipping_indices` (UInt8) — Flag that indicates if table engine supports [跳过索引](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes).
-   `supports_ttl` (UInt8) — Flag that indicates if table engine supports [TTL](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).
-   `supports_sort_order` (UInt8) — Flag that indicates if table engine supports clauses `PARTITION_BY`, `PRIMARY_KEY`, `ORDER_BY` 和 `SAMPLE_BY`.
-   `supports_replication` (UInt8) — Flag that indicates if table engine supports [数据复制](../../engines/table-engines/mergetree-family/replication.md).
-   `supports_duduplication` (UInt8) — Flag that indicates if table engine supports data deduplication.

示例:

``` sql
SELECT *
FROM system.table_engines
WHERE name in ('Kafka', 'MergeTree', 'ReplicatedCollapsingMergeTree')
```

``` text
┌─name──────────────────────────┬─supports_settings─┬─supports_skipping_indices─┬─supports_sort_order─┬─supports_ttl─┬─supports_replication─┬─supports_deduplication─┐
│ Kafka                         │                 1 │                         0 │                   0 │            0 │                    0 │                      0 │
│ MergeTree                     │                 1 │                         1 │                   1 │            1 │                    0 │                      0 │
│ ReplicatedCollapsingMergeTree │                 1 │                         1 │                   1 │            1 │                    1 │                      1 │
└───────────────────────────────┴───────────────────┴───────────────────────────┴─────────────────────┴──────────────┴──────────────────────┴────────────────────────┘
```

**另请参阅**

-   梅树家族 [查询子句](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
-   卡夫卡 [设置](../../engines/table-engines/integrations/kafka.md#table_engine-kafka-creating-a-table)
-   加入我们 [设置](../../engines/table-engines/special/join.md#join-limitations-and-settings)
