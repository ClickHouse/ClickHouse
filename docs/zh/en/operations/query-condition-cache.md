---
description: '在 ClickHouse 中使用和配置查询条件缓存功能的指南'
sidebar_label: '查询条件缓存'
sidebar_position: 64
slug: /operations/query-condition-cache
title: '查询条件缓存'
doc_type: 'guide'
---

:::note
查询条件缓存仅在 [enable&#95;analyzer](https://clickhouse.com/docs/operations/settings/settings#enable_analyzer) 设置为 true 时生效，而这也是默认值。
:::

许多实际工作负载都会反复对相同或几乎相同的数据执行查询 (例如，在原有数据基础上新增了一部分数据) 。
ClickHouse 提供了多种优化技术来优化这类查询模式。
一种方式是通过索引结构 (例如主键索引、跳过索引、projections) 或预计算 (materialized views) 来调整物理数据布局。
另一种方式是使用 ClickHouse 的[查询缓存](query-cache.md)来避免重复执行查询计算。
第一种方法的缺点是需要数据库管理员手动干预和监控。
第二种方法则可能返回过时结果 (因为查询缓存不具备事务一致性) ，而这是否可接受取决于具体用例。

查询条件缓存为这两个问题都提供了一种巧妙的解决方案。
它基于这样一个思路：在相同数据上对某个过滤条件 (例如 `WHERE col = 'xyz'`) 进行计算，结果总是相同的。
更具体地说，查询条件缓存会针对每个已计算过的过滤器和每个粒度 (即默认由 8192 行组成的一个块) 记录该粒度中是否没有任何行满足该过滤条件。
这些信息会用单个比特记录：0 比特表示没有任何行匹配该过滤器，1 比特表示至少存在一行匹配。
在前一种情况下，ClickHouse 可以在计算过滤条件时跳过相应的粒度；在后一种情况下，则必须加载该粒度并进行计算。

如果满足以下三个前置条件，查询条件缓存就会非常有效：

* 第一，工作负载必须反复计算相同的过滤条件。如果同一个查询被重复执行多次，这种情况会自然出现；如果两个查询使用了相同的过滤器，也会出现这种情况，例如 `SELECT product FROM products WHERE quality > 3` 和 `SELECT vendor, count() FROM products WHERE quality > 3`。
* 第二，大多数数据必须是不可变的，也就是说，在多次查询之间不会发生变化。在 ClickHouse 中通常就是如此，因为 parts 是不可变的，并且只会通过 INSERT 创建。
* 第三，过滤器必须具有选择性，也就是说，只有相对较少的行满足该过滤条件。匹配过滤条件的行越少，被记录为 0 比特 (无匹配行) 的粒度就越多，后续计算过滤条件时可“剪枝”的数据也就越多。

<div id="memory-consumption">
  ## 内存占用
</div>

由于查询条件缓存针对每个过滤条件和粒度只存储一个比特，因此占用的内存很少。
查询条件缓存的最大大小可通过服务器设置 [`query_condition_cache_size`](server-configuration-parameters/settings.md#query_condition_cache_size) 进行配置 (默认值：100 MB) 。
100 MB 的缓存大小对应 100 * 1024 * 1024 * 8 = 838,860,800 个条目。
由于每个条目代表一个标记 (默认对应 8192 行) ，该缓存最多可覆盖单个列中的 6,871,947,673,600 (6.8 万亿) 行。
在实际场景中，过滤器通常会基于多个列进行计算，因此这个数字需要除以参与过滤的列数。

<div id="configuration-settings-and-usage">
  ## 配置设置与用法
</div>

设置 [use&#95;query&#95;condition&#95;cache](settings/settings#use_query_condition_cache) 用于控制是仅让某个特定查询使用查询条件缓存，还是让当前会话中的所有查询都使用该缓存。

例如，首次执行以下查询

```sql
SELECT col1, col2
FROM table
WHERE col1 = 'x'
SETTINGS use_query_condition_cache = true;
```

将存储表中不满足谓词的范围。
后续执行相同的查询时，如果同样使用参数 `use_query_condition_cache = true`，将利用查询条件缓存来减少扫描的数据量。

<div id="administration">
  ## 管理
</div>

查询条件缓存不会在 ClickHouse 重启后保留。

要清除查询条件缓存，请运行 [`SYSTEM CLEAR QUERY CONDITION CACHE`](../sql-reference/statements/system.md#drop-query-condition-cache)。

缓存内容显示在系统表 [system.query&#95;condition&#95;cache](system-tables/query_condition_cache.md) 中。
要计算当前查询条件缓存的大小 (MB) ，请运行 `SELECT formatReadableSize(sum(entry_size)) FROM system.query_condition_cache`。
如果你想查看单个过滤条件，可以检查 `system.query_condition_cache` 中的 `condition` 字段。请注意，该字段仅在调试构建中可用。

自数据库启动以来，查询条件缓存的命中和未命中次数会作为事件 &quot;QueryConditionCacheHits&quot; 和 &quot;QueryConditionCacheMisses&quot; 显示在系统表 [system.events](system-tables/events.md) 中。
这两个计数器仅会针对启用设置 `use_query_condition_cache = true` 运行的 `SELECT` 查询进行更新，其他查询不会影响 &quot;QueryCacheMisses&quot;。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[查询条件缓存简介](https://clickhouse.com/blog/introducing-the-clickhouse-query-condition-cache)
* [Predicate Caching: Query-Driven Secondary Indexing for Cloud Data Warehouses (Schmidt et. al., 2024)](https://doi.org/10.1145/3626246.3653395)