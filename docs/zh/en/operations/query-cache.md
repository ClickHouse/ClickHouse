---
description: '在 ClickHouse 中使用和配置查询缓存功能的指南'
sidebar_label: '查询缓存'
sidebar_position: 65
slug: /operations/query-cache
title: '查询缓存'
doc_type: 'guide'
---

查询缓存可让 `SELECT` 查询只计算一次，之后对同一查询的执行可直接从缓存返回结果。
根据查询类型的不同，这可以显著降低 ClickHouse 服务器的延迟和资源消耗。

<div id="background-design-and-limitations">
  ## 背景、设计和局限性
</div>

查询缓存通常可分为事务一致和事务不一致两类。

* 在事务一致的缓存中，如果 `SELECT` 查询的结果发生了变化，或可能发生变化，数据库就会使已缓存的查询结果失效 (丢弃) 。在 ClickHouse 中，会更改数据的操作包括对表执行 insert/update/delete，以及 collapsing merges。事务一致缓存特别适合 OLTP 数据库，例如 [MySQL](https://dev.mysql.com/doc/refman/5.6/en/query-cache.html) (在 v8.0 之后移除了 query cache) 和 [Oracle](https://docs.oracle.com/database/121/TGDBA/tune_result_cache.htm)。
* 在事务不一致的缓存中，可以接受查询结果存在轻微偏差，前提是所有缓存条目都带有一个有效期，到期后即失效 (例如 1 分钟) ，并且底层数据在这段时间内变化很小。总体而言，这种方式更适合 OLAP 数据库。一个适合使用事务不一致缓存的例子是报表工具中的每小时销售报表，同时被多个用户访问。销售数据的变化通常足够缓慢，因此数据库只需计算一次该报表 (即第一次 `SELECT` 查询) 。后续查询可以直接由查询缓存返回结果。在这个例子中，合理的有效期可以是 30 分钟。

事务不一致缓存传统上由与数据库交互的客户端工具或代理软件包 (例如 [chproxy](https://www.chproxy.org/configuration/caching/)) 提供。因此，相同的缓存逻辑和配置往往会被重复实现。借助 ClickHouse 的查询缓存，缓存逻辑被移到了服务器端。这减少了维护工作量，也避免了冗余。

<div id="configuration-settings-and-usage">
  ## 配置设置和用法
</div>

:::note
在 ClickHouse Cloud 中，您必须使用[查询级设置](/zh/operations/settings/query-level)来修改查询缓存设置。目前暂不支持修改[配置级设置](/zh/operations/configuration-files)。
:::

:::note
[clickhouse-local](utilities/clickhouse-local.md) 一次只能运行一个查询。由于查询结果缓存没有实际意义，因此 `clickhouse-local` 中默认禁用了查询结果缓存。
:::

设置 [use&#95;query&#95;cache](/zh/operations/settings/settings#use_query_cache) 可用于控制特定查询或当前会话中的所有查询是否使用查询缓存。例如，首次执行以下查询

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true;
```

会将查询结果存储到查询缓存中。之后再次执行相同的查询时 (同样带有参数 `use_query_cache = true`) ，将会
从缓存中读取已计算好的结果并立即返回。

:::note
设置 `use_query_cache` 以及所有其他与查询缓存相关的设置，仅对独立的 `SELECT` 语句生效。特别是，
对于通过 `CREATE VIEW AS SELECT [...] SETTINGS use_query_cache = true` 创建的视图执行的 `SELECT`，其结果不会被缓存，除非该 `SELECT`
语句运行时带有 `SETTINGS use_query_cache = true`。
:::

还可以使用设置 [enable&#95;writes&#95;to&#95;query&#95;cache](/zh/operations/settings/settings#enable_writes_to_query_cache)
和 [enable&#95;reads&#95;from&#95;query&#95;cache](/zh/operations/settings/settings#enable_reads_from_query_cache) (两者默认均为 `true`) 更细致地配置缓存的使用方式。前者设置
控制是否将查询结果存储到缓存中，而后者设置决定数据库是否尝试从缓存中获取查询
结果。例如，下面的查询只会被动使用缓存，也就是说，会尝试从中读取，但不会将其
结果存入缓存：

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, enable_writes_to_query_cache = false;
```

为了获得最大控制权，通常建议仅在特定查询中提供设置 `use_query_cache`、`enable_writes_to_query_cache` 和
`enable_reads_from_query_cache`。也可以在用户或 profile 级别启用查询缓存 (例如通过 `SET
use_query_cache = true`) ，但需要注意，这样一来，所有 `SELECT` 查询都可能返回缓存结果。

可以使用语句 `SYSTEM CLEAR QUERY CACHE` 清空查询缓存。查询缓存的内容显示在系统表
[system.query&#95;cache](system-tables/query_cache.md) 中。自数据库启动以来的查询缓存命中和未命中次数，会在系统表
[system.events](system-tables/events.md) 中显示为事件
&quot;QueryCacheHits&quot; 和 &quot;QueryCacheMisses&quot;。这两个计数器仅会针对使用设置
`use_query_cache = true` 运行的 `SELECT` 查询更新，其他查询不会影响 &quot;QueryCacheMisses&quot;。系统表
[system.query&#95;log](system-tables/query_log.md) 中的字段 `query_cache_usage`
会显示每个已执行查询的查询结果是否已写入查询缓存，或是否从查询缓存中读取。系统表
[system.metrics](system-tables/metrics.md) 中的指标 `QueryCacheEntries` 和 `QueryCacheBytes`
显示查询缓存当前包含的条目数 / 字节数。

每个 ClickHouse 服务器进程都有各自的查询缓存。不过，默认情况下，缓存结果不会在用户之间共享。虽然这可以
更改 (见下文) ，但出于安全原因，不建议这样做。

查询结果在查询缓存中通过其查询的 [Abstract Syntax Tree (AST)](https://en.wikipedia.org/wiki/Abstract_syntax_tree) 来
引用。这意味着缓存对大小写不敏感，例如 `SELECT 1` 和 `select 1` 会被视为同一个查询。为了让匹配更符合直觉，所有与查询缓存和 [输出格式化](settings/settings-formats.md))
相关的查询级设置都会从 AST 中移除。

如果查询因异常或用户取消而中止，则不会向查询缓存写入任何条目。

查询缓存的字节大小、缓存条目的最大数量，以及单个缓存条目的最大大小 (按字节数和记录数计) ，都可以通过不同的 [服务器配置选项](/zh/operations/server-configuration-parameters/settings#query_cache) 进行配置。

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

也可以使用 [profile](settings/settings-profiles.md) 和 [设置
约束](settings/constraints-on-settings.md) 来限制单个用户的缓存占用。更具体地说，你可以限制用户在查询缓存中可
分配的最大内存量 (以字节为单位) ，以及可存储的查询结果最大数量。为此，先在 `users.xml` 的用户 profile 中配置
[query&#95;cache&#95;max&#95;size&#95;in&#95;bytes](/zh/operations/settings/settings#query_cache_max_size_in_bytes) 和
[query&#95;cache&#95;max&#95;entries](/zh/operations/settings/settings#query_cache_max_entries)，然后将这两个设置设为
readonly：

```xml
<profiles>
    <default>
        <!-- The maximum cache size in bytes for user/profile 'default' -->
        <query_cache_max_size_in_bytes>10000</query_cache_max_size_in_bytes>
        <!-- The maximum number of SELECT query results stored in the cache for user/profile 'default' -->
        <query_cache_max_entries>100</query_cache_max_entries>
        <!-- Make both settings read-only so the user cannot change them -->
        <constraints>
            <query_cache_max_size_in_bytes>
                <readonly/>
            </query_cache_max_size_in_bytes>
            <query_cache_max_entries>
                <readonly/>
            <query_cache_max_entries>
        </constraints>
    </default>
</profiles>
```

要设定查询至少运行多久，其结果才能被缓存，可以使用设置
[query&#95;cache&#95;min&#95;query&#95;duration](/zh/operations/settings/settings#query_cache_min_query_duration)。例如，查询

```sql
SELECT some_expensive_calculation(column_1, column_2)
FROM table
SETTINGS use_query_cache = true, query_cache_min_query_duration = 5000;
```

仅当查询运行超过 5 秒时，其结果才会被缓存。还可以指定查询需要运行多少次后，其结果才会被
缓存——为此请使用设置 [query&#95;cache&#95;min&#95;query&#95;runs](/zh/operations/settings/settings#query_cache_min_query_runs)。

查询缓存中的条目会在经过一段时间后失效 (time-to-live) 。默认情况下，这个时间为 60 秒，但也可以在会话、profile 或查询级别使用设置 [query&#95;cache&#95;ttl](/zh/operations/settings/settings#query_cache_ttl) 指定不同的
值。查询
缓存会以“惰性”方式驱逐条目，也就是说，当条目失效后，并不会立即从缓存中移除。相反，当一个新条目
要插入查询缓存时，数据库会检查缓存是否有足够的可用空间容纳该新条目。如果没有，
数据库会尝试移除所有失效条目。如果缓存仍然没有足够的可用空间，则不会插入新条目。

如果查询是通过 HTTP 运行的，那么 ClickHouse 会设置 `Age` 和 `Expires` 请求头，其中包含
缓存条目的已存在时长 (以秒为单位) 以及过期时间戳。

默认情况下，查询缓存中的条目会被压缩。这会降低总体内存消耗，但代价是写入 / 从查询缓存读取
的速度会变慢。要禁用压缩，请使用设置 [query&#95;cache&#95;compress&#95;entries](/zh/operations/settings/settings#query_cache_compress_entries)。

有时，为同一个查询保留多个已缓存结果会很有用。这可以通过使用设置
[query&#95;cache&#95;tag](/zh/operations/settings/settings#query_cache_tag) 来实现，它可作为查询缓存条目的标签 (或命名空间) 。查询缓存
会将带有不同标签的同一查询结果视为不同结果。

为同一个查询创建三个不同查询缓存条目的示例：

```sql
SELECT 1 SETTINGS use_query_cache = true; -- query_cache_tag is implicitly '' (empty string)
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 1';
SELECT 1 SETTINGS use_query_cache = true, query_cache_tag = 'tag 2';
```

如需仅清除查询缓存中带有标签 `tag` 的条目，可使用语句 `SYSTEM CLEAR QUERY CACHE TAG 'tag'`。

<div id="subquery-caching">
  ## 子查询缓存
</div>

默认情况下，外层查询中的 `use_query_cache` 不会传递到子查询。这意味着每个子查询都必须显式启用缓存：

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = true)
WHERE number > 500;
```

在此示例中，只有内部子查询的结果会被缓存，外部查询不会被缓存。

若要一次性为所有子查询启用缓存，请使用设置 `query_cache_for_subqueries`：

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

如果已启用批量传播，且要对某个特定子查询显式禁用缓存，请在该子查询中设置 `use_query_cache = false`：

```sql
SELECT *
FROM (SELECT number FROM system.numbers LIMIT 1000 SETTINGS use_query_cache = false)
WHERE number > 500
SETTINGS use_query_cache = true, query_cache_for_subqueries = true;
```

子查询缓存条目可在 [system.query&#95;cache](system-tables/query_cache.md) 中通过 `is_subquery = 1` 查看。`query_cache_ttl` 设置也适用于子查询缓存条目，并且可以针对每个子查询进行设置。

ClickHouse 按 [max&#95;block&#95;size](/zh/operations/settings/settings#max_block_size) 行的块读取表数据。由于筛选、聚合等原因，结果块通常远小于 &#39;max&#95;block&#95;size&#39;，但在某些情况下也可能大得多。设置
[query&#95;cache&#95;squash&#95;partial&#95;results](/zh/operations/settings/settings#query_cache_squash_partial_results) (默认启用) 用于控制是否在插入查询结果
缓存之前，将结果块压缩合并 (如果它们很小) 或拆分 (如果它们很大) 为大小为 &#39;max&#95;block&#95;size&#39; 的块。
这会降低写入查询缓存的性能，但会提高缓存条目的压缩率，并在之后从查询缓存返回查询结果时提供更自然的
块粒度。

因此，对于每个查询，查询缓存会存储多个 (部分)
结果块。虽然这种行为默认是合理的，但也可以使用设置
[query&#95;cache&#95;squash&#95;partial&#95;results](/zh/operations/settings/settings#query_cache_squash_partial_results) 禁用。

此外，包含非确定性函数的查询结果默认不会被缓存。这类函数包括

* 用于访问字典的函数：[`dictGet()`](/zh/sql-reference/functions/ext-dict-functions) 等。
* XML
  定义中不带 `<deterministic>true</deterministic>` 标签的[用户自定义函数](../sql-reference/statements/create/function.md)，
* 返回当前日期或时间的函数：[`now()`](../sql-reference/functions/date-time-functions.md#now)、
  [`today()`](../sql-reference/functions/date-time-functions.md#today)、
  [`yesterday()`](../sql-reference/functions/date-time-functions.md#yesterday) 等，
* 返回随机值的函数：[`randomString()`](../sql-reference/functions/random-functions.md#randomString)、
  [`fuzzBits()`](../sql-reference/functions/random-functions.md#fuzzBits) 等，
* 结果取决于查询处理所用内部 chunks 的大小和顺序的函数：
  [`nowInBlock()`](../sql-reference/functions/date-time-functions.md#nowInBlock) 等、
  [`rowNumberInBlock()`](../sql-reference/functions/other-functions.md#rowNumberInBlock)、
  [`runningDifference()`](../sql-reference/functions/other-functions.md#runningDifference)、
  [`blockSize()`](../sql-reference/functions/other-functions.md#blockSize) 等，
* 依赖环境的函数：[`currentUser()`](../sql-reference/functions/other-functions.md#currentUser)、
  [`queryID()`](/zh/sql-reference/functions/other-functions#queryID)、
  [`getMacro()`](../sql-reference/functions/other-functions.md#getMacro) 等。

如果无论如何都要强制缓存包含非确定性函数的查询结果，请使用设置
[query&#95;cache&#95;nondeterministic&#95;function&#95;handling](/zh/operations/settings/settings#query_cache_nondeterministic_function_handling)。

涉及系统表的查询结果 (例如 [system.processes](system-tables/processes.md)&#96; 或
[information&#95;schema.tables](system-tables/information_schema.md)) 默认不会被缓存。如果无论如何都要强制缓存包含
系统表的查询结果，请使用设置 [query&#95;cache&#95;system&#95;table&#95;handling](/zh/operations/settings/settings#query_cache_system_table_handling)。

最后，出于安全考虑，查询缓存中的条目不会在不同用户之间共享。例如，用户 A 不应通过运行与另一位用户 B 相同的查询来绕过
某个表上的行级策略，而用户 B 并不存在此类策略。不过，如有需要，可以通过指定设置
[query&#95;cache&#95;share&#95;between&#95;users](/zh/operations/settings/settings#query_cache_share_between_users) 将缓存条目标记为
允许其他用户访问 (即共享) 。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[ClickHouse 查询缓存介绍](https://clickhouse.com/blog/introduction-to-the-clickhouse-query-cache-and-design)