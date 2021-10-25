---
toc_priority: 37
toc_title: VIEW
---

# CREATE VIEW {#create-view}

创建一个新视图。 有两种类型的视图：普通视图和物化视图。

## Normal {#normal}

语法:

``` sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER] AS SELECT ...
```

普通视图不存储任何数据。 他们只是在每次访问时从另一个表执行读取。换句话说，普通视图只不过是一个保存的查询。 从视图中读取时，此保存的查询用作[FROM](../../../sql-reference/statements/select/from.md)子句中的子查询.

例如，假设您已经创建了一个视图：

``` sql
CREATE VIEW view AS SELECT ...
```

并写了一个查询：

``` sql
SELECT a, b, c FROM view
```

这个查询完全等同于使用子查询：

``` sql
SELECT a, b, c FROM (SELECT ...)
```

## Materialized {#materialized}

``` sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER] [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

物化视图存储由相应的[SELECT](../../../sql-reference/statements/select/index.md)管理.

创建不带`TO [db].[table]`的物化视图时，必须指定`ENGINE` – 用于存储数据的表引擎。

使用`TO [db].[table]` 创建物化视图时，不得使用`POPULATE`。

一个物化视图的实现是这样的：当向SELECT中指定的表插入数据时，插入数据的一部分被这个SELECT查询转换，结果插入到视图中。

!!! important "重要"
ClickHouse 中的物化视图更像是插入触发器。 如果视图查询中有一些聚合，则它仅应用于一批新插入的数据。 对源表现有数据的任何更改（如更新、删除、删除分区等）都不会更改物化视图。

如果指定`POPULATE`，则在创建视图时将现有表数据插入到视图中，就像创建一个`CREATE TABLE ... AS SELECT ...`一样。 否则，查询仅包含创建视图后插入表中的数据。 我们**不建议**使用POPULATE，因为在创建视图期间插入表中的数据不会插入其中。

`SELECT` 查询可以包含`DISTINCT`、`GROUP BY`、`ORDER BY`、`LIMIT`……请注意，相应的转换是在每个插入数据块上独立执行的。 例如，如果设置了`GROUP BY`，则在插入期间聚合数据，但仅在插入数据的单个数据包内。 数据不会被进一步聚合。 例外情况是使用独立执行数据聚合的`ENGINE`，例如`SummingMergeTree`。

在物化视图上执行[ALTER](../../../sql-reference/statements/alter/index.md)查询有局限性，因此可能不方便。 如果物化视图使用构造`TO [db.]name`，你可以`DETACH`视图，为目标表运行`ALTER`，然后`ATTACH`先前分离的（`DETACH`）视图。

请注意，物化视图受[optimize_on_insert](../../../operations/settings/settings.md#optimize-on-insert)设置的影响。 在插入视图之前合并数据。

视图看起来与普通表相同。 例如，它们列在1SHOW TABLES1查询的结果中。

删除视图,使用[DROP VIEW](../../../sql-reference/statements/drop.md#drop-view). `DROP TABLE`也适用于视图。

## Live View (实验性) {#live-view}

!!! important "重要"
这是一项实验性功能，可能会在未来版本中以向后不兼容的方式进行更改。
使用[allow_experimental_live_view](../../../operations/settings/settings.md#allow-experimental-live-view)设置启用实时视图和`WATCH`查询的使用。 输入命令`set allow_experimental_live_view = 1`。

```sql
CREATE LIVE VIEW [IF NOT EXISTS] [db.]table_name [WITH [TIMEOUT [value_in_sec] [AND]] [REFRESH [value_in_sec]]] AS SELECT ...
```

实时视图存储相应[SELECT](../../../sql-reference/statements/select/index.md)查询的结果，并在查询结果更改时随时更新。 查询结果以及与新数据结合所需的部分结果存储在内存中，为重复查询提供更高的性能。当使用[WATCH](../../../sql-reference/statements/watch.md)查询更改查询结果时，实时视图可以提供推送通知。

实时视图是通过插入到查询中指定的最里面的表来触发的。

实时视图的工作方式类似于分布式表中查询的工作方式。 但不是组合来自不同服务器的部分结果，而是将当前数据的部分结果与新数据的部分结果组合在一起。当实时视图查询包含子查询时，缓存的部分结果仅存储在最里面的子查询中。

!!! info "限制"
- [Table function](../../../sql-reference/table-functions/index.md)不支持作为最里面的表.
- 没有插入的表，例如[dictionary](../../../sql-reference/dictionaries/index.md), [system table](../../../operations/system-tables/index.md), [normal view](#normal), [materialized view](#materialized)不会触发实时视图。
- 只有可以将旧数据的部分结果与新数据的部分结果结合起来的查询才有效。 实时视图不适用于需要完整数据集来计算最终结果或必须保留聚合状态的聚合的查询。
- 不适用于在不同节点上执行插入的复制或分布式表。
- 不能被多个表触发。

    [WITH REFRESH](#live-view-with-refresh)强制定期更新实时视图，在某些情况下可以用作解决方法。

### Monitoring Changes {#live-view-monitoring}

您可以使用[WATCH](../../../sql-reference/statements/watch.md)命令监视`LIVE VIEW`查询结果的变化

```sql
WATCH [db.]live_view
```

**示例:**

```sql
CREATE TABLE mt (x Int8) Engine = MergeTree ORDER BY x;
CREATE LIVE VIEW lv AS SELECT sum(x) FROM mt;
```
在对源表进行并行插入时观看实时视图。

```sql
WATCH lv;
```

```bash
┌─sum(x)─┬─_version─┐
│      1 │        1 │
└────────┴──────────┘
┌─sum(x)─┬─_version─┐
│      2 │        2 │
└────────┴──────────┘
┌─sum(x)─┬─_version─┐
│      6 │        3 │
└────────┴──────────┘
...
```

```sql
INSERT INTO mt VALUES (1);
INSERT INTO mt VALUES (2);
INSERT INTO mt VALUES (3);
```

添加[EVENTS](../../../sql-reference/statements/watch.md#events-clause)子句只获取更改事件。

```sql
WATCH [db.]live_view EVENTS;
```

**示例:**

```sql
WATCH lv EVENTS;
```

```bash
┌─version─┐
│       1 │
└─────────┘
┌─version─┐
│       2 │
└─────────┘
┌─version─┐
│       3 │
└─────────┘
...
```

你可以执行[SELECT](../../../sql-reference/statements/select/index.md)与任何常规视图或表格相同的方式查询实时视图。如果查询结果被缓存，它将立即返回结果而不在基础表上运行存储的查询。

```sql
SELECT * FROM [db.]live_view WHERE ...
```

### Force Refresh {#live-view-alter-refresh}

您可以使用`ALTER LIVE VIEW [db.]table_name REFRESH`语法.

### WITH TIMEOUT条件 {#live-view-with-timeout}

当使用`WITH TIMEOUT`子句创建实时视图时，[WATCH](../../../sql-reference/statements/watch.md)观察实时视图的查询。

```sql
CREATE LIVE VIEW [db.]table_name WITH TIMEOUT [value_in_sec] AS SELECT ...
```

如果未指定超时值，则由指定的值[temporary_live_view_timeout](../../../operations/settings/settings.md#temporary-live-view-timeout)决定.

**示例:**

```sql
CREATE TABLE mt (x Int8) Engine = MergeTree ORDER BY x;
CREATE LIVE VIEW lv WITH TIMEOUT 15 AS SELECT sum(x) FROM mt;
```

### WITH REFRESH条件 {#live-view-with-refresh}

当使用`WITH REFRESH`子句创建实时视图时，它将在自上次刷新或触发后经过指定的秒数后自动刷新。

```sql
CREATE LIVE VIEW [db.]table_name WITH REFRESH [value_in_sec] AS SELECT ...
```

如果未指定刷新值，则由指定的值[periodic_live_view_refresh](../../../operations/settings/settings.md#periodic-live-view-refresh)决定.

**示例:**

```sql
CREATE LIVE VIEW lv WITH REFRESH 5 AS SELECT now();
WATCH lv
```

```bash
┌───────────────now()─┬─_version─┐
│ 2021-02-21 08:47:05 │        1 │
└─────────────────────┴──────────┘
┌───────────────now()─┬─_version─┐
│ 2021-02-21 08:47:10 │        2 │
└─────────────────────┴──────────┘
┌───────────────now()─┬─_version─┐
│ 2021-02-21 08:47:15 │        3 │
└─────────────────────┴──────────┘
```

您可以使用`AND`子句组合`WITH TIMEOUT`和`WITH REFRESH`子句。

```sql
CREATE LIVE VIEW [db.]table_name WITH TIMEOUT [value_in_sec] AND REFRESH [value_in_sec] AS SELECT ...
```

**示例:**

```sql
CREATE LIVE VIEW lv WITH TIMEOUT 15 AND REFRESH 5 AS SELECT now();
```

15 秒后，如果没有活动的`WATCH`查询，实时视图将自动删除。

```sql
WATCH lv
```

```
Code: 60. DB::Exception: Received from localhost:9000. DB::Exception: Table default.lv does not exist..
```

### Usage {#live-view-usage}

实时视图表的最常见用途包括：

- 为查询结果更改提供推送通知以避免轮询。
- 缓存最频繁查询的结果以提供即时查询结果。
- 监视表更改并触发后续选择查询。
- 使用定期刷新从系统表中查看指标。

[原始文章](https://clickhouse.tech/docs/en/sql-reference/statements/create/view/) <!--hide-->
