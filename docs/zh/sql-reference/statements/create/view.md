---
sidebar_position: 37
sidebar_label: VIEW
---

# CREATE VIEW {#create-view}

创建一个新视图。 有两种类型的视图：普通视图，物化视图，Live视图和Window视图。

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

删除视图,使用[DROP VIEW](../../../sql-reference/statements/drop#drop-view). `DROP TABLE`也适用于视图。

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

[原始文章](https://clickhouse.com/docs/en/sql-reference/statements/create/view/) <!--hide-->

## Window View [Experimental] {#window-view}

!!! important "重要"
    这是一项试验性功能，可能会在未来版本中以向后不兼容的方式进行更改。
    通过[allow_experimental_window_view](../../../operations/settings/settings.md#allow-experimental-window-view)启用window view以及`WATCH`语句。输入命令
    `set allow_experimental_window_view = 1`。

``` sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [ENGINE = engine] [WATERMARK = strategy] [ALLOWED_LATENESS = interval_function] AS SELECT ... GROUP BY time_window_function
```

Window view可以通过时间窗口聚合数据，并在满足窗口触发条件时自动触发对应窗口计算。其通过将计算状态保存降低处理延迟，支持将处理结果输出至目标表或通过`WATCH`语句输出至终端。

创建window view的方式和创建物化视图类似。Window view使用默认为`AggregatingMergeTree`的内部存储引擎存储计算中间状态。

### 时间窗口函数 {#window-view-shi-jian-chuang-kou-han-shu}

[时间窗口函数](../../functions/time-window-functions.md)用于获取窗口的起始和结束时间。Window view需要和时间窗口函数配合使用。

### 时间属性 {#window-view-shi-jian-shu-xing}

Window view 支持**处理时间**和**事件时间**两种时间类型。

**处理时间**为默认时间类型，该模式下window view使用本地机器时间计算窗口数据。“处理时间”时间类型计算简单，但具有不确定性。该模式下时间可以为时间窗口函数的第一个参数`time_attr`，或通过函数`now()`使用当前机器时间。下面的例子展示了使用“处理时间”创建window view的例子。

``` sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

**事件时间** 是事件真实发生的时间，该时间往往在事件发生时便嵌入数据记录。事件时间处理提供较高的确定性，可以处理乱序数据以及迟到数据。Window view通过水位线(`WATERMARK`)启用事件时间处理。

Window view提供如下三种水位线策略：

* `STRICTLY_ASCENDING`: 提交观测到的最大时间作为水位线，小于最大观测时间的数据不算迟到。
* `ASCENDING`: 提交观测到的最大时间减1作为水位线。小于或等于最大观测时间的数据不算迟到。
* `BOUNDED`: WATERMARK=INTERVAL. 提交最大观测时间减去固定间隔(`INTERVAL`)做为水位线。

以下为使用`WATERMARK`创建window view的示例：

``` sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

通常，窗口会在水位线到达时触发，水位线到达之后的数据会被丢弃。Window view可以通过设置`ALLOWED_LATENESS=INTERVAL`来开启迟到消息处理。示例如下：

``` sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

需要注意的是，迟到消息需要更新之前的处理结果。与在窗口结束时触发不同，迟到消息到达时window view会立即触发计算。因此，会导致同一个窗口输出多次计算结果。用户需要注意这种情况，并消除重复结果。

### 新窗口监控 {#window-view-xin-chuang-kou-jian-kong}

Window view可以通过`WATCH`语句将处理结果推送至终端，或通过`TO`语句将结果推送至数据表。

``` sql
WATCH [db.]name [LIMIT n]
```

`WATCH`语句和`LIVE VIEW`中的类似。支持设置`LIMIT`参数，输出消息数目达到`LIMIT`限制时结束查询。

### 设置 {#window-view-she-zhi}

- `window_view_clean_interval`: window view清除过期数据间隔(单位为秒)。系统会定期清除过期数据，尚未触发的窗口数据不会被清除。
- `window_view_heartbeat_interval`: 用于判断watch查询活跃的心跳时间间隔。

### 示例 {#window-view-shi-li}

假设我们需要每10秒统计一次`data`表中的点击日志，且`data`表的结构如下：

``` sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

首先，使用10秒大小的tumble函数创建window view。

``` sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

随后，我们使用`WATCH`语句获取计算结果。

``` sql
WATCH wv
```

当日志插入表`data`时，

``` sql
INSERT INTO data VALUES(1,now())
```

`WATCH`语句会输出如下结果：

``` text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

或者，我们可以通过`TO`关键字将处理结果输出至另一张表。

``` sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

ClickHouse测试中提供了更多的示例(以`*window_view*`命名)。

### Window View 使用场景 {#window-view-shi-yong-chang-jing}

Window view 在以下场景有用：

* **监控**: 以时间维度聚合及处理数据，并将处理结果输出至目标表。用户可通过目标表获取并操作计算结果。
* **分析**: 以时间维度进行数据分析. 当数据源非常庞大时，window view可以减少重复全表查询的计算量。
