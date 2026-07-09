---
description: 'CREATE VIEW 的文档'
sidebar_label: 'VIEW'
sidebar_position: 37
slug: /sql-reference/statements/create/view
title: 'CREATE VIEW'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import DeprecatedBadge from '@theme/badges/DeprecatedBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="create-view">
  # CREATE VIEW
</div>

创建新的视图。视图可以是[普通视图](#normal-view)、[materialized view](#materialized-view)、[可刷新materialized view](#refreshable-materialized-view)和[窗口视图](/zh/sql-reference/statements/create/view#window-view)。

<div id="normal-view">
  ## 普通视图
</div>

语法：

```sql
CREATE [OR REPLACE] VIEW [IF NOT EXISTS] [db.]table_name [(alias1 [, alias2 ...])] [ON CLUSTER cluster_name]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | INVOKER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

普通视图不存储任何数据。每次访问时，它都只是从另一张表中读取数据。换句话说，普通视图不过是一个已保存的查询。读取视图时，这个已保存的查询会作为 [FROM](../../../sql-reference/statements/select/from.md) 子句中的子查询使用。

例如，假设你已经创建了一个视图：

```sql
CREATE VIEW view AS SELECT ...
```

并编写了如下查询：

```sql
SELECT a, b, c FROM view
```

该查询与使用该子查询完全等价：

```sql
SELECT a, b, c FROM (SELECT ...)
```

<div id="parameterized-view">
  ## 参数化视图
</div>

参数化视图与普通视图类似，但可在创建时定义不会立即解析的参数。这类视图可配合表函数使用：将视图名称作为函数名，并将参数值作为其参数传入。

```sql
CREATE VIEW view AS SELECT * FROM TABLE WHERE Column1={column1:datatype1} and Column2={column2:datatype2} ...
```

上述语句会为该表创建一个视图；按如下所示替换参数后，即可将其作为表函数使用。

```sql
SELECT * FROM view(column1=value1, column2=value2 ...)
```

<div id="materialized-view">
  ## Materialized View
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]table_name [ON CLUSTER cluster_name] [TO[db.]name [(columns)]] [ENGINE = engine] [POPULATE]
[REFRESH ...]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

`OR REPLACE` 和 `IF NOT EXISTS` 不能同时使用：将二者结合使用会产生语法错误。

<div id="create-or-replace-materialized-view">
  ### CREATE OR REPLACE MATERIALIZED VIEW
</div>

`CREATE OR REPLACE MATERIALIZED VIEW` 会以原子方式替换现有的 materialized view 及其内部存储表 (如有) 。该操作要求数据库 engine 为 `Atomic` 或 `Replicated`。

```sql
CREATE OR REPLACE MATERIALIZED VIEW [db.]name [ON CLUSTER cluster]
[TO [db.]target_table]
[ENGINE = engine]
[POPULATE]
[REFRESH ...]
AS SELECT ...
```

关键行为：

* **不带 `TO` 子句**：旧的内部表会被删除，并创建一个新的内部表。除非指定了 `POPULATE`，否则内部表中的现有数据将会丢失。
* **带 `TO` 子句**：只会替换视图定义；目标表及其数据不受影响。
* 兼容 `REFRESH`、`ON CLUSTER` 以及所有引擎选项。`POPULATE` 仅在 `Atomic` 数据库中受支持——在 `Replicated` 数据库中会被拒绝 (请参见下方关于 `POPULATE` 的说明) 。
* 需要 `CREATE VIEW` 和 `DROP VIEW` 特权。

:::note
`CREATE OR REPLACE MATERIALIZED VIEW` 仅支持用于 `Atomic` 或 `Replicated` 数据库引擎。不支持 `Ordinary` 数据库引擎。
:::

**示例：**

```sql
-- Create a materialized view with an inner table
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, sum(y) AS total FROM src GROUP BY x;

-- Replace with a new definition (old inner table data is lost)
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    AS SELECT x, count() AS cnt FROM src GROUP BY x;

-- Replace with POPULATE to backfill from existing source data
CREATE OR REPLACE MATERIALIZED VIEW mv
    ENGINE = MergeTree ORDER BY x
    POPULATE
    AS SELECT x FROM src;

-- Replace an inner-table MV with a TO-table MV (target data is preserved)
CREATE OR REPLACE MATERIALIZED VIEW mv TO target
    AS SELECT x FROM src;
```

:::tip
这里提供了关于如何使用 [Materialized views](/zh/guides/developer/cascading-materialized-views.md) 的分步指南。
:::

Materialized views 会存储由相应的 [SELECT](../../../sql-reference/statements/select/index.md) 查询转换后的数据。

创建不带 `TO [db].[table]` 的 materialized view 时，必须指定 `ENGINE`——即用于存储数据的表引擎。

创建带有 `TO [db].[table]` 的 materialized view 时，不能同时使用 `POPULATE`。

materialized view 的实现方式如下：向 `SELECT` 中指定的表插入数据时，插入数据中的一部分会通过该 `SELECT` 查询进行转换，结果再插入到视图中。

:::note
ClickHouse 中的 Materialized views 在插入目标表时，依据的是**列名**而不是列顺序。如果 `SELECT` 查询结果中缺少某些列名，ClickHouse 会使用默认值，即使该列不是 [Nullable](../../data-types/nullable.md) 类型。稳妥的做法是在使用 Materialized views 时为每一列都添加别名。

ClickHouse 中的 Materialized views 更像是插入触发器。如果视图查询中包含聚合，则聚合只会应用于这一批新插入的数据。对源表中已有数据的任何更改 (如 update、delete、drop partition 等) 都不会影响 materialized view。

ClickHouse 中的 Materialized views 在发生错误时不具有确定性行为。这意味着，已经写入的块会保留在目标表中，而出错后的所有块都不会写入。

默认情况下，如果向某个视图推送时 throws，`INSERT` 查询就会失败。此时该块是否已经到达源表并不保证——这取决于插入管道的时序，而不是视图错误。请使用插入去重 (`insert_deduplicate`、`deduplicate_blocks_in_dependent_materialized_views`) 重试失败的 `INSERT`，以实现向源表及所有依赖视图的 exactly-once 传递。

在 `INSERT` 查询上设置 `materialized_views_ignore_errors=true` 只会改变错误报告方式：每个视图错误都会以警告形式记入日志，而 `INSERT` 查询仍会成功。发送到发生故障的视图目标端的数据只会部分完成——异常发生前已处理的块会被保留，而出错的块及其后该视图中的所有块都会被丢弃。该目标端下游的视图只能看到实际到达的那些块，因此它们的数据传递也同样只是部分完成。未抛出异常的同级视图 (以及它们各自的下游链路) 会被完整写入，而源表也会照常写入。由于 `INSERT` 会报告成功，客户端收不到失败信号，也不会触发自动重试；只有在源表写入绝不能被视图侧问题阻塞时，才应使用此设置 (例如 `system.*_log` 表) 。

对于 `system.*_log` 表，`materialized_views_ignore_errors` 默认值为 `true`。
:::

如果指定了 `POPULATE`，创建视图时会将现有表中的数据插入到视图中，就像执行 `CREATE TABLE ... AS SELECT ...` 一样。否则，查询中只会包含在视图创建之后插入到表中的数据。我们**不建议**使用 `POPULATE`，因为在视图创建期间插入到表中的数据不会被插入到视图中。

:::note
由于 `POPULATE` 的工作方式类似 `CREATE TABLE ... AS SELECT ...`，因此它有以下限制：

* Replicated database 不支持
* ClickHouse Cloud 不支持

可以改用单独的 `INSERT ... SELECT`。
:::

`SELECT` 查询可以包含 `DISTINCT`、`GROUP BY`、`ORDER BY`、`LIMIT`。请注意，相应的转换是对每个插入数据块独立执行的。例如，如果设置了 `GROUP BY`，数据会在插入期间聚合，但仅限于单个插入数据包内，不会再做进一步聚合。例外情况是使用可自行执行数据聚合的 `ENGINE`，例如 `SummingMergeTree`。

如果 materialized view 使用了 `TO [db.]name` 这种写法，你可以先 `DETACH` 该视图，对目标表执行 `ALTER`，然后再 `ATTACH` 之前已分离 (`DETACH`) 的视图。

请注意，materialized view 会受到 [optimize&#95;on&#95;insert](/zh/operations/settings/settings#optimize_on_insert) 设置的影响。数据会先合并，再插入到视图中。

视图看起来与普通表相同。例如，它们会列在 `SHOW TABLES` 查询的结果中。

要删除视图，请使用 [DROP VIEW](../../../sql-reference/statements/drop.md#drop-view)。不过，`DROP TABLE` 对 VIEW 同样适用。

<div id="sql_security">
  ## SQL security
</div>

`DEFINER` 和 `SQL SECURITY` 允许你指定在执行视图底层查询时使用哪个 ClickHouse 用户。
`SQL SECURITY` 有三个合法取值：`DEFINER`、`INVOKER` 或 `NONE`。你可以在 `DEFINER` 子句中指定任意现有用户或 `CURRENT_USER`。

下表说明了从视图中查询时，不同用户分别需要哪些权限。
请注意，无论采用哪种 SQL security 选项，在任何情况下，仍然需要具备 `GRANT SELECT ON <view>` 才能从中读取。

| SQL security option | View                            | Materialized View                                      |
| ------------------- | ------------------------------- | ------------------------------------------------------ |
| `DEFINER alice`     | `alice` 必须对视图的源表拥有 `SELECT` 权限。 | `alice` 必须对视图的源表拥有 `SELECT` 权限，并对视图的目标表拥有 `INSERT` 权限。 |
| `INVOKER`           | 用户必须对视图的源表拥有 `SELECT` 权限。       | materialized view 不能指定 `SQL SECURITY INVOKER`。         |
| `NONE`              | -                               | -                                                      |

:::note
`SQL SECURITY NONE` 是一个已弃用选项。任何有权创建带有 `SQL SECURITY NONE` 的视图的用户，都能够执行任意查询。
因此，要使用此选项创建视图，必须具备 `GRANT ALLOW SQL SECURITY NONE TO <user>`。
:::

如果未指定 `DEFINER`/`SQL SECURITY`，则会使用默认值：

* `SQL SECURITY`：普通视图为 `INVOKER`，materialized view 为 `DEFINER` ([可通过设置配置](../../../operations/settings/settings.md#default_normal_view_sql_security))
* `DEFINER`：`CURRENT_USER` ([可通过设置配置](../../../operations/settings/settings.md#default_view_definer))

如果视图在附加时未指定 `DEFINER`/`SQL SECURITY`，则 materialized view 的默认值为 `SQL SECURITY NONE`，普通视图的默认值为 `SQL SECURITY INVOKER`。

要更改现有视图的 SQL security，请使用

```sql
ALTER TABLE MODIFY SQL SECURITY { DEFINER | INVOKER | NONE } [DEFINER = { user | CURRENT_USER }]
```

<div id="examples">
  ### 示例
</div>

```sql
CREATE VIEW test_view
DEFINER = alice SQL SECURITY DEFINER
AS SELECT ...
```

```sql
CREATE VIEW test_view
SQL SECURITY INVOKER
AS SELECT ...
```

<div id="live-view">
  ## Live View
</div>

<DeprecatedBadge />

该功能已弃用，后续将被移除。

为方便查阅，旧版文档见[此处](https://pastila.nl/?00f32652/fdf07272a7b54bda7e13b919264e449f.md)

<div id="refreshable-materialized-view">
  ## 可刷新 materialized view
</div>

```sql
CREATE MATERIALIZED VIEW [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
REFRESH [EVERY|AFTER interval [OFFSET interval]]
[RANDOMIZE FOR interval]
[DEPENDS ON [db.]name [, [db.]name [, ...]]]
[SETTINGS name = value [, name = value [, ...]]]
[APPEND]
[TO[db.]name] [(columns)] [ENGINE = engine]
[EMPTY]
[DEFINER = { user | CURRENT_USER }] [SQL SECURITY { DEFINER | NONE }]
AS SELECT ...
[COMMENT 'comment']
```

其中，`interval` 是由若干简单时间间隔组成的序列：

```sql
number SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR
```

`REFRESH` 子句必须至少指定 `EVERY`、`AFTER` 或 `DEPENDS ON` 之一。单独使用 `REFRESH` (即不带其中任何一项) 会被拒绝。不带 `EVERY`/`AFTER` 的 `REFRESH DEPENDS ON ...` 是 `REFRESH AFTER 0 SECOND DEPENDS ON ...` 的简写；请参见下方的[刷新依赖](#refresh-dependencies)。

它会定期运行相应的查询，并将结果存储到表中。

* 如果指定了 `APPEND`，每次刷新都会向表中插入行，而不会删除现有行。该插入不是原子的，与普通的 `INSERT INTO ... SELECT` 查询一样。
* 否则，每次刷新都会以原子方式替换表中原有的内容。

与普通的非可刷新 materialized view 的区别：

* 没有插入触发器。当新数据插入到 `SELECT` 中指定的表时，*不会*自动写入可刷新materialized view。相反，只有在定期刷新或手动刷新执行时才会写入数据。
* `SELECT` 查询不受限制。表函数 (例如 `url()`) 、视图、UNION、JOIN 都允许使用。

:::note
查询中 `REFRESH ... SETTINGS` 部分里的 settings 是 refresh settings (例如 `refresh_retries`) ，与常规 settings (例如 `max_threads`) 不同。常规 settings 可以通过在查询末尾使用 `SETTINGS` 来指定。
:::

<div id="refresh-schedule">
  ### 刷新调度
</div>

刷新调度示例：

```sql
REFRESH EVERY 1 DAY -- every day, at midnight (UTC)
REFRESH EVERY 1 MONTH -- on 1st day of every month, at midnight
REFRESH EVERY 1 MONTH OFFSET 5 DAY 2 HOUR -- on 6th day of every month, at 2:00 am
REFRESH EVERY 2 WEEK OFFSET 5 DAY 15 HOUR 10 MINUTE -- every other Saturday, at 3:10 pm
REFRESH EVERY 30 MINUTE -- at 00:00, 00:30, 01:00, 01:30, etc
REFRESH AFTER 30 MINUTE -- 30 minutes after the previous refresh completes, no alignment with time of day
-- REFRESH AFTER 1 HOUR OFFSET 1 MINUTE -- syntax error, OFFSET is not allowed with AFTER
REFRESH EVERY 1 WEEK 2 DAYS -- every 9 days, not on any particular day of the week or month;
                            -- specifically, when day number (since 1969-12-29) is divisible by 9
REFRESH EVERY 5 MONTHS -- every 5 months, different months each year (as 12 is not divisible by 5);
                       -- specifically, when month number (since 1970-01) is divisible by 5
```

`RANDOMIZE FOR` 会随机调整每次刷新的时间，例如：

```sql
REFRESH EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 1 HOUR -- every day at random time between 01:30 and 02:30
```

对于给定视图，同时最多只能有一个刷新在运行。例如，如果一个带有 `REFRESH EVERY 1 MINUTE` 的视图需要 2 分钟才能完成刷新，那么它实际上只会每 2 分钟刷新一次。如果之后它变快了，并开始在 10 秒内完成刷新，那么它就会恢复为每分钟刷新一次。 (特别地，它不会为了补上之前错过的刷新而每 10 秒刷新一次——并不存在这样的积压。)

通常，第一次刷新会在 materialized view 创建后立即开始：由于距离上次刷新的时间可视为无穷大，因此无论是什么 schedule，都会认为现在应该刷新。如果指定了 `EMPTY`，则会跳过这次初始刷新，第一次刷新会在下一个计划时间发生；例如，对于 `EVERY 1 HOUR`，第一次刷新会在当前小时结束时发生。

<div id="in-replicated-db">
  ### 在 Replicated DB 中
</div>

如果可刷新materialized view 位于 [Replicated 数据库](../../../engines/database-engines/replicated.md) 中，各个副本会相互协调，因此在每个计划的刷新时间点只会由一个副本执行刷新。这里要求使用 [ReplicatedMergeTree](../../../engines/table-engines/mergetree-family/replication.md) 表引擎，这样所有副本都能看到刷新生成的数据。

在 `APPEND` 模式下，可以使用 `SETTINGS all_replicas = 1` 禁用协调。这样各个副本会彼此独立地执行刷新。在这种情况下，不要求使用 ReplicatedMergeTree。

在非 `APPEND` 模式下，仅支持协调刷新。若要使用非协调方式，请使用 `Atomic` database 和 `CREATE ... ON CLUSTER` 查询，在所有副本上创建可刷新materialized view。

协调通过 Keeper 完成。znode 路径由 [default&#95;replica&#95;path](../../../operations/server-configuration-parameters/settings.md#default_replica_path) server setting 决定。

<div id="refresh-dependencies">
  ### 刷新依赖关系
</div>

`DEPENDS ON` 可用于同步不同表的刷新：

```sql
CREATE MATERIALIZED VIEW dependent REFRESH EVERY 1 HOUR DEPENDS ON dependency [...]
```

依赖视图的刷新会在所有被依赖视图完成刷新后才开始。

若要在另一个视图刷新后立即触发刷新：

```sql
CREATE MATERIALIZED VIEW dependent REFRESH AFTER 0 SECOND DEPENDS ON dependency [...]
```

或者，等价地说：

```sql
CREATE MATERIALIZED VIEW dependent REFRESH DEPENDS ON dependency [...]
```

:::note
`DEPENDS ON` 仅适用于可刷新materialized view 之间。尤其要注意，如果依赖视图使用了 `TO <table>`，请务必使用视图名而不是表名。如果 `DEPENDS ON` 列表中包含普通表、不可刷新的视图，或者存在拼写错误，该视图将永远不会刷新，并会在 `system.view_refreshes` 中显示状态 `MissingDependencies`。可以使用 `ALTER` 更改或移除依赖关系，参见 [更改刷新参数](#changing-refresh-parameters)。
:::

<div id="using-depends-on-for-consistent-propagation-latency">
  #### 使用 DEPENDS ON 保持一致的传播延迟
</div>

如果两个视图都以相同的周期使用 `REFRESH EVERY`，那么该依赖关系会在每个时间段内生效。

例如，假设视图 X 和 Y 都使用 `REFRESH EVERY 1 HOUR`，并且 Y 从 X 的输出表读取数据。如果没有依赖关系，Y 通常看到的是 X 在前一个小时刷新产生的数据。使用 `DEPENDS ON X` 后，Y 在 11:00 的刷新只会在 X 于 11:00 的刷新完成后才开始。

```text
           10:00            11:00            12:00
           │                │                │
  X:        [run]┐           [run]┐           [run]┐
                 │                │                │
  Y:             └►[run]          └►[run]          └►[run]
```

如果刷新耗时长于刷新周期，被依赖对象和依赖它的对象都可能各自跳过某些时间片。无法保证依赖方会在被依赖对象每次刷新后都恰好刷新一次。

```text
           10:00          11:00          12:00          13:00
           │              │              │              |
  X:        [run]┐         [run]┐         [run]┐         [run]┐
                 │              └────┐    (Y skips 12:00)     └───┐
  Y:             └►[10:00 ru------un]└►[11:00 ru---------------un]└►[13:00 run]
```

<div id="using-depends-on-for-batched-stream-processing">
  #### 使用 DEPENDS ON 进行批次式 stream 处理
</div>

如果未使用 `REFRESH EVERY`，则依赖视图 X 会在自上次刷新以来其所有依赖项都至少刷新过一次后进行刷新。`REFRESH AFTER T` 则会增加一个延迟：依赖视图会在其依赖项完成刷新后的 T 时间后开始刷新。

允许循环依赖，而且这很有用。请看下面这个由可刷新materialized view 构成的图：

1. X 从某个 stream 中取出一批行，并将其写入一个表中。
2. 然后，Y 和 Z 都从该表中读取数据，执行不同的聚合，并将结果追加到其他表中。
3. 在该批次被完全处理后，X 会取出下一批次，然后循环重复。

```text
            source
               │
               ▼
          ┌─────────┐
     ┌───►│    X    │◄───┐
     │    └──┬───┬──┘    │
  DEPENDS    │   │    DEPENDS
    ON       ▼   ▼      ON
     │      ┌─┐ ┌─┐      │
     └──────┤Y│ │Z├──────┘
            └─┘ └─┘
```

完整示例：

```sql
CREATE TABLE current_batch (t UInt64, v Int64) ENGINE ReplicatedMergeTree ORDER BY t;
CREATE TABLE batch_log (max_t UInt64, n Int64, v_sum Int64, processed_at DateTime64) ENGINE ReplicatedMergeTree ORDER BY max_t;
CREATE TABLE stats (h UInt64, n UInt64) ENGINE ReplicatedSummingMergeTree ORDER BY h;

-- (system.numbers stands in for a data source with monotonically increasing timestamps or sequence numbers)
CREATE MATERIALIZED VIEW current_batch_v REFRESH EVERY 10 SECOND DEPENDS ON batch_log_v, stats_v TO current_batch AS SELECT number as t, number * 10 as v FROM system.numbers WHERE number > (SELECT max(max_t) FROM batch_log) LIMIT 100;

CREATE MATERIALIZED VIEW batch_log_v REFRESH DEPENDS ON current_batch_v APPEND TO batch_log AS SELECT max(t) as max_t, count() as n, sum(v) as v_sum, now64() as processed_at FROM current_batch;

CREATE MATERIALIZED VIEW stats_v REFRESH DEPENDS ON current_batch_v APPEND TO stats AS SELECT cityHash64(v) % 20 as h, count() as n FROM current_batch GROUP BY h;

-- Must trigger initial refresh manually.
SYSTEM REFRESH VIEW current_batch_v;
```

更长的事件链同样也可行。

只有在启用刷新协调时，这种做法才能正常工作，也就是说，这些视图位于 Replicated 或 Shared database 中。没有协调时，服务器重启会中断这一循环，因此每次重启后都需要手动执行一次 `SYSTEM REFRESH VIEW`，而不是只需在创建这些视图后执行一次。

<div id="refresh-settings">
  ### 刷新设置
</div>

可用的刷新设置：

* `refresh_retries` - 如果刷新查询因异常失败，允许重试的次数。如果所有重试都失败，则跳过并等待下一次计划刷新时间。0 表示不重试，-1 表示无限重试。默认值：2。
* `refresh_retry_initial_backoff_ms` - 如果 `refresh_retries` 不为 0，首次重试前的延迟时间。此后每次重试的延迟都会翻倍，直到达到 `refresh_retry_max_backoff_ms`。默认值：100 毫秒。
* `refresh_retry_max_backoff_ms` - 刷新尝试之间延迟按指数增长时的上限。默认值：60000 毫秒 (1 分钟) 。
* `all_replicas` - 在使用 `APPEND` 的 [Replicated 数据库](../../../engines/database-engines/replicated.md)中，用于控制是所有副本各自独立刷新，还是每个计划时间点仅由一个副本执行刷新。创建视图后无法更改。默认值：`false`。

<div id="changing-refresh-parameters">
  ### 更改刷新参数
</div>

可使用 [`ALTER TABLE ... MODIFY REFRESH`](../alter/view.md#alter-table--modify-refresh-statement) 更改现有可刷新materialized view的刷新参数：

```sql
ALTER TABLE [db.]name MODIFY REFRESH EVERY|AFTER ... [RANDOMIZE FOR ...] [DEPENDS ON ...] [SETTINGS ...]
```

调度 (`EVERY` 或 `AFTER`) 是必填项：该语句始终会用指定内容替换*所有*刷新参数——包括调度、`RANDOMIZE FOR`、`DEPENDS ON` 以及刷新设置。凡是省略的内容，都会被重置为默认值 (设置) 或移除 (依赖项、随机化) 。

:::note

* 如果只想修改刷新设置 (例如 `refresh_retries`) ，请重复现有调度：

  ```sql
  ALTER TABLE rmv MODIFY REFRESH EVERY 1 HOUR SETTINGS refresh_retries = 5;
  ```

* `ALTER TABLE ... MODIFY SETTING refresh_retries = ...` 在 materialized view 上不受支持；必须通过 `MODIFY REFRESH` 进行修改。

* 不支持添加或移除 `APPEND`。

* `all_replicas` 设置在创建后无法更改。
  :::

示例：

```sql
-- Change the schedule, drop existing settings and dependencies.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE;

-- Change the schedule and tune retry behavior.
ALTER TABLE rmv MODIFY REFRESH EVERY 30 MINUTE
SETTINGS refresh_retries = 5,
         refresh_retry_initial_backoff_ms = 500,
         refresh_retry_max_backoff_ms = 60000;

-- Keep the dependency while changing the period.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR DEPENDS ON other_rmv;

-- Drop the dependency by omitting `DEPENDS ON`.
ALTER TABLE rmv MODIFY REFRESH EVERY 6 HOUR;
```

<div id="other-operations">
  ### 其他操作
</div>

所有可刷新materialized view 的状态都可以在表 [`system.view_refreshes`](../../../operations/system-tables/view_refreshes.md) 中查看。具体包括刷新进度 (如果正在运行) 、上次和下一次刷新时间，以及刷新失败时的异常信息。

如需手动停止、启动、触发或取消刷新，请使用 [`SYSTEM STOP|START|REFRESH|WAIT|CANCEL VIEW`](../system.md#managing-refreshable-materialized-views)。

如需等待刷新完成，请使用 [`SYSTEM WAIT VIEW`](../system.md#wait-view)。这在创建视图后等待首次刷新时尤其有用。

:::note
有个有趣的事实：刷新查询可以从正在刷新的视图中读取数据，看到的是刷新前版本的数据。这意味着你可以实现康威生命游戏：https://pastila.nl/?00021a4b/d6156ff819c83d490ad2dcec05676865#O0LGWTO7maUQIA4AcGUtlA==
:::

<div id="window-view">
  ## Window View
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

:::info
这是一个 Experimental 功能，未来的发行版中可能会以不向后兼容的方式发生变更。请使用 [allow&#95;experimental&#95;window&#95;view](/zh/operations/settings/settings#allow_experimental_window_view) 设置启用 Window View 和 `WATCH` 查询。输入命令 `set allow_experimental_window_view = 1`。
:::

```sql
CREATE WINDOW VIEW [IF NOT EXISTS] [db.]table_name [TO [db.]table_name] [INNER ENGINE engine] [ENGINE engine] [WATERMARK strategy] [ALLOWED_LATENESS interval_function] [POPULATE]
AS SELECT ...
GROUP BY time_window_function
[COMMENT 'comment']
```

窗口视图可以按时间窗口聚合数据，并在窗口满足触发条件时输出结果。它会将部分聚合结果存储在内部 (或指定的) 表中以降低延迟，也可以将处理结果推送到指定表，或使用 `WATCH` 查询推送通知。

创建窗口视图与创建 `MATERIALIZED VIEW` 类似。窗口视图需要一个内部存储引擎来保存中间数据。可以使用 `INNER ENGINE` 子句指定内部存储，窗口视图默认使用 `AggregatingMergeTree` 作为内部引擎。

创建不带 `TO [db].[table]` 的窗口视图时，必须指定 `ENGINE`——即用于存储数据的表引擎。

<div id="time-window-functions">
  ### 时间窗口函数
</div>

[时间窗口函数](../../functions/time-window-functions.md)用于获取记录的窗口上下边界。窗口视图需要与时间窗口函数配合使用。

<div id="time-attributes">
  ### 时间属性
</div>

Window view 支持基于 **处理时间** 和 **事件时间** 进行处理。

**处理时间** 允许 Window view 基于本地机器时间生成结果，默认使用这种方式。它是最直观的时间概念，但不具备确定性。处理时间属性可以通过将时间窗口函数的 `time_attr` 设置为表中的某一列，或使用函数 `now()` 来定义。以下查询创建了一个使用处理时间的 Window view。

```sql
CREATE WINDOW VIEW wv AS SELECT count(number), tumbleStart(w_id) as w_start from date GROUP BY tumble(now(), INTERVAL '5' SECOND) as w_id
```

**事件时间**是指每个事件在其源设备上实际发生的时间。这个时间通常会在记录生成时嵌入其中。事件时间处理即使在事件乱序或延迟到达的情况下，也能得到一致的结果。窗口视图通过 `WATERMARK` 语法支持事件时间处理。

窗口视图提供三种水位线策略：

* `STRICTLY_ASCENDING`：发出截至当前观测到的最大时间戳作为水位线。时间戳小于该最大时间戳的行不视为迟到数据。
* `ASCENDING`：发出截至当前观测到的最大时间戳减 1 作为水位线。时间戳等于或小于该最大时间戳的行不视为迟到数据。
* `BOUNDED`：WATERMARK=INTERVAL。发出水位线，其值为当前观测到的最大时间戳减去指定的延迟。

以下查询展示了如何使用 `WATERMARK` 创建窗口视图：

```sql
CREATE WINDOW VIEW wv WATERMARK=STRICTLY_ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=ASCENDING AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
CREATE WINDOW VIEW wv WATERMARK=INTERVAL '3' SECOND AS SELECT count(number) FROM date GROUP BY tumble(timestamp, INTERVAL '5' SECOND);
```

默认情况下，当水位线到达时，窗口会被触发，晚于水位线到达的元素将被丢弃。Window View 支持通过设置 `ALLOWED_LATENESS=INTERVAL` 处理迟到事件。迟到事件处理示例如下：

```sql
CREATE WINDOW VIEW test.wv TO test.dst WATERMARK=ASCENDING ALLOWED_LATENESS=INTERVAL '2' SECOND AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM test.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND) AS wid;
```

请注意，延迟触发发出的元素应视为此前一次计算结果的更新。窗口视图不会在窗口结束时触发，而是会在延迟事件到达时立即触发。因此，同一个窗口会产生多次输出。Users 需要将这些重复结果考虑在内，或对其进行去重。

你可以使用 `ALTER TABLE ... MODIFY QUERY` 语句来修改窗口视图中指定的 `SELECT` 查询。无论是否带有 `TO [db.]name` 子句，新 `SELECT` 查询生成的数据结构都应与原始 `SELECT` 查询保持一致。请注意，当前窗口中的数据将会丢失，因为中间状态无法复用。

<div id="monitoring-new-windows">
  ### 监控新窗口
</div>

窗口视图支持使用 [WATCH](../../../sql-reference/statements/watch.md) 查询来监控变更，也可以使用 `TO` 语法将结果输出到表中。

```sql
WATCH [db.]window_view
[EVENTS]
[LIMIT n]
[FORMAT format]
```

可以指定 `LIMIT`，以设置在终止查询前接收更新的次数。`EVENTS` 子句可用于获取 `WATCH` 查询的简化形式：在这种形式下，你不会得到查询结果，而只会得到最新的查询水位线。

<div id="settings-1">
  ### 设置
</div>

* `window_view_clean_interval`：window view 的清理间隔，单位为秒，用于清理过期数据。系统会根据系统时间或 `WATERMARK` 配置，保留尚未完全触发的窗口，其他数据将被删除。
* `window_view_heartbeat_interval`：心跳间隔，单位为秒，用于表明 watch 查询仍处于活动状态。
* `wait_for_window_view_fire_signal_timeout`：在事件时间处理中等待 window view 触发信号的超时时间。

<div id="examples">
  ### 示例
</div>

假设需要统计名为 `data` 的日志表中每 10 秒的点击日志数量，其表结构如下：

```sql
CREATE TABLE data ( `id` UInt64, `timestamp` DateTime) ENGINE = Memory;
```

首先，我们创建一个窗口视图，使用 10 秒时间间隔的翻滚窗口：

```sql
CREATE WINDOW VIEW wv as select count(id), tumbleStart(w_id) as window_start from data group by tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

然后，使用 `WATCH` 查询获取结果。

```sql
WATCH wv
```

当日志写入表 `data` 时，

```sql
INSERT INTO data VALUES(1,now())
```

`WATCH` 查询应输出如下结果：

```text
┌─count(id)─┬────────window_start─┐
│         1 │ 2020-01-14 16:56:40 │
└───────────┴─────────────────────┘
```

或者，我们也可以使用 `TO` 语法将输出写入另一个表。

```sql
CREATE WINDOW VIEW wv TO dst AS SELECT count(id), tumbleStart(w_id) as window_start FROM data GROUP BY tumble(timestamp, INTERVAL '10' SECOND) as w_id
```

更多示例可在 ClickHouse 的有状态测试用例中找到 (其中名称为 `*window_view*`) 。

<div id="window-view-usage">
  ### 窗口视图的用途
</div>

窗口视图适用于以下场景：

* **监控**：按时间对日志指标进行聚合和计算，并将结果输出到目标表。仪表板可以将目标表用作源表。
* **分析**：在时间窗口内自动聚合并预处理数据。这在分析大量日志时非常有用。预处理可避免多个查询中的重复计算，并降低查询延迟。

<div id="related-content">
  ## 相关内容
</div>

* 博客：[在 ClickHouse 中处理时间序列数据](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* 博客：[使用 ClickHouse 构建可观测性解决方案：第 2 部分：链路追踪](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)

<div id="temporary-views">
  ## 临时视图
</div>

ClickHouse 支持 **临时视图**，具有以下特性 (在适用情况下与临时表一致) ：

* **会话级生命周期**
  临时视图仅在当前会话期间存在。会话结束时会自动删除。

* **无数据库**
  你**不能**使用数据库名称限定临时视图。它独立于数据库存在 (位于会话命名空间中) 。

* **不复制 / 不支持 ON CLUSTER**
  临时对象仅限当前会话本地使用，**不能**通过 `ON CLUSTER` 创建。

* **名称解析**
  如果某个临时对象 (表或视图) 与持久对象同名，且查询在**不带**数据库名的情况下引用该名称，则会使用**临时**对象。

* **逻辑对象 (无存储)&#x20;**&#xA;临时视图只存储其 `SELECT` 文本 (内部使用 `View` 存储) 。它不会持久化数据，也不能接受 `INSERT`。

* **engine 子句**
  你**无需**指定 `ENGINE`；如果写成 `ENGINE = View`，也会被忽略/视为同一个逻辑视图。

* **安全 / 特权**
  创建临时视图需要 `CREATE TEMPORARY VIEW` 特权，而 `CREATE VIEW` 会隐式授予该特权。

* **SHOW CREATE**
  使用 `SHOW CREATE TEMPORARY VIEW view_name;` 可输出临时视图的 DDL。

<div id="temporary-views-syntax">
  ### 语法
</div>

```sql
CREATE TEMPORARY VIEW [IF NOT EXISTS] view_name AS <select_query>
```

临时视图不支持 `OR REPLACE` (以保持与临时表一致) 。如果需要“替换”临时视图，请先将其 drop，再重新创建。

<div id="examples">
  ### 示例
</div>

创建一个临时源表，并基于它创建一个临时视图：

```sql
CREATE TEMPORARY TABLE t_src (id UInt32, val String);
INSERT INTO t_src VALUES (1, 'a'), (2, 'b');

CREATE TEMPORARY VIEW tview AS
SELECT id, upper(val) AS u
FROM t_src
WHERE id <= 2;

SELECT * FROM tview ORDER BY id;
```

查看其 DDL：

```sql
SHOW CREATE TEMPORARY VIEW tview;
```

将其删除：

```sql
DROP TEMPORARY VIEW IF EXISTS tview;  -- temporary views are dropped with TEMPORARY TABLE syntax
```

<div id="temporary-views-limitations">
  ### 不允许 / 限制
</div>

* `CREATE OR REPLACE TEMPORARY VIEW ...` → **不允许** (请使用 `DROP` + `CREATE`) 。
* `CREATE TEMPORARY MATERIALIZED VIEW ...` / `WINDOW VIEW` → **不允许**。
* `CREATE TEMPORARY VIEW db.view AS ...` → **不允许** (不能使用数据库限定符) 。
* `CREATE TEMPORARY VIEW view ON CLUSTER 'name' AS ...` → **不允许** (临时对象仅限当前 session) 。
* `POPULATE`、`REFRESH`、`TO [db.table]`、内部引擎以及所有 MV 专用子句 → **不适用于**临时视图。

<div id="temporary-views-distributed-notes">
  ### 关于分布式查询的说明
</div>

临时**视图**只是一项定义，本身不包含可传递的数据。如果你的临时视图引用了临时**表** (例如 `Memory`) ，那么这些表中的数据可以在分布式查询执行期间像临时表一样传送到远程服务器。

<div id="temporary-views-distributed-example">
  #### 示例
</div>

```sql
-- A session-scoped, in-memory table
CREATE TEMPORARY TABLE temp_ids (id UInt64) ENGINE = Memory;

INSERT INTO temp_ids VALUES (1), (5), (42);

-- A session-scoped view over the temp table (purely logical)
CREATE TEMPORARY VIEW v_ids AS
SELECT id FROM temp_ids;

-- Replace 'test' with your cluster name.
-- GLOBAL JOIN forces ClickHouse to *ship* the small join-side (temp_ids via v_ids)
-- to every remote server that executes the left side.
SELECT count()
FROM cluster('test', system.numbers) AS n
GLOBAL ANY INNER JOIN v_ids USING (id)
WHERE n.number < 100;

```