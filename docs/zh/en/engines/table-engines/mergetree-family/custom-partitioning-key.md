---
description: '了解如何为 MergeTree 表添加自定义分区键。'
sidebar_label: '自定义分区键'
sidebar_position: 30
slug: /engines/table-engines/mergetree-family/custom-partitioning-key
title: '自定义分区键'
doc_type: 'guide'
---

:::note
在大多数情况下，你不需要分区键；而在其他大多数情况下，除非是面向可观测性用例 (此时按天分区很常见) ，否则也不需要比按月更细的分区键。

切勿使用过细的分区粒度。不要按客户端标识符或名称对数据进行分区。相反，应将客户端标识符或名称作为 ORDER BY 表达式中的第一列。
:::

[MergeTree 家族表](../../../engines/table-engines/mergetree-family/mergetree.md)支持分区，包括[复制表](../../../engines/table-engines/mergetree-family/replication.md)和 [materialized view](/zh/sql-reference/statements/create/view#materialized-view)。

分区是按指定条件对表中记录进行的逻辑分组。你可以按任意条件设置分区，例如按月、按天或按事件类型。每个分区都会单独存储，以简化对此类数据的操作。访问数据时，ClickHouse 会尽可能只使用最小范围的分区子集。对于包含分区键的查询，分区可以提升性能，因为 ClickHouse 会先按该分区进行过滤，然后再选择该分区中的 parts 和粒度。

分区是在[创建表](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)时通过 `PARTITION BY expr` 子句指定的。分区键可以是基于表中列的任意表达式。例如，要指定按月分区，请使用表达式 `toYYYYMM(date_column)`：

```sql
CREATE TABLE visits
(
    VisitDate Date,
    Hour UInt8,
    ClientID UUID
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(VisitDate)
ORDER BY Hour;
```

分区键也可以是由多个表达式组成的 Tuple (类似于[主键](../../../engines/table-engines/mergetree-family/mergetree.md#primary-keys-and-indexes-in-queries)) 。例如：

```sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

在本示例中，我们按当前周内发生的事件类型进行分区。

默认情况下，不支持浮点型分区键。要使用它，请启用设置 [allow&#95;floating&#95;point&#95;partition&#95;key](../../../operations/settings/merge-tree-settings.md#allow_floating_point_partition_key)。

向表中插入新数据时，这些数据会作为单独的 part (chunk) 存储，并按主键排序。插入后的 10–15 分钟内，同一分区中的 parts 会合并成一个完整的 part。

:::info
合并仅对分区表达式值相同的数据分区片段生效。这意味着**你不应创建过于细粒度的分区** (分区数量最好不要超过约一千个) 。否则，由于文件系统中的文件数量过多以及打开的文件描述符过多，`SELECT` 查询的性能会很差。
:::

使用 [system.parts](../../../operations/system-tables/parts.md) 表查看表 parts 和分区。例如，假设我们有一个按月分区的 `visits` 表。现在对 `system.parts` 表执行 `SELECT` 查询：

```sql
SELECT
    partition,
    name,
    active
FROM system.parts
WHERE table = 'visits'
```

```text
┌─partition─┬─name──────────────┬─active─┐
│ 201901    │ 201901_1_3_1      │      0 │
│ 201901    │ 201901_1_9_2_11   │      1 │
│ 201901    │ 201901_8_8_0      │      0 │
│ 201901    │ 201901_9_9_0      │      0 │
│ 201902    │ 201902_4_6_1_11   │      1 │
│ 201902    │ 201902_10_10_0_11 │      1 │
│ 201902    │ 201902_11_11_0_11 │      1 │
└───────────┴───────────────────┴────────┘
```

`partition` 列包含分区名称。此示例中有两个分区：`201901` 和 `201902`。您可以使用此列的值，在 [ALTER ... PARTITION](../../../sql-reference/statements/alter/partition.md) 查询中指定分区名称。

`name` 列包含分区数据分区片段的名称。您可以使用此列，在 [ALTER ATTACH PART](/zh/sql-reference/statements/alter/partition#attach-partitionpart) 查询中指定 part 的名称。

下面来拆解 part 名称：`201901_1_9_2_11`：

* `201901` 是分区名称。
* `1` 是数据块的最小编号。
* `9` 是数据块的最大编号。
* `2` 是 chunk 层级 (即它所形成的合并树深度) 。
* `11` 是变更版本 (如果某个 part 发生了变更) 。

:::info
旧类型表的 parts 名称如下：`20190117_20190123_2_2_0` (最小日期 - 最大日期 - 最小块编号 - 最大块编号 - 层级) 。
:::

`active` 列显示 part 的状态。`1` 表示活跃；`0` 表示非活跃。例如，非活跃 parts 可能是合并成更大 part 后保留下来的源 parts。损坏的数据分区片段也会被标记为非活跃。

如您在示例中所见，同一分区中有多个彼此独立的 parts (例如，`201901_1_3_1` 和 `201901_1_9_2`) 。这意味着这些 parts 尚未合并。ClickHouse 会定期合并已插入的数据分区片段，通常在插入后约 15 分钟进行。此外，您还可以使用 [OPTIMIZE](../../../sql-reference/statements/optimize.md) 查询执行一次非计划合并。示例：

```sql
OPTIMIZE TABLE visits PARTITION 201902;
```

```text
┌─partition─┬─name─────────────┬─active─┐
│ 201901    │ 201901_1_3_1     │      0 │
│ 201901    │ 201901_1_9_2_11  │      1 │
│ 201901    │ 201901_8_8_0     │      0 │
│ 201901    │ 201901_9_9_0     │      0 │
│ 201902    │ 201902_4_6_1     │      0 │
│ 201902    │ 201902_4_11_2_11 │      1 │
│ 201902    │ 201902_10_10_0   │      0 │
│ 201902    │ 201902_11_11_0   │      0 │
└───────────┴──────────────────┴────────┘
```

非活跃 parts 会在合并后约 10 分钟内被删除。

查看一组 parts 和分区的另一种方法是进入该表的目录：`/var/lib/clickhouse/data/<database>/<table>/`。例如：

```bash
/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2_11
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

像 &#39;201901&#95;1&#95;1&#95;0&#39;、&#39;201901&#95;1&#95;7&#95;1&#39; 这样的文件夹，就是各个 parts 的目录。每个 part 都对应一个分区，并且只包含某一个月份的数据 (本例中的表按月分区) 。

`detached` 目录包含通过 [DETACH](/zh/sql-reference/statements/detach) 查询从表中分离出的 parts。损坏的 parts 也会被移到这个目录，而不是直接删除。服务器不会使用 `detached` 目录中的 parts。你可以随时在该目录中添加、删除或修改数据——在你运行 [ATTACH](/zh/sql-reference/statements/alter/partition#attach-partitionpart) 查询之前，服务器都不会感知到这些变化。

请注意，在正在运行的服务器上，你不能在文件系统中手动更改 parts 集合或其数据，因为服务器无法感知这些变更。对于非复制表，你可以在服务器停止时这样做，但不建议这么做。对于复制表，在任何情况下都不能更改 parts 集合。

ClickHouse 允许你对分区执行多种操作：删除分区、从一个表复制到另一个表，或者创建备份。有关所有操作的列表，请参见 [Manipulations With Partitions and Parts](/zh/sql-reference/statements/alter/partition) 一节。

<div id="group-by-optimisation-using-partition-key">
  ## 使用分区键优化 Group By
</div>

对于表的分区键与查询的 Group By 键的某些组合，可以对每个分区分别独立执行聚合。
这样一来，最后就不必再合并所有执行线程产生的部分聚合数据，
因为这可以保证同一个 Group By 键值不会同时出现在两个不同线程的工作集中。

典型示例如下：

```sql
CREATE TABLE session_log
(
    UserID UInt64,
    SessionID UUID
)
ENGINE = MergeTree
PARTITION BY sipHash64(UserID) % 16
ORDER BY tuple();

SELECT
    UserID,
    COUNT()
FROM session_log
GROUP BY UserID;
```

:::note
这类查询的性能在很大程度上取决于表的布局。因此，该优化默认未启用。
:::

获得良好性能的关键因素包括：

* 查询涉及的分区数量应足够多 (大于 `max_threads / 2`) ，否则查询将无法充分利用机器资源
* 分区不应过小，否则批次处理会退化为逐行处理
* 各分区的大小应大致相当，这样所有线程承担的工作量才会基本一致

:::info
建议在 `PARTITION BY` 子句中的列上应用某种哈希函数，以便将数据均匀分布到各个分区中。
:::

相关设置包括：

* `allow_aggregate_partitions_independently` - 控制是否启用该优化
* `force_aggregate_partitions_independently` - 在保证正确性的前提下，只要适用就强制使用该优化，即使内部用于评估其收益的逻辑原本会将其禁用
* `max_number_of_partitions_for_independent_aggregation` - 对表可拥有的最大分区数量设置的硬性上限