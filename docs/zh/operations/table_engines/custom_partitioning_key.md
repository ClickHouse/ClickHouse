# 自定义分区键

[MergeTree](mergetree.md) 系列的表（包括 [可复制表](replication.md) ）可以使用分区。基于 MergeTree 表的 [物化视图](materializedview.md) 也支持分区。

一个分区是指按指定规则逻辑组合一起的表的记录集。可以按任意标准进行分区，如按月，按日或按事件类型。为了减少需要操作的数据，每个分区都是分开存储的。访问数据时，ClickHouse 尽量使用这些分区的最小子集。

分区是在 [建表](mergetree.md#table_engine-mergetree-creating-a-table) 的 `PARTITION BY expr` 子句中指定。分区键可以是关于列的任何表达式。例如，指定按月分区，表达式为 `toYYYYMM(date_column)`：

``` sql
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

分区键也可以是表达式元组（类似 [主键](mergetree.md#primary-keys-and-indexes-in-queries) ）。例如：

``` sql
ENGINE = ReplicatedCollapsingMergeTree('/clickhouse/tables/name', 'replica1', Sign)
PARTITION BY (toMonday(StartDate), EventType)
ORDER BY (CounterID, StartDate, intHash32(UserID));
```

上例中，我们设置按一周内的事件类型分区。

新数据插入到表中时，这些数据会存储为按主键排序的新片段（块）。插入后 10-15 分钟，同一分区的各个片段会合并为一整个片段。

!!! attention "注意"
    那些有相同分区表达式值的数据片段才会合并。这意味着 **你不应该用太精细的分区方案**（超过一千个分区）。否则，会因为文件系统中的文件数量和需要找开的文件描述符过多，导致 `SELECT` 查询效率不佳。

可以通过 [system.parts](../system_tables.md#system_tables-parts) 表查看表片段和分区信息。例如，假设我们有一个 `visits` 表，按月分区。对 `system.parts` 表执行 `SELECT`：

``` sql
SELECT 
    partition,
    name, 
    active
FROM system.parts 
WHERE table = 'visits'
```

```
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      1 │
│ 201902    │ 201902_10_10_0 │      1 │
│ 201902    │ 201902_11_11_0 │      1 │
└───────────┴────────────────┴────────┘
```

`partition` 列存储分区的名称。此示例中有两个分区：`201901` 和 `201902`。在 [ALTER ... PARTITION](#alter_manipulations-with-partitions) 语句中你可以使用该列值来指定分区名称。

`name` 列为分区中数据片段的名称。在 [ALTER ATTACH PART](#alter_attach-partition) 语句中你可以使用此列值中来指定片段名称。

这里我们拆解下第一部分的名称：`201901_1_3_1`：

- `201901` 是分区名称。
- `1` 是数据块的最小编号。
- `3` 是数据块的最大编号。
- `1` 是块级别（即在由块组成的合并树中，该块在树中的深度）。

!!! attention "注意"
    旧类型表的片段名称为：`20190117_20190123_2_2_0`（最小日期 - 最大日期 - 最小块编号 - 最大块编号 - 块级别）。

`active` 列为片段状态。`1` 激活状态；`0` 非激活状态。非激活片段是那些在合并到较大片段之后剩余的源数据片段。损坏的数据片段也表示为非活动状态。

正如在示例中所看到的，同一分区中有几个独立的片段（例如，`201901_1_3_1`和`201901_1_9_2`）。这意味着这些片段尚未合并。ClickHouse 大约在插入后15分钟定期报告合并操作，合并插入的数据片段。此外，你也可以使用 [OPTIMIZE](../../query_language/misc.md#misc_operations-optimize) 语句直接执行合并。例：

``` sql
OPTIMIZE TABLE visits PARTITION 201902;
```

```
┌─partition─┬─name───────────┬─active─┐
│ 201901    │ 201901_1_3_1   │      0 │
│ 201901    │ 201901_1_9_2   │      1 │
│ 201901    │ 201901_8_8_0   │      0 │
│ 201901    │ 201901_9_9_0   │      0 │
│ 201902    │ 201902_4_6_1   │      0 │
│ 201902    │ 201902_4_11_2  │      1 │
│ 201902    │ 201902_10_10_0 │      0 │
│ 201902    │ 201902_11_11_0 │      0 │
└───────────┴────────────────┴────────┘
```

非激活片段会在合并后的10分钟左右删除。

查看片段和分区信息的另一种方法是进入表的目录：`/var/lib/clickhouse/data/<database>/<table>/`。例如：

```bash
dev:/var/lib/clickhouse/data/default/visits$ ls -l
total 40
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 201901_1_3_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201901_1_9_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_8_8_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 15:52 201901_9_9_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_10_10_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:17 201902_11_11_0
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 16:19 201902_4_11_2
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  5 12:09 201902_4_6_1
drwxr-xr-x 2 clickhouse clickhouse 4096 Feb  1 16:48 detached
```

文件夹 '201901_1_1_0'，'201901_1_7_1' 等是片段的目录。每个片段都与一个对应的分区相关，并且只包含这个月的数据（本例中的表按月分区）。

`detached` 目录存放着使用 [DETACH](#alter_detach-partition) 语句从表中分离的片段。损坏的片段也会移到该目录，而不是删除。服务器不使用`detached`目录中的片段。可以随时添加，删除或修改此目录中的数据 – 在运行 [ATTACH](../../query_language/alter.md#alter_attach-partition) 语句前，服务器不会感知到。

注意，在操作服务器时，你不能手动更改文件系统上的片段集或其数据，因为服务器不会感知到这些修改。对于非复制表，可以在服务器停止时执行这些操作，但不建议这样做。对于复制表，在任何情况下都不要更改片段文件。

ClickHouse 支持对分区执行这些操作：删除分区，从一个表复制到另一个表，或创建备份。了解分区的所有操作，请参阅 [分区和片段的操作](../../query_language/alter.md#alter_manipulations-with-partitions) 一节。

[来源文章](https://clickhouse.yandex/docs/en/operations/table_engines/custom_partitioning_key/) <!--hide-->
