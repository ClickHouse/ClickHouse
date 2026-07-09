---
description: 'PARTITION 文档'
sidebar_label: 'PARTITION'
sidebar_position: 38
slug: /sql-reference/statements/alter/partition
title: '分区和 分片 的操作'
doc_type: 'reference'
---

以下是对[分区](/zh/engines/table-engines/mergetree-family/custom-partitioning-key.md)可用的操作：

* [DETACH PARTITION|PART](#detach-partitionpart) — 将分区或 分片 移动到 `detached` 目录，并将其从活动状态中分离。
* [DROP PARTITION|PART](#drop-partitionpart) — 删除分区或 分片。
* [DROP DETACHED PARTITION|PART](#drop-detached-partitionpart) - 从 `detached` 中删除一个 分片，或删除某个分区中的所有 分片。
* [FORGET PARTITION](#forget-partition) — 如果分区为空，则从 ZooKeeper 中删除该分区的元数据。
* [ATTACH PARTITION|PART](#attach-partitionpart) — 将 `detached` 目录中的分区或 分片 添加到表中。
* [ATTACH PARTITION FROM](#attach-partition-from) — 将数据分区从一个表复制到另一个表并附加到目标表。
* [REPLACE PARTITION](#replace-partition) — 将数据分区从一个表复制到另一个表并替换目标分区。
* [MOVE PARTITION TO TABLE](#move-partition-to-table) — 将数据分区从一个表移动到另一个表。
* [CLEAR COLUMN IN PARTITION](#clear-column-in-partition) — 重置分区中指定列的值。
* [CLEAR INDEX IN PARTITION](#clear-index-in-partition) — 重置分区中指定的次级索引。
* [FREEZE PARTITION](#freeze-partition) — 创建分区的备份。
* [UNFREEZE PARTITION](#unfreeze-partition) — 删除分区的备份。
* [FETCH PARTITION|PART](#fetch-partitionpart) — 从另一台服务器拉取 分片 或分区。
* [MOVE PARTITION|PART](#move-partitionpart) — 将分区/数据分片移动到另一个 磁盘 或 卷。
* [UPDATE IN PARTITION](#update-in-partition) — 按条件更新分区内的数据。
* [DELETE IN PARTITION](#delete-in-partition) — 按条件删除分区内的数据。
* [REWRITE PARTS](#rewrite-parts) — 完全重写表中的 分片 (或特定分区中的 分片) 。

{/* */ }

<div id="detach-partitionpart">
  ## DETACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DETACH PARTITION|PART partition_expr
```

将指定分区的所有数据移至 `detached` 目录。服务器会忘记这个已脱离的数据分区，视其为不存在。在你执行 [ATTACH](#attach-partitionpart) 查询之前，服务器都不会识别这些数据。

示例：

```sql
ALTER TABLE mt DETACH PARTITION '2020-11-21';
ALTER TABLE mt DETACH PART 'all_2_2_0';
```

有关如何设置分区表达式，请参阅[如何设置分区表达式](#how-to-set-partition-expression)一节。

查询执行后，您可以对 `detached` 目录中的数据进行任意操作——从文件系统中将其删除，或者直接保留。

该查询会在所有副本上复制执行——它会将数据移动到各个副本上的 `detached` 目录。请注意，您只能在 leader 副本上执行此查询。要判断某个副本是否为 leader，请对 [system.replicas](/zh/operations/system-tables/replicas) 表执行 `SELECT` 查询。或者，更简单的做法是在所有副本上执行 `DETACH` 查询——除 leader 副本外，其他所有副本都会抛出异常 (因为允许存在多个 leader) 。

<div id="drop-partitionpart">
  ## DROP PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP PARTITION|PART partition_expr
```

从表中删除指定的分区。此查询会将该分区标记为非活动状态，并在大约 10 分钟后将数据彻底删除。

有关如何设置分区表达式，请参见[如何设置分区表达式](#how-to-set-partition-expression)一节。

该查询是复制执行的——会删除所有副本上的数据。

示例：

```sql
ALTER TABLE mt DROP PARTITION '2020-11-21';
ALTER TABLE mt DROP PART 'all_4_4_0';
```

<div id="drop-detached-partitionpart">
  ## DROP DETACHED PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] DROP DETACHED PARTITION|PART ALL|partition_expr
```

从 `detached` 中移除指定的 分片，或移除指定分区中的所有 分片。
有关设置分区表达式的更多信息，请参见[如何设置分区表达式](#how-to-set-partition-expression)一节。

<div id="forget-partition">
  ## FORGET PARTITION
</div>

```sql
ALTER TABLE table_name FORGET PARTITION partition_expr
```

从 ZooKeeper 中删除空分区的所有元数据。如果分区非空或不存在，该查询将失败。请确保仅对今后不再使用的分区执行此操作。

关于如何设置分区表达式，请参见 [如何设置分区表达式](#how-to-set-partition-expression) 一节。

示例：

```sql
ALTER TABLE mt FORGET PARTITION '20201121';
```

<div id="attach-partitionpart">
  ## ATTACH PARTITION|PART
</div>

```sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

将 `detached` 目录中的数据添加到表中。既可以添加整个分区的数据，也可以添加单独的分片。示例：

```sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

有关如何设置分区表达式的更多信息，请参见[如何设置分区表达式](#how-to-set-partition-expression)一节。

该查询会在各副本间复制执行。发起操作的副本会检查 `detached` 目录中是否有数据。
如果存在数据，查询会检查其完整性。如果一切正常，查询会将数据添加到表中。

如果接收到 ATTACH 命令的非发起方副本在自己的 `detached` 目录中找到了校验和正确的数据分片，它会直接附加该数据，而无需从其他副本拉取。
如果没有校验和正确的数据分片，则会从任意拥有该分片的副本下载数据。

你可以将数据放到某个副本上的 `detached` 目录中，然后使用 `ALTER ... ATTACH` 查询将其添加到所有副本上的表中。

<div id="attach-partition-from">
  ## ATTACH PARTITION FROM
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] ATTACH PARTITION partition_expr FROM table1
```

此查询会将数据分区从 `table1` 复制到 `table2`。

请注意：

* 不会删除 `table1` 或 `table2` 中的数据。
* `table1` 可以是临时表。

要使该查询成功执行，必须满足以下条件：

* 两个表必须具有相同的结构。
* 两个表必须具有相同的分区键、相同的 ORDER BY 键以及相同的主键。
* 两个表必须具有相同的存储策略。
* 目标表必须包含源表中的所有索引和投影。如果目标表启用了 `enforce_index_structure_match_on_partition_manipulation` 设置，则索引和投影必须完全一致。否则，目标表中的索引和投影可以是源表的超集。

<div id="replace-partition">
  ## REPLACE PARTITION
</div>

```sql
ALTER TABLE table2 [ON CLUSTER cluster] REPLACE PARTITION partition_expr FROM table1
```

此查询会将数据分区从 `table1` 复制到 `table2`，并替换 `table2` 中现有的分区。该操作具有原子性。

请注意：

* 不会从 `table1` 删除数据。
* `table1` 可以是临时表。

要使该查询成功运行，必须满足以下条件：

* 两个表必须具有相同的结构。
* 两个表必须具有相同的分区键、相同的 ORDER BY 键，以及相同的主键。
* 两个表必须具有相同的存储策略。
* 目标表必须包含源表中的所有索引和投影。如果目标表启用了 `enforce_index_structure_match_on_partition_manipulation` 设置，则索引和投影必须完全一致。否则，目标表可以包含源表索引和投影的超集。

<div id="move-partition-to-table">
  ## MOVE PARTITION TO TABLE
</div>

```sql
ALTER TABLE table_source [ON CLUSTER cluster] MOVE PARTITION partition_expr TO TABLE table_dest
```

此查询会将数据分区从 `table_source` 移动到 `table_dest`，并删除 `table_source` 中的数据。

要使查询成功运行，必须满足以下条件：

* 两张表必须具有相同的结构。
* 两张表必须具有相同的分区键、相同的 ORDER BY 键以及相同的主键。
* 两张表必须具有相同的存储策略。
* 两张表必须属于同一引擎家族 (复制或非复制) 。
* 目标表必须包含源表中的所有索引和投影。如果目标表启用了 `enforce_index_structure_match_on_partition_manipulation` 设置，则索引和投影必须完全一致。否则，目标表中的索引和投影可以是源表索引和投影的超集。

<div id="clear-column-in-partition">
  ## CLEAR COLUMN IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR COLUMN column_name IN PARTITION partition_expr
```

重置指定分区中指定列的所有值。如果在创建表时定义了 `DEFAULT` 子句，此查询会将该列的值设置为指定的默认值。

示例：

```sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

<div id="freeze-partition">
  ## FREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FREEZE [PARTITION partition_expr] [WITH NAME 'backup_name']
```

此查询会为指定分区创建本地备份。如果省略 `PARTITION` 子句，则该查询会一次性为所有分区创建备份。

:::note
整个备份过程都无需停止服务器。
:::

请注意，对于旧式表，你可以指定分区名称的前缀 (例如 `2019`) ——这样查询会为所有对应分区创建备份。有关如何设置分区表达式，请参见 [如何设置分区表达式](#how-to-set-partition-expression) 一节。

执行时，为了创建数据快照，该查询会为表数据创建硬链接。硬链接位于目录 `/var/lib/clickhouse/shadow/N/...` 中，其中：

* `/var/lib/clickhouse/` 是在配置中指定的 ClickHouse 工作目录。
* `N` 是备份的递增编号。
* 如果指定了 `WITH NAME` 参数，则使用 `'backup_name'` 参数的值代替递增编号。

:::note
如果你为表使用了[一组用于数据存储的磁盘](/zh/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes)，则每个磁盘上都会出现 `shadow/N` 目录，其中存储与 `PARTITION` 表达式匹配的数据分片。
:::

备份内会创建与 `/var/lib/clickhouse/` 中相同的目录结构。该查询会对所有文件执行 `chmod`，禁止对其进行写入。

创建备份后，你可以将数据从 `/var/lib/clickhouse/shadow/` 复制到远程服务器，然后再从本地服务器中删除。请注意，`ALTER t FREEZE PARTITION` 查询不会被复制到其他副本。它只会在本地服务器上创建本地备份。

该查询几乎可以瞬间创建备份 (但首先会等待对应表上的当前查询运行结束) 。

`ALTER TABLE t FREEZE PARTITION` 只复制数据，不复制表元数据。要备份表元数据，请复制文件 `/var/lib/clickhouse/metadata/database/table.sql`

要从备份中恢复数据，请执行以下操作：

1. 如果表不存在，则创建该表。要查看查询，请使用 .sql 文件 (将其中的 `ATTACH` 替换为 `CREATE`) 。
2. 将备份中 `data/database/table/` 目录下的数据复制到 `/var/lib/clickhouse/data/database/table/detached/` 目录。
3. 运行 `ALTER TABLE t ATTACH PARTITION` 查询，将数据添加到表中。

从备份恢复数据时，无需停止服务器。

该查询会并行处理 分片，线程数由 `max_threads` 设置控制。

有关备份和恢复数据的更多信息，请参见[“ClickHouse 中的备份与恢复”](/zh/operations/backup/overview)一节。

<div id="unfreeze-partition">
  ## UNFREEZE PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] UNFREEZE [PARTITION 'part_expr'] WITH NAME 'backup_name'
```

从磁盘中删除指定名称的 `frozen` 分区。如果省略 `PARTITION` 子句，该查询将一次性删除所有分区的备份。

<div id="clear-index-in-partition">
  ## CLEAR INDEX IN PARTITION
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] CLEAR INDEX index_name IN PARTITION partition_expr
```

该查询的工作方式与 `CLEAR COLUMN` 类似，但它重置的是索引，而不是列数据。

<div id="fetch-partitionpart">
  ## FETCH PARTITION|PART
</div>

```sql
ALTER TABLE table_name [ON CLUSTER cluster] FETCH PARTITION|PART partition_expr FROM 'path-in-zookeeper'
```

从另一台服务器拉取一个分区。此查询仅适用于复制表。

该查询会执行以下操作：

1. 从指定的分片拉取 partition|分片。在 &#39;path-in-zookeeper&#39; 中，您必须指定该分片在 ZooKeeper 中的路径。
2. 然后，查询会将下载的数据放入 `table_name` 表的 `detached` 目录中。使用 [ATTACH PARTITION|PART](#attach-partitionpart) 查询将这些数据添加到表中。

例如：

1. FETCH PARTITION

```sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

2. FETCH PART

```sql
ALTER TABLE users FETCH PART 201901_2_2_0 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PART 201901_2_2_0;
```

注意：

* `ALTER ... FETCH PARTITION|PART` 查询不会被复制。它只会将该分区或分片放到本地 server 上的 `detached` directory 中。
* `ALTER TABLE ... ATTACH` 查询会被复制。它会将数据添加到所有副本中。数据会从 `detached` directory 添加到其中一个副本，而其他副本则从邻近副本获取数据。

在拉取之前，系统会检查该分区是否存在，以及表结构是否匹配。系统会自动从健康副本中选择最合适的副本。

尽管该查询名为 `ALTER TABLE`，但它不会更改表结构，也不会立即更改表中可用的数据。

<div id="move-partitionpart">
  ## MOVE PARTITION|PART
</div>

将分区或数据分片移动到使用 `MergeTree` 引擎的表中的另一个卷或磁盘。请参阅[使用多个块设备进行数据存储](/zh/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-multiple-volumes)。

```sql
ALTER TABLE table_name [ON CLUSTER cluster] MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

`ALTER TABLE t MOVE` 查询：

* 不会被复制，因为不同副本可能具有不同的存储策略。
* 如果指定的磁盘或卷未配置，则会返回错误。如果无法应用存储策略中指定的数据移动条件，查询也会返回错误。
* 在以下情况下也可能返回错误：要移动的数据已被后台进程、并发执行的 `ALTER TABLE t MOVE` 查询，或后台数据合并移动。在这种情况下，用户无需执行任何额外操作。

示例：

```sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

<div id="update-in-partition">
  ## UPDATE IN PARTITION
</div>

更新指定分区中与指定过滤表达式匹配的数据。该操作通过[变更](/zh/sql-reference/statements/alter/index.md#mutations)实现。

语法：

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] UPDATE column1 = expr1 [, ...] [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### 示例
</div>

```sql
-- using partition name
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt UPDATE x = x + 1 IN PARTITION ID '2' WHERE p = 2;
```

<div id="see-also">
  ### 另请参阅
</div>

* [UPDATE](/zh/sql-reference/statements/alter/partition#update-in-partition)

<div id="delete-in-partition">
  ## DELETE IN PARTITION
</div>

删除指定分区中与指定过滤表达式匹配的数据。该操作通过 [变更](/zh/sql-reference/statements/alter/index.md#mutations) 实现。

语法：

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] DELETE [IN PARTITION partition_expr] WHERE filter_expr
```

<div id="example">
  ### 示例
</div>

```sql
-- using partition name
ALTER TABLE mt DELETE IN PARTITION 2 WHERE p = 2;

-- using partition id
ALTER TABLE mt DELETE IN PARTITION ID '2' WHERE p = 2;
```

<div id="rewrite-parts">
  ## REWRITE PARTS
</div>

这会使用所有新的设置，从头重写这些 parts。这样做是有意义的，因为默认情况下，像 `use_const_adaptive_granularity` 这样的表级设置只会应用于新写入的 parts。

<div id="example">
  ### 示例
</div>

```sql
ALTER TABLE mt REWRITE PARTS;
ALTER TABLE mt REWRITE PARTS IN PARTITION 2;
```

<div id="see-also">
  ### 另请参阅
</div>

* [DELETE](/zh/sql-reference/statements/alter/delete)

<div id="how-to-set-partition-expression">
  ## 如何设置分区表达式
</div>

你可以在 `ALTER ... PARTITION` 查询中通过不同方式指定分区表达式：

* 使用 `system.parts` 表中 `partition` 列的值。例如：`ALTER TABLE visits DETACH PARTITION 201901`。
* 使用关键字 `ALL`。它只能与 DROP/DETACH/ATTACH/ATTACH FROM 一起使用。例如：`ALTER TABLE visits ATTACH PARTITION ALL`。
* 使用与表分区键元组在类型上匹配的表达式或常量元组。对于单元素分区键，表达式应包含在 `tuple (...)` 函数中。例如：`ALTER TABLE visits DETACH PARTITION tuple(toYYYYMM(toDate('2019-01-25')))`。
* 使用分区 ID。分区 ID 是分区的字符串标识符 (如果可能，会采用便于人类阅读的形式) ，在文件系统和 ZooKeeper 中用作分区名称。分区 ID 必须在 `PARTITION ID` 子句中以单引号形式指定。例如：`ALTER TABLE visits DETACH PARTITION ID '201901'`。
* 在 [ALTER ATTACH PART](#attach-partitionpart) 和 [DROP DETACHED PART](#drop-detached-partitionpart) 查询中，如需指定 分片 名称，请使用 [system.detached&#95;parts](/zh/operations/system-tables/detached_parts) 表 `name` 列中的值作为字符串字面量。例如：`ALTER TABLE visits ATTACH PART '201901_1_1_0'`。

指定分区时是否需要加引号，取决于分区表达式的类型。例如，对于 `String` 类型，必须用引号 (`'`) 括起来；对于 `Date` 和 `Int*` 类型，则不需要引号。

以上所有规则同样适用于 [OPTIMIZE](/zh/sql-reference/statements/optimize.md) 查询。如果你在优化非分区表时需要指定唯一分区，请将表达式设为 `PARTITION tuple()`。例如：

```sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

`IN PARTITION` 用于指定在 `ALTER TABLE` 查询中，[UPDATE](/zh/sql-reference/statements/alter/update) 或 [DELETE](/zh/sql-reference/statements/alter/delete) 表达式要应用到哪个分区。只会基于指定的分区创建新的 分片。这样一来，当表被划分为许多分区，而你只需要按点更新数据时，`IN PARTITION` 有助于降低负载。

`ALTER ... PARTITION` 查询的示例可见以下测试：[`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) 和 [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql)。