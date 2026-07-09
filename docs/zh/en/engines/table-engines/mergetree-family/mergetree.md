---
description: '`MergeTree` 家族表引擎专为高数据摄取速率和海量数据场景而设计。'
sidebar_label: 'MergeTree'
sidebar_position: 11
slug: /engines/table-engines/mergetree-family/mergetree
title: 'MergeTree 表引擎'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="mergetree-table-engine">
  # MergeTree 表引擎
</div>

`MergeTree` 引擎以及 `MergeTree` 家族中的其他引擎 (例如 `ReplacingMergeTree`、`AggregatingMergeTree`) 是 ClickHouse 中最常用、也最稳健的表引擎。

`MergeTree` 家族表引擎专为高数据摄取速率和海量数据而设计。
插入操作会创建 table parts，这些 parts 会由后台进程与其他 table parts 合并。

`MergeTree` 家族表引擎的主要特性。

* 表的主键决定了每个 table part 内的排序顺序 (聚簇索引) 。主键引用的也不是单独的行，而是由 8192 行组成、称为粒度的块。这使得海量数据集的主键足够小，能够常驻主内存，同时仍可快速访问磁盘上的数据。

* 表可以使用任意分区表达式进行分区。分区裁剪可确保在查询条件允许时跳过读取某些分区。

* 数据可以在 cluster 的多个节点之间复制，以实现高可用性、故障转移和零停机升级。请参阅 [数据复制](/zh/engines/table-engines/mergetree-family/replication.md)。

* `MergeTree` 表引擎支持多种统计信息类型和采样方法，以帮助进行查询优化。

:::note
尽管名称相似，[Merge](/zh/engines/table-engines/special/merge) 引擎与 `*MergeTree` 引擎并不相同。
:::

<div id="table_engine-mergetree-creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr1] [COMMENT ...] [CODEC(codec1)] [STATISTICS(stat1)] [TTL expr1] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    name2 [type2] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr2] [COMMENT ...] [CODEC(codec2)] [STATISTICS(stat2)] [TTL expr2] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2],
    ...
    PROJECTION projection_name_1 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]),
    PROJECTION projection_name_2 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr
    [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ] ]
[SETTINGS name = value, ...]
```

有关参数的详细说明，请参见 [CREATE TABLE](/zh/sql-reference/statements/create/table.md) 语句

<div id="mergetree-query-clauses">
  ### 查询子句
</div>

<div id="engine">
  #### ENGINE
</div>

`ENGINE` — 引擎的名称和参数。`ENGINE = MergeTree()`。`MergeTree` 引擎没有参数。

<div id="order_by">
  #### ORDER BY
</div>

`ORDER BY` —— 排序键。

由列名或任意表达式组成的元组。示例：`ORDER BY (CounterID + 1, EventDate)`。

如果未定义主键 (即未指定 `PRIMARY KEY`) ，ClickHouse 会将排序键用作主键。

如果不需要排序，可以使用 `ORDER BY tuple()` 语法。
或者，如果启用了 `create_table_empty_primary_key_by_default` 设置，系统会在 `CREATE TABLE` 语句中隐式添加 `ORDER BY ()`。请参阅[选择主键](#selecting-a-primary-key)。

<div id="partition-by">
  #### PARTITION BY
</div>

`PARTITION BY` — [分区键](/zh/engines/table-engines/mergetree-family/custom-partitioning-key.md)。可选。大多数情况下，你不需要分区键；即使确实需要分区，通常也不需要使用比按月更细的分区键。分区并不会加速查询 (这与 ORDER BY 表达式不同) 。绝不要使用过细粒度的分区。不要按客户端标识符或名称对数据进行分区 (应将客户端标识符或名称作为 ORDER BY 表达式中的第一列) 。

如需按月分区，请使用 `toYYYYMM(date_column)` 表达式，其中 `date_column` 是一个类型为 [Date](/zh/sql-reference/data-types/date.md) 的日期列。这里的分区名称采用 `"YYYYMM"` 格式。

<div id="primary-key">
  #### PRIMARY KEY
</div>

`PRIMARY KEY` — 如果其[不同于排序键](#choosing-a-primary-key-that-differs-from-the-sorting-key)，则用于指定主键。可选。

指定排序键 (使用 `ORDER BY` 子句) 时，也会隐式指定主键。
通常无需在排序键之外再单独指定主键。

<div id="sample-by">
  #### SAMPLE BY
</div>

`SAMPLE BY` — 采样表达式。可选。

如果指定了该表达式，它必须包含在主键中。
采样表达式的结果必须为无符号整数。

示例：`SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`。

<div id="ttl">
  #### 生存时间 (TTL)
</div>

`TTL` — 一组规则，用于指定行的存储保留时长，以及在[磁盘和卷之间](#table_engine-mergetree-multiple-volumes)自动移动 parts 的逻辑。可选。

表达式的结果必须为 `Date` 或 `DateTime`，例如 `TTL date + INTERVAL 1 DAY`。

规则类型 `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY` 用于指定当表达式条件满足 (即达到当前时间) 时，对 part 执行的操作：删除过期行、将 part (如果 part 中所有行都满足该表达式) 移动到指定磁盘 (`TO DISK 'xxx'`) 或卷 (`TO VOLUME 'xxx'`) ，或者聚合过期行中的值。规则的默认类型为删除 (`DELETE`) 。可以指定多条规则，但 `DELETE` 规则最多只能有一条。

更多详情，请参阅[列和表的 TTL](#table_engine-mergetree-ttl)

<div id="settings">
  #### 设置
</div>

请参阅 [MergeTree 设置](../../../operations/settings/merge-tree-settings.md)。

**Sections 设置示例**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

在该示例中，我们将分区设置为按月分区。

我们还将 sampling&#95;expression 设置为基于用户 ID 的哈希值。这样可以针对每个 `CounterID` 和 `EventDate`，对表中的数据进行伪随机分布。如果你在查询数据时指定 [SAMPLE](/zh/sql-reference/statements/select/sample) 子句，ClickHouse 将针对部分用户返回均匀的伪随机数据样本。

可以省略 `index_granularity` 设置，因为 8192 是默认值。

<details markdown="1">
  <summary>已弃用的建表方法</summary>

  :::note
  请勿在新项目中使用此方法。如有可能，请将旧项目切换到上文所述的方法。
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  **MergeTree() 参数**

  * `date-column` — [Date](/zh/sql-reference/data-types/date.md) 类型的列名。ClickHouse 会根据此列自动按月创建分区。分区名称采用 `"YYYYMM"` 格式。
  * `sampling_expression` — 用于采样的表达式。
  * `(primary, key)` — 主键。类型：[Tuple()](/zh/sql-reference/data-types/tuple.md)
  * `index_granularity` — 索引粒度。即索引“标记”之间的数据行数。值 8192 适用于大多数任务。

  **示例**

  ```sql
  MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
  ```

  `MergeTree` 引擎的配置方式与上面主引擎配置方法示例中的配置方式相同。
</details>

<div id="mergetree-data-storage">
  ## 数据存储
</div>

一张表由按主键排序的数据分区片段组成。

当数据插入表中时，会创建独立的数据分区片段，并且每个数据分区片段都会按主键进行字典序排序。例如，如果主键是 `(CounterID, Date)`，则该数据分区片段中的数据会按 `CounterID` 排序，并且在每个 `CounterID` 内再按 `Date` 排序。

属于不同分区的数据会被分到不同的 parts 中。在后台，ClickHouse 会合并数据分区片段，以提高存储效率。属于不同分区的 parts 不会被合并。合并机制并不能保证具有相同主键的所有行都会位于同一个数据分区片段中。

数据分区片段可以采用 `Wide` 或 `Compact` 格式存储。在 `Wide` 格式中，每一列都存储在文件系统中的单独文件里；在 `Compact` 格式中，所有列都存储在一个文件中。`Compact` 格式可用于提升小批量且频繁插入的性能。

数据存储格式由表引擎的 `min_bytes_for_wide_part` 和 `min_rows_for_wide_part` 设置控制。如果一个数据分区片段中的字节数或行数小于对应设置的值，则该 parts 会以 `Compact` 格式存储；否则会以 `Wide` 格式存储。如果这两个设置都未设置，则数据分区片段会以 `Wide` 格式存储。

每个数据分区片段在逻辑上都会被划分为粒度。粒度是 ClickHouse 在查询数据时读取的最小不可再分数据集。ClickHouse 不会拆分行或值，因此每个粒度始终包含整数数量的行。粒度的第一行会用该行的主键值进行标记。对于每个数据分区片段，ClickHouse 都会创建一个存储这些标记的索引文件。对于每一列，无论它是否属于主键，ClickHouse 也会存储相同的标记。这些标记使你能够直接在列文件中定位数据。

粒度大小受表引擎的 `index_granularity` 和 `index_granularity_bytes` 设置限制。粒度中的行数取决于行大小，范围在 `[1, index_granularity]` 之间。如果单行大小大于该设置的值，则粒度大小可以超过 `index_granularity_bytes`。在这种情况下，粒度大小等于该行的大小。

<div id="primary-keys-and-indexes-in-queries">
  ## 查询中的主键和索引
</div>

以 `(CounterID, Date)` 这个主键为例。在这种情况下，排序方式和索引如下所示：

```text
Whole data:     [---------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
Marks:           |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
Marks numbers:   0      1      2      3      4      5      6      7      8      9      10
```

如果数据查询指定了：

* `CounterID in ('a', 'h')`，服务器会读取标记范围 `[0, 3)` 和 `[6, 8)` 中的数据。
* `CounterID IN ('a', 'h') AND Date = 3`，服务器会读取标记范围 `[1, 3)` 和 `[7, 8)` 中的数据。
* `Date = 3`，服务器会读取标记范围 `[1, 10]` 中的数据。

上述示例表明，使用索引始终比全表扫描更高效。

稀疏索引会导致读取额外的数据。读取主键的单个范围时，每个数据块中最多可能会多读取 `index_granularity * 2` 行。

稀疏索引让你能够处理数量非常庞大的表行，因为在大多数情况下，这类索引都可以装入计算机的 RAM。

ClickHouse 不要求主键唯一。你可以插入多行具有相同主键的行。

你可以在 `PRIMARY KEY` 和 `ORDER BY` 子句中使用 `Nullable` 类型的表达式，但强烈不建议这样做。要启用此功能，请打开 [allow&#95;nullable&#95;key](/zh/operations/settings/merge-tree-settings/#allow_nullable_key) 设置。对于 `ORDER BY` 子句中的 `NULL` 值，适用 [NULLS&#95;LAST](/zh/sql-reference/statements/select/order-by.md/#sorting-of-special-values) 原则。

<div id="selecting-a-primary-key">
  ### 选择主键
</div>

主键中的列数没有明确限制。可以根据数据结构，在主键中包含更多或更少的列。这可能会：

* 提高索引性能。

  如果主键为 `(a, b)`，那么在满足以下条件时，增加一列 `c` 可以提升性能：

  * 存在按列 `c` 进行条件过滤的查询。
  * `(a, b)` 取值相同的长数据范围 (长度达到 `index_granularity` 的数倍) 较为常见。换句话说，增加一列后，可以跳过相当长的数据范围。

* 提高数据压缩效果。

  ClickHouse 会按主键对数据排序，因此数据一致性越高，压缩效果越好。

* 在 [CollapsingMergeTree](/zh/engines/table-engines/mergetree-family/collapsingmergetree) 和 [SummingMergeTree](/zh/engines/table-engines/mergetree-family/summingmergetree.md) 引擎合并数据分区片段时，提供额外的逻辑。

  在这种情况下，指定一个与主键不同的*排序键*是有意义的。

较长的主键会对 insert 性能和内存消耗产生负面影响，但主键中的额外列不会影响 ClickHouse 执行 `SELECT` 查询时的性能。

你可以使用 `ORDER BY tuple()` 语法创建一个没有主键的表。在这种情况下，ClickHouse 会按照插入顺序存储数据。如果你希望在通过 `INSERT ... SELECT` 查询插入数据时保留数据顺序，请设置 [max&#95;insert&#95;threads = 1](/zh/operations/settings/settings#max_insert_threads)。

要按初始顺序查询数据，请使用[单线程](/zh/operations/settings/settings.md/#max_threads) `SELECT` 查询。

<div id="choosing-a-primary-key-that-differs-from-the-sorting-key">
  ### 选择与排序键不同的主键
</div>

可以指定一个与排序键不同的主键 (即为每个标记将其值写入索引文件的表达式) 。排序键则是用于对数据分区片段中的行排序的表达式。在这种情况下，主键表达式元组必须是排序键表达式元组的前缀。

此功能在使用 [SummingMergeTree](/zh/engines/table-engines/mergetree-family/summingmergetree.md) 和
[AggregatingMergeTree](/zh/engines/table-engines/mergetree-family/aggregatingmergetree.md) 表引擎时尤其有用。在使用这些引擎的常见场景中，表通常有两类列：*维度* 和 *度量*。典型查询会按任意 `GROUP BY` 对度量列的值进行聚合，并按维度进行过滤。由于 SummingMergeTree 和 AggregatingMergeTree 会聚合排序键值相同的行，因此很自然会将所有维度都加入排序键。这样一来，键表达式就会包含一长串列，而且这个列表还需要随着新增维度而频繁更新。

在这种情况下，更合适的做法是只在主键中保留少数几个能够提供高效范围扫描的列，再将其余维度列加入排序键元组。

对排序键执行 [ALTER](/zh/sql-reference/statements/alter/index.md) 是一种轻量级操作，因为当新列同时添加到表和排序键中时，现有数据分区片段无需修改。由于旧排序键是新排序键的前缀，并且新添加的列中没有数据，因此在修改表的那一刻，数据同时满足按旧排序键和新排序键排序。

<div id="use-of-indexes-and-partitions-in-queries">
  ### 在查询中使用索引和分区
</div>

对于 `SELECT` 查询，ClickHouse 会分析是否能够使用索引。如果 `WHERE/PREWHERE` 子句中包含某个表达式 (作为合取条件之一，或整个子句本身) ，且该表达式表示等值或不等值比较操作，或者包含对主键或分区键中的列或表达式使用带固定前缀的 `IN` 或 `LIKE`，或者包含这些列的某些部分重复函数，或者包含这些表达式之间的逻辑关系，则可以使用索引。

因此，可以在主键的一个或多个范围上快速执行查询。在这个示例中，如果查询针对特定的跟踪标签、特定标签和日期范围、特定标签和日期，或者多个标签加上日期范围等条件执行，查询都会很快。

下面来看按如下方式配置的引擎：

```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

在这种情况下，对于查询：

```sql
SELECT count() FROM table
WHERE EventDate = toDate(now())
AND CounterID = 34

SELECT count() FROM table
WHERE EventDate = toDate(now())
AND (CounterID = 34 OR CounterID = 42)

SELECT count() FROM table
WHERE ((EventDate >= toDate('2014-01-01')
AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01'))
AND CounterID IN (101500, 731962, 160656)
AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse 将使用主键索引来剪枝不满足条件的数据，并使用按月分区键来剪枝不在目标日期范围内的分区。

上述查询表明，即使对于复杂表达式，也会使用索引。对表的读取经过了这样的组织，因此使用索引不会比全表扫描更慢。

在下面的示例中，无法使用索引。

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

要检查 ClickHouse 在运行查询时能否使用索引，请使用设置 [force&#95;index&#95;by&#95;date](/zh/operations/settings/settings.md/#force_index_by_date) 和 [force&#95;primary&#95;key](/zh/operations/settings/settings#force_primary_key)。

按月分区的键可以只读取包含相应日期范围的数据块。在这种情况下，一个数据块可能包含多个日期的数据 (最多可涵盖整个月) 。在块内，数据按主键排序，而主键的第一列未必是日期。因此，如果查询只包含日期条件，却没有指定主键前缀，那么读取的数据会比仅查询单个日期时更多。

<div id="use-of-index-for-deterministic-expressions-in-primary-keys">
  ### 对主键中的确定性表达式使用索引
</div>

主键不仅可以包含列名，也可以包含表达式。这些表达式并不局限于简单的函数链：只要是确定性的，就可以是任意形式的表达式树 (例如嵌套函数和复合表达式) 。

如果一个表达式对于相同的输入值总是返回相同的结果 (例如：`length()`, `toDate()`, `lower()`, `left()`, `cityHash64()`, `toUUID()`；不同于 `now()` 或 `rand()`) ，那么它就是**确定性的**。如果主键中包含确定性表达式，ClickHouse 就可以将这些表达式应用于查询中的常量值，并利用结果在主键索引上构造条件。这使得 `=`、`IN` 和 `has` 这类谓词也能实现数据跳过。

一个常见的用例是让主键保持紧凑 (例如，存储哈希而不是较长的 `String`) ，同时仍允许针对原始列的谓词使用索引。

确定性 (但非单射) 主键示例：

```sql
ENGINE = MergeTree()
ORDER BY length(user_id)
```

可以使用索引的示例谓词：

```sql
SELECT * FROM table WHERE user_id = 'alice';
SELECT * FROM table WHERE user_id IN ('alice', 'bob');
SELECT * FROM table WHERE has(['alice', 'bob'], user_id);
```

在这些情况下，ClickHouse 会将 `length('alice')` (以及其他常量) 只计算一次，并利用这些长度值缩小主键索引中的范围。由于字符串长度**不是单射的**，不同的 `user_id` 字符串可能具有相同的长度，因此索引可能会读取额外的粒度 (误报) 。但结果仍然正确，因为读取后仍会应用原始谓词 (`user_id = ...`、`IN` 等) 。

如果该确定性表达式同时还是**单射的** (对于所使用的参数类型，不同输入不会产生相同输出) ，那么 ClickHouse 还可以有效地将索引用于其取反形式：`!=`、`NOT IN` 和 `NOT has(...)`。例如，对于 `String`，`reverse(p)` 和 `hex(p)` 都是单射的。

单射主键示例：

```sql
ENGINE = MergeTree()
ORDER BY hex(p)
```

也支持更复杂的单射表达式，例如：

```sql
ENGINE = MergeTree()
ORDER BY reverse(tuple(reverse(p), hex(p)))
```

可使用该索引的示例谓词：

```sql
SELECT * FROM table WHERE p != 'abc';
SELECT * FROM table WHERE p NOT IN ('abc', '12345');
SELECT * FROM table WHERE NOT has(['abc', '12345'], p);
```

<div id="use-of-index-for-partially-monotonic-primary-keys">
  ### 部分单调主键中索引的使用
</div>

例如，月中的日期在单个月份内构成一个[单调序列](https://en.wikipedia.org/wiki/Monotonic_function)，但如果时间跨度更长，就不再单调。这就是部分单调序列。如果用户使用部分单调主键创建表，ClickHouse 仍会像往常一样创建稀疏索引。当用户从这类表中查询数据时，ClickHouse 会分析查询条件。如果用户想获取索引中两个标记之间的数据，并且这两个标记都位于同一个月内，那么在这种特定情况下，ClickHouse 可以使用索引，因为它能够计算查询参数与索引标记之间的距离。

如果查询参数范围内的主键值不构成单调序列，ClickHouse 就无法使用索引。在这种情况下，ClickHouse 会采用全表扫描方法。

ClickHouse 不仅将这一逻辑用于月内日期序列，也适用于任何表示部分单调序列的主键。

<div id="table_engine-mergetree-data_skipping-indexes">
  ### 数据跳过索引
</div>

索引声明位于 `CREATE` 查询中 columns 部分。

```sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

对于 `*MergeTree` 家族中的表，可以指定数据跳过索引。

这些索引会在由 `granularity_value` 个粒度组成的块上，对指定表达式聚合某些信息 (粒度大小通过表引擎中的 `index_granularity` 设置指定) 。随后，这些聚合信息会在 `SELECT` 查询中使用，通过跳过那些无法满足 `where` 查询条件的大块数据，减少从磁盘读取的数据量。

`GRANULARITY` 子句可以省略，`granularity_value` 的默认值为 1。

**示例**

```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX idx1 u64 TYPE bloom_filter GRANULARITY 3,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 3,
    INDEX idx3 u64 * length(s) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

示例中的索引可用于让 ClickHouse 在以下查询中减少从磁盘读取的数据量：

```sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

也可以基于复合列创建数据跳过索引：

```sql
-- on columns of type Map:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- on columns of type JSON:
INDEX json_paths_index JSONAllPaths(json_column) TYPE bloom_filter

-- on columns of type Tuple:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- on columns of type Nested:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

<div id="skip-index-types">
  ### 跳过索引类型
</div>

`MergeTree` 表引擎支持以下类型的跳过索引。
有关使用跳过索引进行性能优化的更多信息，
请参阅 [“了解 ClickHouse 数据跳过索引”](/zh/optimize/skipping-indexes)。

* [`MinMax`](#minmax) 索引
* [`Set`](#set) 索引
* [`bloom_filter`](#bloom-filter) 索引
* [`ngrambf_v1`](#n-gram-bloom-filter) 索引 *(已弃用)*
* [`tokenbf_v1`](#token-bloom-filter) 索引 *(已弃用)*
* [`text`](#text) 索引
* [`vector_similarity`](#vector-similarity) 索引

<div id="minmax">
  #### MinMax 跳过索引
</div>

对于每个索引粒度，都会存储表达式的最小值和最大值。
(如果表达式的类型是 `tuple`，则会为元组中的每个元素分别存储最小值和最大值。)

```text title="Syntax"
minmax
```

<div id="set">
  #### Set
</div>

对于每个索引粒度，最多存储指定表达式的 `max_rows` 个唯一值。
`max_rows = 0` 表示 &quot;存储所有唯一值&quot;。

```text title="Syntax"
set(max_rows)
```

<div id="bloom-filter">
  #### 布隆过滤器
</div>

每个索引粒度都会为指定列存储一个[布隆过滤器](https://en.wikipedia.org/wiki/Bloom_filter)。

```text title="Syntax"
bloom_filter([false_positive_rate])
```

`false_positive_rate` 参数可以取 0 到 1 之间的值 (默认值：`0.025`) ，用于指定产生误报的概率 (这会增加需要读取的数据量) 。

支持以下数据类型：

* `(U)Int*`
* `Float*`
* `Enum`
* `Date`
* `DateTime`
* `String`
* `FixedString`
* `Array`
* `LowCardinality`
* `Nullable`
* `UUID`
* `Map`

:::note Map 数据类型：指定为键或值创建索引
对于 `Map` 数据类型，客户端可以使用 [`mapKeys`](/zh/sql-reference/functions/tuple-map-functions.md/#mapKeys) 或 [`mapValues`](/zh/sql-reference/functions/tuple-map-functions.md/#mapValues) 函数来指定索引是为键还是为值创建。
:::

:::note JSON 数据类型：为 JSON 路径建立索引
对于 [`JSON`](/zh/sql-reference/data-types/newjson) 数据类型，可以使用 [`JSONAllPaths`](/zh/sql-reference/functions/json-functions#JSONAllPaths) 函数在路径集合上创建布隆过滤器索引。这样可以跳过所查询的 JSON 路径不存在的粒度。详见 [JSON 的数据跳过索引](/zh/sql-reference/data-types/newjson#data-skipping-indexes-for-json)。
:::

<div id="n-gram-bloom-filter">
  #### N-gram bloom filter&#x20;*&#x20;(已弃用)&#x20;*
</div>

:::note
从 ClickHouse 26.2 版本开始，随着 `text` 索引正式 GA，`ngrambf_v1` 索引已不再推荐用于全文检索。

详情请参阅[“使用文本索引进行全文检索”](./textindexes.md)。
:::

对于每个索引粒度，都会为指定列的 [n-grams](https://en.wikipedia.org/wiki/N-gram) 存储一个 [布隆过滤器](https://en.wikipedia.org/wiki/Bloom_filter)。

```text title="Syntax"
ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

| 参数                              | 描述                                                          |
| ------------------------------- | ----------------------------------------------------------- |
| `n`                             | ngram 的大小                                                   |
| `size_of_bloom_filter_in_bytes` | 布隆过滤器的大小 (以字节为单位) 。这里可以使用较大的值，例如 `256` 或 `512`，因为它可以被很好地压缩。 |
| `number_of_hash_functions`      | 布隆过滤器中使用的哈希函数数量。                                            |
| `random_seed`                   | 布隆过滤器哈希函数的 seed。                                            |

此索引仅适用于以下数据类型：

* [`String`](/zh/sql-reference/data-types/string.md)
* [`FixedString`](/zh/sql-reference/data-types/fixedstring.md)
* [`Map`](/zh/sql-reference/data-types/map.md)

要估算 `ngrambf_v1` 的参数，可以使用以下[用户自定义函数 (UDFs) ](/zh/sql-reference/statements/create/function.md)。

```sql title="UDFs for ngrambf_v1"
CREATE FUNCTION bfEstimateFunctions [ON CLUSTER cluster]
AS
(total_number_of_all_grams, size_of_bloom_filter_in_bits) -> round((size_of_bloom_filter_in_bits / total_number_of_all_grams) * log(2));

CREATE FUNCTION bfEstimateBmSize [ON CLUSTER cluster]
AS
(total_number_of_all_grams,  probability_of_false_positives) -> ceil((total_number_of_all_grams * log(probability_of_false_positives)) / log(1 / pow(2, log(2))));

CREATE FUNCTION bfEstimateFalsePositive [ON CLUSTER cluster]
AS
(total_number_of_all_grams, number_of_hash_functions, size_of_bloom_filter_in_bytes) -> pow(1 - exp(-number_of_hash_functions/ (size_of_bloom_filter_in_bytes / total_number_of_all_grams)), number_of_hash_functions);

CREATE FUNCTION bfEstimateGramNumber [ON CLUSTER cluster]
AS
(number_of_hash_functions, probability_of_false_positives, size_of_bloom_filter_in_bytes) -> ceil(size_of_bloom_filter_in_bytes / (-number_of_hash_functions / log(1 - exp(log(probability_of_false_positives) / number_of_hash_functions))))
```

要使用这些函数，您至少需要指定两个参数：

* `total_number_of_all_grams`
* `probability_of_false_positives`

例如，某个粒度中有 `4300` 个 ngram，且您希望误报率低于 `0.0001`。
然后即可通过执行以下查询来估算其他参数：

```sql
--- estimate number of bits in the filter
SELECT bfEstimateBmSize(4300, 0.0001) / 8 AS size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- estimate number of hash functions
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘
```

当然，你也可以使用这些函数来估算其他条件下的参数。
上述函数参考了[这里](https://hur.st/bloomfilter)的 布隆过滤器 计算器。

<div id="token-bloom-filter">
  #### 标记布隆过滤器
</div>

:::note
从 ClickHouse 26.2 版本开始，随着 `text` 索引正式可用 (GA) ，`tokenbf_v1` 索引不再推荐用于全文检索。

详见[&quot;使用文本索引进行全文检索&quot;](./textindexes.md)页面。
:::

```text title="Syntax"
tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="sparse-grams-bloom-filter">
  #### 稀疏 grams 布隆过滤器
</div>

稀疏 grams 布隆过滤器与 `ngrambf_v1` 类似，但它使用的是[稀疏 grams 标记](/zh/sql-reference/functions/string-functions.md/#sparseGrams)，而不是 ngrams。

```text title="Syntax"
sparse_grams(min_ngram_length, max_ngram_length, min_cutoff_length, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)
```

<div id="text">
  ### 文本索引
</div>

基于分词后的字符串数据构建倒排索引，可实现高效且具有确定性的全文检索。详见[这里](textindexes.md)。

<div id="vector-similarity">
  #### 向量相似度
</div>

支持近似最近邻搜索，详情请参见[此处](annindexes.md)。

<div id="functions-support">
  ### 函数支持
</div>

`WHERE` 子句中的条件包含对作用于列的函数的调用。如果某列属于某个索引的一部分，ClickHouse 在执行这些函数时会尝试使用该索引。ClickHouse 支持不同的函数子集来使用索引。

`set` 类型的索引可供所有函数使用。其他索引类型的支持情况如下：

| 函数 (运算符) / 索引                                                                                                             | 主键 | minmax | ngrambf&#95;v1 | tokenbf&#95;v1 | bloom&#95;filter | sparse&#95;grams | text |
| ------------------------------------------------------------------------------------------------------------------------- | -- | ------ | -------------- | -------------- | ---------------- | ---------------- | ---- |
| [equals (=, ==)](/zh/sql-reference/functions/comparison-functions.md/#equals)                                                | ✔  | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notEquals(!=, &lt;&gt;)](/zh/sql-reference/functions/comparison-functions.md/#notEquals)                                    | ✔  | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [like](/zh/sql-reference/functions/string-search-functions.md/#like)                                                         | ✔  | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [notLike](/zh/sql-reference/functions/string-search-functions.md/#notLike)                                                   | ✔  | ✔      | ✔              | ✔              | ✗                | ✔                | ✗    |
| [match](/zh/sql-reference/functions/string-search-functions.md/#match)                                                       | ✗  | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [startsWith](/zh/sql-reference/functions/string-functions.md/#startsWith)                                                    | ✔  | ✔      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [endsWith](/zh/sql-reference/functions/string-functions.md/#endsWith)                                                        | ✗  | ✗      | ✔              | ✔              | ✗                | ✔                | ✔    |
| [multiSearchAny](/zh/sql-reference/functions/string-search-functions.md/#multiSearchAny)                                     | ✗  | ✗      | ✔              | ✗              | ✗                | ✗                | ✔    |
| [multiSearchAnyUTF8](/zh/sql-reference/functions/string-search-functions.md/#multiSearchAnyUTF8)                             | ✗  | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [multiMatchAny](/zh/sql-reference/functions/string-search-functions.md/#multiMatchAny)                                       | ✗  | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [in](/zh/sql-reference/functions/in-functions)                                                                               | ✔  | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [notIn](/zh/sql-reference/functions/in-functions)                                                                            | ✔  | ✔      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [less (`<`)](/zh/sql-reference/functions/comparison-functions.md/#less)                                                      | ✔  | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greater (`>`)](/zh/sql-reference/functions/comparison-functions.md/#greater)                                                | ✔  | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [lessOrEquals (`<=`)](/zh/sql-reference/functions/comparison-functions.md/#lessOrEquals)                                     | ✔  | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [greaterOrEquals (`>=`)](/zh/sql-reference/functions/comparison-functions.md/#greaterOrEquals)                               | ✔  | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [empty](/zh/sql-reference/functions/array-functions/#empty)                                                                  | ✔  | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [notEmpty](/zh/sql-reference/functions/array-functions/#notEmpty)                                                            | ✗  | ✔      | ✗              | ✗              | ✗                | ✔                | ✗    |
| [has](/zh/sql-reference/functions/array-functions#has)                                                                       | ✔  | ✔      | ✔              | ✔              | ✔                | ✔                | ✔    |
| [hasAny](/zh/sql-reference/functions/array-functions#hasAny)                                                                 | ✗  | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasAll](/zh/sql-reference/functions/array-functions#hasAll)                                                                 | ✗  | ✗      | ✔              | ✔              | ✔                | ✔                | ✗    |
| [hasToken](/zh/sql-reference/functions/string-search-functions.md/#hasToken)                                                 | ✗  | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenOrNull](/zh/sql-reference/functions/string-search-functions.md/#hasTokenOrNull)                                     | ✗  | ✗      | ✗              | ✔              | ✗                | ✗                | ✔    |
| [hasTokenCaseInsensitive (`*`)](/zh/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitive)             | ✗  | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasTokenCaseInsensitiveOrNull (`*`)](/zh/sql-reference/functions/string-search-functions.md/#hasTokenCaseInsensitiveOrNull) | ✗  | ✗      | ✗              | ✔              | ✗                | ✗                | ✗    |
| [hasAnyTokens](/zh/sql-reference/functions/string-search-functions.md/#hasAnyTokens)                                         | ✗  | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [hasAllTokens](/zh/sql-reference/functions/string-search-functions.md/#hasAllTokens)                                         | ✗  | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [pointInPolygon](/zh/sql-reference/functions/geo/coordinates.md#pointinpolygon)                                              | ✔  | ✔      | ✗              | ✗              | ✗                | ✗                | ✗    |
| [mapContains (mapContainsKey)](/zh/sql-reference/functions/tuple-map-functions#mapContainsKey)                               | ✗  | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsKeyLike](/zh/sql-reference/functions/tuple-map-functions#mapContainsKeyLike)                                     | ✗  | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValue](/zh/sql-reference/functions/tuple-map-functions#mapContainsValue)                                         | ✗  | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |
| [mapContainsValueLike](/zh/sql-reference/functions/tuple-map-functions#mapContainsValueLike)                                 | ✗  | ✗      | ✗              | ✗              | ✗                | ✗                | ✔    |

带有常量参数且该参数小于 ngram 大小的函数，无法被 `ngrambf_v1` 用于查询优化。

(*) 要使 `hasTokenCaseInsensitive` 和 `hasTokenCaseInsensitiveOrNull` 生效，必须基于已转换为小写的数据创建 `tokenbf_v1` 索引，例如 `INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)`。

:::note
布隆过滤器可能会产生假阳性，因此 `ngrambf_v1`、`tokenbf_v1`、`sparse_grams` 和 `bloom_filter` 索引不能用于优化函数结果预期为 false 的查询。

例如：

* 可以优化：
  * `s LIKE '%test%'`
  * `NOT s NOT LIKE '%test%'`
  * `s = 1`
  * `NOT s != 1`
  * `startsWith(s, 'test')`
* 不能优化：
  * `NOT s LIKE '%test%'`
  * `s NOT LIKE '%test%'`
  * `NOT s = 1`
  * `s != 1`
  * `NOT startsWith(s, 'test')`
    :::

<div id="projections">
  ## 投影
</div>

投影类似于 [materialized views](/zh/sql-reference/statements/create/view)，但定义在分片级别。它可在查询中自动用于查询，并提供一致性保证。

:::note
实现投影时，还应考虑 [force&#95;optimize&#95;projection](/zh/operations/settings/settings#force_optimize_projection) 设置。
:::

带有 [FINAL](/zh/sql-reference/statements/select/from#final-modifier) 修饰符的 `SELECT` 语句不支持投影。

<div id="projection-query">
  ### 投影查询
</div>

投影查询用于定义投影。它会隐式地从父表中选取数据。
**语法**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

投影可以使用 [ALTER](/zh/sql-reference/statements/alter/projection.md) 语句进行修改或删除。

<div id="projection-index">
  ### 投影索引
</div>

投影索引通过提供一种轻量且明确的方式来定义投影级索引，扩展了投影子系统。
从外部看，投影索引仍然属于投影，但语法更简洁、意图更清晰：它定义的是专用于过滤的表达式，而不是用于提供 materialized 数据。
从内部实现上看，投影索引不会像常规投影那样，按置换后的行顺序将原始表物化。
相反，置换信息会以数值置换列 `_part_offset` 的形式存储，即 `SELECT _part_offset ORDER BY <index_expr>`。

<div id="projection-index-syntax">
  #### 语法
</div>

```sql
PROJECTION <name> INDEX <index_expr> TYPE <index_type>
```

示例：

```sql
CREATE TABLE example
(
    id UInt64,
    region String,
    user_id UInt32,
    PROJECTION region_proj INDEX region TYPE basic,
    PROJECTION uid_proj INDEX user_id TYPE basic
)
ENGINE = MergeTree
ORDER BY id;
```

<div id="projection-index-types">
  #### 索引类型
</div>

当前支持：

* **basic**：等同于表达式上的普通 MergeTree 索引。

该框架未来还可以添加更多索引类型。

<div id="projection-storage">
  ### Projection 存储
</div>

Projection 存储在分片目录中。它类似于索引，但包含一个子目录，用于存储匿名 `MergeTree` 表的分片。该表由 Projection 的定义查询派生而来。如果存在 `GROUP BY` 子句，底层存储引擎会变为 [AggregatingMergeTree](aggregatingmergetree.md)，并且所有聚合函数都会被转换为 `AggregateFunction`。如果存在 `ORDER BY` 子句，`MergeTree` 表会将其用作主键表达式。在合并过程中，Projection 分片会通过其存储的合并逻辑进行合并。父表分片的 checksum 会与 Projection 分片的 checksum 合并。其他维护作业与数据跳过索引类似。

<div id="projection-query-analysis">
  ### 查询分析
</div>

1. 检查该投影能否用于回答给定查询，也就是说，它生成的结果必须与查询基表时相同。
2. 选择最佳的可行匹配，即需要读取的粒度最少的匹配。
3. 使用投影的查询管道不同于使用原始 parts 的查询管道。如果某些 parts 中缺少该投影，我们可以动态添加相应的管道，以即时生成该投影。

<div id="concurrent-data-access">
  ## 并发数据访问
</div>

对于表的并发访问，我们采用多版本机制。换句话说，当一个表被同时读取和更新时，读取的数据来自查询发起时当前的一组 parts。无需长时间加锁。插入操作不会影响读取操作。

从表中读取数据时会自动并行化。

<div id="table_engine-mergetree-ttl">
  ## 列和表的 生存时间 (TTL)
</div>

用于确定值的生命周期。

可以为整个表以及每个单独的列设置 `TTL` 子句。表级 `生存时间 (TTL)` 还可以指定数据在磁盘和卷之间自动移动的逻辑，或者在其中所有数据都已过期时重新压缩 parts。

表达式必须求值为 [Date](/zh/sql-reference/data-types/date.md)、[Date32](/zh/sql-reference/data-types/date32.md)、[DateTime](/zh/sql-reference/data-types/datetime.md) 或 [DateTime64](/zh/sql-reference/data-types/datetime64.md) 数据类型。

:::tip[避免在 生存时间 (TTL) 表达式中使用非确定性函数]
TTL 在后台合并期间计算，而不是在写入时计算。
像 `rand()`、`now()` 或 `now64()` 这样的函数会在每次合并时重新计算，从而导致不可预测的删除行为。
ClickHouse 会阻止完全不依赖任何列的表达式，但目前不会拒绝与列引用混合使用的非确定性函数 (例如 `ts + rand()`) 。为了获得可预测的结果，生存时间 (TTL) 表达式应仅基于由列推导出的确定性值。
:::

**语法**

为列设置生存时间 (生存时间 (TTL))：

```sql
TTL time_column
TTL time_column + interval
```

要定义 `interval`，请使用[时间间隔](/zh/sql-reference/operators#operators-for-working-with-dates-and-times)运算符，例如：

```sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

<div id="mergetree-column-ttl">
  ### 列 生存时间 (TTL)
</div>

当列中的值过期后，ClickHouse 会将其替换为该列数据类型的默认值。如果某个数据分区片段中该列的所有值都已过期，ClickHouse 会从文件系统中的该数据分区片段中删除这一列。

`生存时间 (TTL)` 子句不能用于键列。

**示例**

<div id="creating-a-table-with-ttl">
  #### 创建带有 `生存时间 (TTL)` 的表：
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

<div id="adding-ttl-to-a-column-of-an-existing-table">
  #### 为现有表的列添加生存时间 (TTL)
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

<div id="altering-ttl-of-the-column">
  #### 修改列的生存时间 (TTL)
</div>

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

<div id="mergetree-table-ttl">
  ### 表生存时间 (TTL)
</div>

表可以定义用于删除过期行的表达式，以及多个用于在[磁盘或卷](#table_engine-mergetree-multiple-volumes)之间自动移动 parts 的表达式。当表中的行过期时，ClickHouse 会删除对应的所有行。对于 parts 的移动或重新压缩，某个 part 中的所有行都必须满足 `TTL` 表达式的条件。

```sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

每个 TTL 表达式后都可以跟一个 TTL 规则类型。它决定了当表达式条件满足时 (即达到当前时间) 要执行的操作：

* `DELETE` - 删除过期行 (默认操作) ；
* `RECOMPRESS codec_name` - 使用 `codec_name` 重新压缩数据分区片段；
* `TO DISK 'aaa'` - 将分区片段移动到磁盘 `aaa`；
* `TO VOLUME 'bbb'` - 将分区片段移动到卷 `bbb`；
* `GROUP BY` - 聚合过期行。

`DELETE` 操作可以与 `WHERE` 子句结合使用，根据过滤条件仅删除部分过期行：

```sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

`GROUP BY` 表达式必须是表主键的前缀。

如果某一列不属于 `GROUP BY` 表达式，且未在 `SET` 子句中显式设置，那么在结果行中，该列会包含分组后各行中的某个值 (就像对其应用了聚合函数 `any` 一样) 。

**示例**

<div id="creating-a-table-with-ttl">
  #### 创建带有 `生存时间 (TTL)` 的表：
</div>

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE,
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

<div id="altering-ttl-of-the-table">
  #### 修改表的 `生存时间 (TTL)`：
</div>

```sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

创建一个表，其中的行会在一个月后过期。过期且日期为星期一的行将被删除：

```sql
CREATE TABLE table_with_where
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
```

<div id="creating-a-table-where-expired-rows-are-recompressed">
  #### 创建一个表，对过期行重新压缩：
</div>

```sql
CREATE TABLE table_for_recompression
(
    d DateTime,
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
```

创建一个表，其中过期行会被聚合。在结果行中，`x` 包含分组后各行中的最大值，`y` —— 最小值，`d` —— 分组后各行中的任意一个值。

```sql
CREATE TABLE table_for_aggregation
(
    d DateTime,
    k1 Int,
    k2 Int,
    x Int,
    y Int
)
ENGINE = MergeTree
ORDER BY (k1, k2)
TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
```

<div id="mergetree-removing-expired-data">
  ### 删除过期数据
</div>

带有已过期 `TTL` 的数据会在 ClickHouse 合并数据 parts 时被删除。

当 ClickHouse 检测到数据已过期时，会执行一次计划外合并。要控制此类合并的频率，可以设置 `merge_with_ttl_timeout`。如果该值过低，可能会执行大量计划外合并，从而消耗大量资源。

如果在两次合并之间执行 `SELECT` 查询，可能会查到过期数据。为避免这种情况，请在执行 `SELECT` 之前先使用 [OPTIMIZE](/zh/sql-reference/statements/optimize.md) 查询。

**另请参见**

* [ttl&#95;only&#95;drop&#95;parts](/zh/operations/settings/merge-tree-settings#ttl_only_drop_parts) 设置

<div id="disk-types">
  ## 磁盘类型
</div>

除了本地块设备外，ClickHouse 还支持以下存储类型：

* [`s3` 用于 S3 和 MinIO](#table_engine-mergetree-s3)
* [`gcs` 用于 GCS](/zh/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
* [`blob_storage_disk` 用于 Azure Blob 存储](/zh/operations/storing-data#azure-blob-storage)
* [`hdfs` 用于 HDFS](/zh/engines/table-engines/integrations/hdfs)
* [`web` 用于通过 Web 进行只读访问](/zh/operations/storing-data#web-storage)
* [`cache` 用于本地缓存](/zh/operations/storing-data#using-local-cache)
* [`s3_plain` 用于备份到 S3](/zh/operations/backup/disk)
* [`s3_plain_rewritable` 用于 S3 中不可变的非复制表](/zh/operations/storing-data.md#s3-plain-rewritable-storage)

<div id="table_engine-mergetree-multiple-volumes">
  ## 使用多个块设备存储数据
</div>

<div id="introduction">
  ### 简介
</div>

`MergeTree` 家族表引擎可将数据存储在多个块设备上。例如，当某个表的数据自然分为“热”数据和“冷”数据时，这种方式就很有用。最新数据会被频繁查询，但只占用少量空间。相反，长尾历史数据很少被查询。如果有多个磁盘可用，“热”数据可以放在高速磁盘上 (例如 NVMe SSD 或内存中) ，而“冷”数据则可以放在相对较慢的磁盘上 (例如 HDD) 。

这适用于所有磁盘类型，包括 S3 和其他对象存储磁盘。例如，你可以在单个卷内将数据分散到多个 S3 存储桶中，或者创建分层策略，将数据从本地磁盘迁移到 S3。详情请参见[使用具有多个卷的 S3 磁盘](#s3-multiple-volumes)。

对于 `MergeTree` 引擎表，parts 是最小的可移动单元。属于同一个 parts 的数据会存储在同一块磁盘上。parts 既可以在后台于磁盘之间移动 (依据用户设置) ，也可以通过 [ALTER](/zh/sql-reference/statements/alter/partition) 查询移动。

<div id="terms">
  ### 术语
</div>

* 磁盘 — 挂载到文件系统上的块设备。
* 默认磁盘 — 存储 [path](/zh/operations/server-configuration-parameters/settings.md/#path) 服务器设置所指定路径的磁盘。
* 卷 — 由相同类型磁盘组成的有序集合 (类似于 [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)) 。
* 存储策略 — 由多个卷及其间数据移动规则组成的集合。

上述实体的名称可在系统表 [system.storage&#95;policies](/zh/operations/system-tables/storage_policies) 和 [system.disks](/zh/operations/system-tables/disks) 中找到。要为某个表应用已配置的存储策略，请使用 `MergeTree` 引擎家族表的 `storage_policy` 设置。

<div id="table_engine-mergetree-multiple-volumes_configure">
  ### 配置
</div>

磁盘、卷和存储策略应在 `<storage_configuration>` 标签内声明，并放在 `config.d` 目录中的文件中。

:::tip
也可以在查询的 `SETTINGS` 部分中声明磁盘。这对于临时分析非常有用，
例如可临时附加一个托管在某个 URL 上的磁盘。
更多详情，请参见[动态存储](/zh/operations/storing-data#dynamic-configuration)。
:::

配置结构：

```xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

标签：

* `<disk_name_N>` — 磁盘名称。所有磁盘的名称都必须各不相同。
* `path` — 服务器用于存储数据 (`data` 和 `shadow` 文件夹) 的路径，应以 &#39;/&#39; 结尾。
* `keep_free_space_bytes` — 需要预留的磁盘空闲空间大小。

磁盘定义的顺序无关紧要。

存储策略配置标记：

```xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                    <load_balancing>round_robin</load_balancing>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

标签：

* `policy_name_N` — 策略名称。策略名称必须唯一。
* `volume_name_N` — 卷名称。卷名称必须唯一。
* `disk` — 卷中的一个磁盘。
* `max_data_part_size_bytes` — 该卷中任一磁盘上可存储的分区片段最大大小。如果估算某个 merged part 的大小会超过 `max_data_part_size_bytes`，则该分区片段会写入下一个卷。这个功能主要用于将新的或较小的 parts 保留在热 (SSD) 卷上，并在其变大后转移到冷 (HDD) 卷上。如果你的策略只有一个卷，请不要使用此设置。
* `move_factor` — 当可用空间低于该因子时，数据会自动开始移动到下一个卷 (如果存在，默认值为 0.1) 。ClickHouse 会按大小从大到小 (降序) 对现有 parts 排序，并选择总大小足以满足 `move_factor` 条件的 parts。如果所有 parts 的总大小仍然不足，则会移动所有 parts。
* `perform_ttl_move_on_insert` — 禁止在数据分区片段 INSERT 时执行 TTL move。默认情况下 (启用时) ，如果插入的数据分区片段根据 TTL move 规则已经过期，它会立即进入 move 规则中声明的卷/磁盘。如果目标端卷/磁盘较慢 (例如 S3) ，这可能会显著降低 insert 速度。如果禁用，则已过期的数据分区片段会先写入默认卷，然后立即移动到 TTL 卷。
* `load_balancing` - 磁盘负载均衡策略，`round_robin` 或 `least_used`。
* `least_used_ttl_ms` - 配置所有磁盘可用空间的更新 timeout (以毫秒为单位)  (`0` - 始终更新，`-1` - 从不更新，默认值为 `60000`) 。注意，如果该磁盘仅供 ClickHouse 使用，且不会发生在线 filesystem 扩容/缩容，则可以使用 `-1`；其他情况下不建议这样做，因为最终会导致错误的空间分配。
* `prefer_not_to_merge` — 你不应使用此设置。它会禁用此卷上数据 parts 的 merging (这有害且会导致性能下降) 。启用此设置时 (不要这样做) ，将不允许在此卷上 merging 数据 (这很糟糕) 。这让你可以 (但并不需要) 控制 (如果你想控制这些，那你就错了) ClickHouse 如何使用慢速磁盘 (但 ClickHouse 更清楚该怎么做，所以请不要使用此设置) 。
* `volume_priority` — 定义卷的填充优先级 (顺序) 。值越小，优先级越高。该参数的值应为自然数，并且整体上应无跳号地覆盖从 1 到 N 的范围 (N 为最低优先级) 。
  * 如果 *所有* 卷都带有标签，则按给定顺序确定优先级。
  * 如果只有 *部分* 卷带有标签，则未带标签的卷优先级最低，并按其在 config 中定义的顺序确定优先级。
  * 如果 *没有* 卷带有标签，则其优先级对应于它们在 configuration 中声明的顺序。
  * 两个卷不能具有相同的优先级值。

配置示例：

```xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>

        <small_jbod_with_external_no_merges>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                </external>
            </volumes>
        </small_jbod_with_external_no_merges>
    </policies>
    ...
</storage_configuration>
```

在给定示例中，`hdd_in_order` 策略采用了 [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) 方法。因此，该策略只定义了一个卷 (`single`) ，数据 parts 会按循环顺序存储到该卷的所有磁盘上。如果系统中挂载了多个性能相近的磁盘，但未配置 RAID，这种策略会很有用。请注意，单块磁盘本身并不可靠，你可能需要通过将复制因子设为 3 或更高来弥补这一点。

如果系统中有不同类型的磁盘，则可以改用 `moving_from_ssd_to_hdd` 策略。卷 `hot` 由一个 SSD 磁盘 (`fast_ssd`) 组成，且可存储在该卷上的单个分区片段最大大小为 1GB。所有大于 1GB 的 parts 都会直接存储在 `cold` 卷上，其中包含一个 HDD 磁盘 `disk1`。
此外，一旦 `fast_ssd` 磁盘的使用率超过 80%，后台进程就会将数据转移到 `disk1`。

在存储策略中，卷的枚举顺序非常重要，尤其是在列出的卷中至少有一个未显式指定 `volume_priority` parameter 的情况下。
一旦某个卷已满，数据就会被移动到下一个卷。磁盘的枚举顺序也同样重要，因为数据会依次轮流存储到这些磁盘上。

创建表时，可以为其应用一个已配置的存储策略：

```sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

`default` 存储策略表示仅使用一个卷，而该卷仅包含 `<path>` 中指定的一个磁盘。
创建表后，您可以使用 [ALTER TABLE ... MODIFY SETTING] 查询修改存储策略；新策略应包含所有旧磁盘以及同名卷。

执行数据 parts 后台移动的线程数可以通过 [background&#95;move&#95;pool&#95;size](/zh/operations/server-configuration-parameters/settings.md/#background_move_pool_size) 设置进行调整。

<div id="details">
  ### 细节
</div>

对于 `MergeTree` 表，数据会通过不同方式写入磁盘：

* 作为 insert (`INSERT` 查询) 的结果。
* 在后台合并和[变更](/zh/sql-reference/statements/alter#mutations)期间。
* 从另一个副本下载时。
* 作为分区冻结 [ALTER TABLE ... FREEZE PARTITION](/zh/sql-reference/statements/alter/partition#freeze-partition) 的结果。

除变更和分区冻结外，在上述所有情况下，part 都会按照给定的存储策略存放到某个 volume 和 disk 上：

1. 选择第一个 (按定义顺序) 具有足够磁盘空间来存储 part (`unreserved_space > current_part_size`) ，并且允许存储该大小 parts (`max_data_part_size_bytes > current_part_size`) 的 volume。
2. 在该 volume 内，选择位于上一个数据 chunk 所存储 disk 之后的那个 disk，并且其可用空间大于 part 大小 (`unreserved_space - keep_free_space_bytes > current_part_size`) 。

在底层实现中，变更和分区冻结会使用 [hard links](https://en.wikipedia.org/wiki/Hard_link)。不同 disk 之间不支持 hard links，因此在这种情况下，生成的 parts 会存储在与原始 parts 相同的 disk 上。

在后台，parts 会根据可用空间的多少 (`move_factor` 参数) ，按照配置文件中声明 volume 的顺序在各个 volume 之间移动。
数据绝不会从最后一个传输到第一个。可以使用系统表 [system.part&#95;log](/zh/operations/system-tables/part_log) (field `type = MOVE_PART`) 和 [system.parts](/zh/operations/system-tables/parts.md) (fields `path` 和 `disk`) 来监控后台移动。此外，也可以在服务器日志中查看详细信息。

用户可以使用查询 [ALTER TABLE ... MOVE PART|PARTITION ... TO VOLUME|DISK ...](/zh/sql-reference/statements/alter/partition) 强制将某个 part 或分区从一个 volume 移动到另一个 volume，后台操作的所有限制都会被纳入考虑。该查询会自行发起移动，不会等待后台操作完成。如果没有足够的可用空间，或任何必需的 condition 未满足，用户都会收到错误消息。

移动数据不会影响数据复制。因此，同一个表在不同副本上可以指定不同的存储策略。

后台合并和变更完成后，旧 parts 只有在经过一段时间 (`old_parts_lifetime`) 后才会被删除。
在此期间，它们不会被移动到其他 volume 或 disk。因此，在这些 parts 最终被删除之前，它们仍会计入已占用磁盘空间的评估。

用户可以使用 [min&#95;bytes&#95;to&#95;rebalance&#95;partition&#95;over&#95;jbod](/zh/operations/settings/merge-tree-settings.md/#min_bytes_to_rebalance_partition_over_jbod) 设置，将新的大 parts 以均衡方式分配到 [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) volume 的不同 disk 上。

<div id="table_engine-mergetree-s3">
  ## 使用外部存储来存储数据
</div>

[MergeTree](/zh/engines/table-engines/mergetree-family/mergetree.md) 家族表引擎可通过类型分别为 `s3`、`azure_blob_storage`、`hdfs` 的 disk，将数据存储到 `S3`、`AzureBlobStorage` 和 `HDFS`。更多详情，请参阅[配置外部存储选项](/zh/operations/storing-data.md/#configuring-external-storage)。

以下示例展示了如何将 [S3](https://aws.amazon.com/s3/) 用作外部存储，并使用类型为 `s3` 的 disk。

配置如下：

```xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <support_batch_delete>true</support_batch_delete>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <header>Authorization: Bearer SOME-TOKEN</header>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <server_side_encryption_kms_key_id>your_kms_key_id</server_side_encryption_kms_key_id>
            <server_side_encryption_kms_encryption_context>your_kms_encryption_context</server_side_encryption_kms_encryption_context>
            <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled>
            <proxy>
                <uri>http://proxy1</uri>
                <uri>http://proxy2</uri>
            </proxy>
            <connect_timeout_ms>10000</connect_timeout_ms>
            <request_timeout_ms>5000</request_timeout_ms>
            <retry_attempts>10</retry_attempts>
            <single_read_retries>4</single_read_retries>
            <min_bytes_for_seek>1000</min_bytes_for_seek>
            <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            <skip_access_check>false</skip_access_check>
        </s3>
        <s3_cache>
            <type>cache</type>
            <disk>s3</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    ...
</storage_configuration>
```

另请参见[配置外部存储选项](/zh/operations/storing-data.md/#configuring-external-storage)。

<div id="s3-multiple-volumes">
  ### 将 S3 磁盘与多个卷配合使用
</div>

S3 (以及其他对象存储) 磁盘与本地磁盘一样，也可用于多磁盘和多卷存储策略。这样一来，你可以在单个卷内将数据以 JBOD 的方式分布到多个 S3 存储桶中，或者使用 S3 卷设置分层存储策略。

例如，要以轮询方式将数据分布到两个 S3 存储桶中：

```xml
<storage_configuration>
    <disks>
        <s3_bucket1>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-1/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket1>
        <s3_bucket2>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/bucket-2/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_bucket2>
    </disks>
    <policies>
        <s3_multi_bucket>
            <volumes>
                <main>
                    <disk>s3_bucket1</disk>
                    <disk>s3_bucket2</disk>
                </main>
            </volumes>
        </s3_multi_bucket>
    </policies>
</storage_configuration>
```

你还可以在分层存储策略中组合使用本地卷和 S3 卷，例如让数据随着时间推移从本地 SSD 移动到 S3：

```xml
<storage_configuration>
    <disks>
        <local_ssd>
            <path>/mnt/fast_ssd/clickhouse/</path>
        </local_ssd>
        <s3_cold>
            <type>s3</type>
            <endpoint>https://s3.amazonaws.com/cold-storage/data/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3_cold>
    </disks>
    <policies>
        <local_to_s3>
            <volumes>
                <hot>
                    <disk>local_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>s3_cold</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </local_to_s3>
    </policies>
</storage_configuration>
```

:::note
使用 `use_environment_credentials` 进行 S3 身份验证时，环境凭据 (`AWS_ACCESS_KEY_ID`、`AWS_SECRET_ACCESS_KEY`、`AWS_SESSION_TOKEN`) 会在所有 S3 disk 之间共享。无法为不同的 disk 使用不同的环境凭据。如果需要为每个 S3 disk 使用不同的凭据，请改为在每个 disk 上显式设置 `access_key_id` 和 `secret_access_key`。
:::

可以在共享存储上为非复制表 MergeTree 表配置单写入、多读取场景。这是通过自动刷新 parts 列表实现的，并且可在读取端进行配置。请注意，这要求各副本之间共享 filesystem metadata (或者在使用表本地 disk 时设置 `table_disk = true`) 。请参阅 [refresh&#95;parts&#95;interval and table&#95;disk](/zh/operations/storing-data.md/#refresh-parts-interval-and-table-disk)。

:::note cache 配置
ClickHouse 22.3 到 22.7 版本使用不同的 cache 配置；如果你使用的是其中某个版本，请参阅[使用本地 cache](/zh/operations/storing-data.md/#using-local-cache)。
:::

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_part` — 数据分片名称。
* `_part_index` — 该数据分片在查询结果中的顺序索引。
* `_part_starting_offset` — 该数据分片在查询结果中的累计起始行。
* `_part_offset` — 该数据分片中的行号。
* `_part_granule_offset` — 该数据分片中的粒度编号。
* `_partition_id` — 分区名称。
* `_part_uuid` — 唯一的数据分片标识符 (如果启用了 MergeTree 设置 `assign_part_uuids`) 。
* `_part_data_version` — 数据分片的数据版本 (最小块编号或变更版本) 。
* `_partition_value` — `partition by` 表达式的值 (一个元组) 。
* `_sample_factor` — 样本因子 (来自查询) 。
* `_block_number` — 插入时分配给该行的原始块编号；启用设置 `enable_block_number_column` 后，该值在合并后仍会保留。
* `_block_offset` — 插入时分配给该行的原始块内行号；启用设置 `enable_block_offset_column` 后，该值在合并后仍会保留。
* `_disk_name` — 用于存储的磁盘名称。

<div id="column-statistics">
  ## 列统计信息
</div>

<CloudNotSupportedBadge />

对于 `*MergeTree*` 家族的表，统计信息声明位于 `CREATE` 查询的列定义部分：

```sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

我们还可以使用 `ALTER` 语句来管理统计信息：

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

这些轻量级统计信息会汇总列中值分布的相关信息。统计信息存储在每个分片中，并会在每次执行 insert 时更新。
只有启用 `set use_statistics = 1` 后，它们才能用于 PREWHERE 优化。

<div id="part-pruning-with-statistics">
  #### 使用统计信息进行数据分区片段裁剪
</div>

启用 `use_statistics_for_part_pruning` 后，可以使用统计信息进行数据分区片段裁剪。
目前，只有 `MinMax` 和 `Basic` 统计信息支持数据分区片段裁剪。当为某一列定义了这类统计信息时，ClickHouse 会跟踪该列在每个数据分区片段中的最小值和最大值。
数据分区片段裁剪可在查询过滤条件不可能匹配某个数据分区片段中的任何行时，跳过读取整个数据分区片段。

**示例：**

```sql
-- Create a table with MinMax statistics on the 'value' column
CREATE TABLE test_stats
(
    id UInt64,
    value Int64 STATISTICS(MinMax)
)
ENGINE = MergeTree
ORDER BY id;

SYSTEM STOP MERGES test_stats;

-- Insert data in separate inserts to create multiple parts
INSERT INTO test_stats SELECT number, number FROM numbers(1000); -- Part 1: value range [0, 999]
INSERT INTO test_stats SELECT number, number + 10000 FROM numbers(1000); -- Part 2: value range [10000, 10999]

SET use_statistics_for_part_pruning = 1;

-- This query will skip Part 1 entirely because its max value (999) < 5000
SELECT count() FROM test_stats WHERE value > 5000;

-- Use EXPLAIN to see the pruning effect
EXPLAIN indexes = 1 SELECT count() FROM test_stats WHERE value > 5000;
-- The output will show "Parts: 1/2" indicating one part was pruned
```

<div id="available-types-of-column-statistics">
  ### 可用的列统计信息类型
</div>

* `Basic`

  从列中提取的一组紧凑的单值摘要。根据列类型，会填充以下信息：

  * 对于任何值可表示为数值的列 (整数、浮点数、`Decimal*`、`Date*`、`DateTime*`、`Enum*`、`IPv4` 等) ：最小值和最大值，可用于估算范围过滤器的选择性并启用数据分区片段裁剪；
  * 对于 `String` 和 `FixedString` 列：非 `NULL` 值的总字节长度 (可据此推导出字符串的平均长度) ；
  * 对于 `Nullable` 和 `LowCardinality(Nullable)` 列：`NULL` 值的数量，优化器会用它在选择性估算时扣除 `NULL` 行。

    单个 `Basic` 统计信息可以同时填充其中多项——例如，在 `Nullable(UInt32)` 列上，它会同时跟踪数值最小/最大值以及空值计数。与 `MinMax` 相比，`Basic` 还适用于 `String` / `FixedString` 列，并且还可以声明在 `UUID` 或 `IPv6` 等类型的 `Nullable` 包装类型上，仅用于跟踪空值计数。

    语法：`basic`

* `MinMax`

  列的最小值和最大值，可用于估算数值列上范围过滤器的选择性。

  语法：`minmax`

* `TDigest`

:::warning
`tdigest` 类型的统计信息创建成本较高，并且可能会减慢数据摄取速度。
:::

[TDigest](https://github.com/tdunning/t-digest) 草图，可用于计算数值列的近似百分位数 (例如第 90 百分位数) 。

语法：`tdigest`

* `Uniq`

  [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) 草图，可用于估算一列中包含多少个不同值。

  语法：`uniq`

* `CountMin`

:::warning
`countmin` 类型的统计信息创建成本较高，并且可能会减慢数据摄取速度。
:::

[CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) 草图，可对列中每个值的出现频率提供近似计数。

语法：`countmin`

<div id="supported-data-types">
  ### 支持的数据类型
</div>

|          | (U)Int*, Float*, Decimal(*), Date*, 布尔值, Enum* | IPv4 | String 或 FixedString |
| -------- | ---------------------------------------------- | ---- | -------------------- |
| Basic    | ✔                                              | ✔    | ✔                    |
| CountMin | ✔                                              | ✔    | ✔                    |
| MinMax   | ✔                                              | ✔    | ✗                    |
| TDigest  | ✔                                              | ✗    | ✗                    |
| Uniq     | ✔                                              | ✔    | ✔                    |

以上所有项也都接受上述类型的 `Nullable` 和 `LowCardinality(Nullable)` 包装类型。`Basic` 还可以额外声明在诸如 `UUID` 或 `IPv6` 等类型的 `Nullable` 包装类型上，仅用于跟踪 NULL 计数。

<div id="supported-operations">
  ### 支持的操作
</div>

|          | 等值过滤器 (==) | 范围过滤器 (`>, >=, <, <=`) |
| -------- | ---------- | ---------------------- |
| Basic    | ✗          | ✔ (仅限数值列)              |
| CountMin | ✔          | ✗                      |
| MinMax   | ✗          | ✔ (仅限数值列)              |
| TDigest  | ✗          | ✔ (仅限数值列)              |
| Uniq     | ✔          | ✗                      |

对于 `String` / `FixedString` 列上的 `Basic`，该统计信息仅记录总的
非 NULL 字节长度 (用于估算平均字符串长度) 以及 NULL 计数；
范围过滤器和数据分区片段裁剪都不依赖它。

<div id="column-level-settings">
  ## 列级设置
</div>

某些 MergeTree 设置可以在列级别覆盖：

* `max_compress_block_size` — 压缩并写入表之前，未压缩数据块的最大大小。
* `min_compress_block_size` — 写入下一个标记时，触发压缩所需的未压缩数据块的最小大小。

示例：

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

可以使用 [ALTER MODIFY COLUMN](/zh/sql-reference/statements/alter/column.md) 修改或移除列级设置，例如：

* 从列声明中移除 `SETTINGS`：

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

* 修改设置：

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

* 重置一个或多个设置，同时还会移除该表 CREATE 查询中列表达式里的设置声明。

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```