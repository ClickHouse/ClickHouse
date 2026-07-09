---
description: 'OPTIMIZE 文档'
sidebar_label: 'OPTIMIZE'
sidebar_position: 47
slug: /sql-reference/statements/optimize
title: 'OPTIMIZE 语句'
doc_type: 'reference'
---

该查询会尝试触发一次针对表数据 parts 的计划外合并。请注意，我们通常不建议使用 `OPTIMIZE TABLE ... FINAL` (请参阅[相关文档](/zh/optimize/avoidoptimizefinal)) ，因为它的用例是管理场景，而非日常运维。

:::note
`OPTIMIZE` 无法修复 `Too many parts` 错误。
:::

**语法**

```sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL | FORCE] [DEDUPLICATE [BY expression]]
```

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

`OPTIMIZE` 查询支持 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 家族 (包括 [materialized view](/zh/sql-reference/statements/create/view#materialized-view)) 和 [Buffer](../../engines/table-engines/special/buffer.md) 引擎。其他表引擎不支持。

当 `OPTIMIZE` 与 [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) 家族的表引擎一起使用时，ClickHouse 会创建一个合并任务，并等待其在所有副本上执行 (如果 [alter&#95;sync](/zh/operations/settings/settings#alter_sync) 设置为 `2`) ，或者在当前副本上执行 (如果 [alter&#95;sync](/zh/operations/settings/settings#alter_sync) 设置为 `1`) 。

* 如果 `OPTIMIZE` 因任何原因未执行合并，它不会通知客户端。要启用通知，请使用 [optimize&#95;throw&#95;if&#95;noop](/zh/operations/settings/settings#optimize_throw_if_noop) 设置。
* 如果指定了 `PARTITION`，则只会优化指定的分区。[如何设置分区表达式](alter/partition.md#how-to-set-partition-expression)。
* 如果指定了 `FINAL` 或 `FORCE`，即使所有数据已经在一个 part 中，也会执行优化。你可以使用 [optimize&#95;skip&#95;merged&#95;partitions](/zh/operations/settings/settings#optimize_skip_merged_partitions) 控制此行为。此外，即使正在执行并发合并，也会强制执行合并。
* 如果指定了 `DEDUPLICATE`，则会对完全相同的行进行去重 (除非指定了 by-clause；否则会比较所有列) ，这仅对 MergeTree 引擎有意义。

你可以通过 [replication&#95;wait&#95;for&#95;inactive&#95;replica&#95;timeout](/zh/operations/settings/settings#replication_wait_for_inactive_replica_timeout) 设置，指定等待非活动副本执行 `OPTIMIZE` 查询的时长 (以秒为单位) 。

:::note
如果 `alter_sync` 设置为 `2`，并且某些副本处于非活动状态的时间超过 `replication_wait_for_inactive_replica_timeout` 设置指定的时长，则会抛出 `UNFINISHED` 异常。
:::

<div id="dry-run">
  ## DRY RUN
</div>

`DRY RUN` 子句会在不提交结果的情况下模拟合并指定的 parts。合并后的 part 会写入临时位置，经过验证后再丢弃。原始 parts 和表数据均保持不变。

这在以下场景中很有用：

* 测试不同 ClickHouse 版本之间合并的正确性。
* 以确定性的方式复现与合并相关的 bug。
* 对合并性能进行基准测试。

`DRY RUN` 仅支持 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 家族的表。必须使用 `PARTS` 关键字并提供 part 名称列表。所有指定的 parts 都必须存在、处于活动状态，并且属于同一分区。

`DRY RUN` 与 `FINAL` 和 `PARTITION` 不兼容。它可以与 `DEDUPLICATE` (可选指定列) 以及 `CLEANUP` (用于 `ReplacingMergeTree` 表) 结合使用。

**语法**

```sql
OPTIMIZE TABLE [db.]name DRY RUN PARTS 'part_name1', 'part_name2' [, ...] [DEDUPLICATE [BY expression]] [CLEANUP]
```

默认情况下，生成的合并后分片会以类似于 [`CHECK TABLE`](/zh/sql-reference/statements/check-table) 查询的方式进行校验。此行为由 [optimize&#95;dry&#95;run&#95;check&#95;part](/zh/operations/settings/settings#optimize_dry_run_check_part) 设置控制 (默认启用) 。禁用后将跳过校验，这在对合并操作本身进行基准测试时可能会很有用。

**示例**

```sql
CREATE TABLE dry_run_example (key UInt64, value String) ENGINE = MergeTree ORDER BY key;

INSERT INTO dry_run_example VALUES (1, 'a'), (2, 'b');
INSERT INTO dry_run_example VALUES (1, 'c'), (4, 'd');

-- Simulate merging using two parts
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0';

-- Simulate merging with deduplication
OPTIMIZE TABLE dry_run_example DRY RUN PARTS 'all_1_1_0', 'all_2_2_0' DEDUPLICATE;

-- Parts and data remain unchanged after DRY RUN
SELECT name, rows FROM system.parts
WHERE database = currentDatabase() AND table = 'dry_run_example' AND active
ORDER BY name;
```

```response
┌─name────────┬─rows─┐
│ all_1_1_0   │    2 │
│ all_2_2_0   │    2 │
└─────────────┴──────┘
```

<div id="by-expression">
  ## BY 表达式
</div>

如果你想基于自定义的一组列而非全部列执行去重，可以显式指定列列表，也可以使用 [`*`](../../sql-reference/statements/select/index.md#asterisk)、[`COLUMNS`](/zh/sql-reference/statements/select#select-clause) 或 [`EXCEPT`](/zh/sql-reference/statements/select/except-modifier) 表达式的任意组合。显式写出或隐式展开的列列表，必须包含行排序表达式 (包括主键和排序键) 以及分区表达式 (分区键) 中指定的所有列。

:::note
请注意，`*` 的行为与 `SELECT` 中完全一致：[MATERIALIZED](/zh/sql-reference/statements/create/view#materialized-view) 和 [ALIAS](../../sql-reference/statements/create/table.md#alias) 列不会参与展开。

此外，指定空列列表、写出结果为空列列表的表达式，或按 `ALIAS` 列去重，都会报错。
:::

**语法**

```sql
OPTIMIZE TABLE table DEDUPLICATE; -- all columns
OPTIMIZE TABLE table DEDUPLICATE BY *; -- excludes MATERIALIZED and ALIAS columns
OPTIMIZE TABLE table DEDUPLICATE BY colX,colY,colZ;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY * EXCEPT (colX, colY);
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex');
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT colX;
OPTIMIZE TABLE table DEDUPLICATE BY COLUMNS('column-matched-by-regex') EXCEPT (colX, colY);
```

**示例**

假设有下表：

```sql title="Query"
CREATE TABLE example (
    primary_key Int32,
    secondary_key Int32,
    value UInt32,
    partition_key UInt32,
    materialized_value UInt32 MATERIALIZED 12345,
    aliased_value UInt32 ALIAS 2,
    PRIMARY KEY primary_key
) ENGINE=MergeTree
PARTITION BY partition_key
ORDER BY (primary_key, secondary_key);
```

```sql title="Query"
INSERT INTO example (primary_key, secondary_key, value, partition_key)
VALUES (0, 0, 0, 0), (0, 0, 0, 0), (1, 1, 2, 2), (1, 1, 2, 3), (1, 1, 3, 3);
```

```sql title="Query"
SELECT * FROM example;
```

```sql title="Response"

┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

以下所有示例均基于这一包含 5 行数据的状态执行。

<div id="deduplicate">
  #### `DEDUPLICATE`
</div>

如果未指定去重所依据的列，则默认会考虑所有列。只有当所有列中的所有值都与前一行对应的值相等时，才会移除该行：

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-">
  #### `DEDUPLICATE BY *`
</div>

当未显式指定列时，表会按所有不是 `ALIAS` 或 `MATERIALIZED` 的列进行去重。对于上面的表，这些列包括 `primary_key`、`secondary_key`、`value` 和 `partition_key`：

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY *;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
│           1 │             1 │     3 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by--except">
  #### `DEDUPLICATE BY * EXCEPT`
</div>

按所有既不是 `ALIAS`、也不是 `MATERIALIZED`，并且明确排除 `value`、`primary_key`、`secondary_key` 和 `partition_key` 的列进行去重。

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY * EXCEPT value;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-list-of-columns">
  #### `DEDUPLICATE BY <list of columns>`
</div>

按 `primary_key`、`secondary_key` 和 `partition_key` 列显式去重：

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY primary_key, secondary_key, partition_key;
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```

<div id="deduplicate-by-columnsregex">
  #### `DEDUPLICATE BY COLUMNS(<regex>)`
</div>

对所有匹配该正则表达式的列进行去重：`primary_key`、`secondary_key` 和 `partition_key` 列：

```sql title="Query"
OPTIMIZE TABLE example FINAL DEDUPLICATE BY COLUMNS('.*_key');
```

```sql title="Query"
SELECT * FROM example;
```

```response title="Response"
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           0 │             0 │     0 │             0 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             2 │
└─────────────┴───────────────┴───────┴───────────────┘
┌─primary_key─┬─secondary_key─┬─value─┬─partition_key─┐
│           1 │             1 │     2 │             3 │
└─────────────┴───────────────┴───────┴───────────────┘
```