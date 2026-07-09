---
description: 'SAMPLE 子句文档'
sidebar_label: 'SAMPLE'
slug: /sql-reference/statements/select/sample
title: 'SAMPLE 子句'
doc_type: 'reference'
---

`SAMPLE` 子句允许对 `SELECT` 查询进行近似查询处理。

启用数据采样后，查询不会在全部数据上执行，而只会在某一部分数据 (样本) 上执行。例如，如果你需要计算所有访问的统计信息，只需在全部访问数据的 1/10 上执行查询，再将结果乘以 10 即可。

近似查询处理在以下情况下会很有用：

* 当你对延迟有严格要求 (例如低于 100ms) ，但又无法为满足这些要求而投入额外硬件资源。
* 当原始数据本身并不精确，因此近似处理不会明显影响结果质量。
* 当业务本身接受近似结果 (例如出于成本效益考虑，或将精确结果提供给高级用户) 。

:::note
你只能对 [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) 家族中的表使用采样，并且只有在创建表时指定了采样表达式才可以 (参见 [MergeTree 引擎](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)) 。
:::

数据采样具有以下特性：

* 数据采样是一种确定性机制。同一个 `SELECT .. SAMPLE` 查询的结果始终相同。
* 采样在不同表之间能够保持一致。对于具有单一采样键的表，相同采样系数的样本总是会选取相同的数据子集。例如，按用户 ID 采样时，不同表中会选取对应于同一组可能用户 ID 子集的行。这意味着你可以在 [IN](../../../sql-reference/operators/in.md) 子句的子查询中使用样本，也可以使用 [JOIN](../../../sql-reference/statements/select/join.md) 子句连接样本。
* 采样可以减少从磁盘读取的数据量。请注意，必须正确指定采样键。更多信息，请参见 [创建 MergeTree 表](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)。

`SAMPLE` 子句支持以下语法：

| SAMPLE Clause Syntax | Description                                                                                                       |
| -------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `SAMPLE k`           | 其中 `k` 是 0 到 1 之间的数。查询会在 `k` 比例的数据上执行。例如，`SAMPLE 0.1` 表示在 10% 的数据上执行查询。[了解更多](#sample-k)                          |
| `SAMPLE n`           | 其中 `n` 是一个足够大的整数。查询会在至少 `n` 行的样本上执行 (但不会明显超过这个数量) 。例如，`SAMPLE 10000000` 表示在至少 10,000,000 行上执行查询。[了解更多](#sample-n) |
| `SAMPLE k OFFSET m`  | 其中 `k` 和 `m` 是 0 到 1 之间的数。查询会在占数据 `k` 比例的样本上执行。用于采样的数据会按 `m` 的比例进行偏移。[了解更多](#sample-k-offset-m)                   |

<div id="sample-k">
  ## SAMPLE K
</div>

这里的 `k` 是 0 到 1 之间的数值 (支持分数和小数两种表示法) 。例如，`SAMPLE 1/2` 或 `SAMPLE 0.5`。

在 `SAMPLE k` 子句中，会从占数据总量 `k` 比例的数据中抽取样本。示例如下：

```sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

在此示例中，查询是在 0.1 (10%) 的数据样本上执行的。聚合函数的值不会自动校正，因此要获得近似结果，需要手动将 `count()` 的值乘以 10。

<div id="sample-n">
  ## SAMPLE N
</div>

这里的 `n` 是一个足够大的整数。例如，`SAMPLE 10000000`。

在这种情况下，查询会基于至少 `n` 行的样本执行 (但不会比这个数量大很多) 。例如，`SAMPLE 10000000` 会在至少 10,000,000 行数据上运行查询。

由于数据读取的最小单位是一个粒度 (其大小由 `index_granularity` 设置决定) ，因此样本设置得明显大于粒度大小才有意义。

使用 `SAMPLE n` 子句时，你无法知道实际处理的数据占总体数据的百分比。因此，你也无法确定聚合函数应乘以的系数。请使用 `_sample_factor` 虚拟列来获取近似结果。

`_sample_factor` 列包含动态计算出的相对系数。在使用指定采样键[创建](../../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)表时，会自动创建该列。下面给出了 `_sample_factor` 列的使用示例。

下面以 `visits` 表为例，它包含站点访问统计信息。第一个示例展示了如何计算页面浏览量：

```sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

下面的示例展示了如何计算访问总次数：

```sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

下面的示例展示了如何计算平均会话耗时。请注意，计算平均值时不需要使用相对系数。

```sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

<div id="sample-k-offset-m">
  ## SAMPLE K OFFSET M
</div>

其中 `k` 和 `m` 都是介于 0 和 1 之间的数字。示例如下。

**示例 1**

```sql
SAMPLE 1/10
```

在此示例中，样本占全部数据的 1/10：

`[++------------]`

**示例 2**

```sql
SAMPLE 1/10 OFFSET 1/2
```

这里，从数据后半部分抽取 10% 作为样本。

`[------++------]`