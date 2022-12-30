---
sidebar_label: PREWHERE
---

# PREWHERE 子句 {#prewhere-clause}

Prewhere是更有效地进行过滤的优化。 默认情况下，即使在 `PREWHERE` 子句未显式指定。 它也会自动移动 [WHERE](../../../sql-reference/statements/select/where.md) 条件到prewhere阶段。 `PREWHERE` 子句只是控制这个优化，如果你认为你知道如何做得比默认情况下更好才去控制它。

使用prewhere优化，首先只读取执行prewhere表达式所需的列。 然后读取运行其余查询所需的其他列，但只读取prewhere表达式所在的那些块 “true” 至少对于一些行。 如果有很多块，其中prewhere表达式是 “false” 对于所有行和prewhere需要比查询的其他部分更少的列，这通常允许从磁盘读取更少的数据以执行查询。

## 手动控制Prewhere {#controlling-prewhere-manually}

该子句具有与 `WHERE` 相同的含义，区别在于从表中读取数据。 当手动控制 `PREWHERE` 对于查询中的少数列使用的过滤条件，但这些过滤条件提供了强大的数据过滤。 这减少了要读取的数据量。

查询可以同时指定 `PREWHERE` 和 `WHERE`. 在这种情况下, `PREWHERE` 先于 `WHERE`.

如果 `optimize_move_to_prewhere` 设置为0，启发式自动移动部分表达式 `WHERE` 到 `PREWHERE` 被禁用。

## 限制 {#limitations}

`PREWHERE` 只有支持 `*MergeTree` 族系列引擎的表。
