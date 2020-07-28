---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: PREWHERE
---

# PREWHERE条款 {#prewhere-clause}

Prewhere是更有效地应用过滤的优化。 默认情况下，即使在 `PREWHERE` 子句未显式指定。 它的工作原理是自动移动的一部分 [WHERE](../../../sql-reference/statements/select/where.md) 条件到prewhere阶段。 的作用 `PREWHERE` 子句只是控制这个优化，如果你认为你知道如何做得比默认情况下更好。

使用prewhere优化，首先只读取执行prewhere表达式所需的列。 然后读取运行其余查询所需的其他列，但只读取prewhere表达式所在的那些块 “true” 至少对于一些行。 如果有很多块，其中prewhere表达式是 “false” 对于所有行和prewhere需要比查询的其他部分更少的列，这通常允许从磁盘读取更少的数据以执行查询。

## 手动控制Prewhere {#controlling-prewhere-manually}

该条款具有相同的含义 `WHERE` 条款 区别在于从表中读取数据。 当手动控制 `PREWHERE` 对于查询中的少数列使用的过滤条件，但这些过滤条件提供了强大的数据过滤。 这减少了要读取的数据量。

查询可以同时指定 `PREWHERE` 和 `WHERE`. 在这种情况下, `PREWHERE` 先于 `WHERE`.

如果 `optimize_move_to_prewhere` 设置为0，启发式自动移动部分表达式 `WHERE` 到 `PREWHERE` 被禁用。

## 限制 {#limitations}

`PREWHERE` 只有从表支持 `*MergeTree` 家人
