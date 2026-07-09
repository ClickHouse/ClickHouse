---
description: 'PREWHERE 子句文档'
sidebar_label: 'PREWHERE'
slug: /sql-reference/statements/select/prewhere
title: 'PREWHERE 子句'
doc_type: 'reference'
---

Prewhere 是一种用于更高效应用过滤条件的优化机制。即使未显式指定 `PREWHERE` 子句，它默认也处于启用状态。它的工作方式是自动将 [WHERE](../../../sql-reference/statements/select/where.md) 条件的一部分移到 prewhere 阶段。`PREWHERE` 子句的作用仅在于控制这种优化，也就是说，如果你认为自己能比默认行为更好地处理时，可以使用它。

启用 prewhere 优化后，系统首先只读取执行 prewhere 表达式所需的列。然后再读取执行查询其余部分所需的其他列，但只针对那些至少有部分行的 prewhere 表达式结果为 `true` 的块进行读取。如果存在大量块，其 prewhere 表达式对所有行都为 `false`，并且 prewhere 所需列数少于查询其他部分所需的列数，那么这种方式通常可以在查询执行时显著减少从磁盘读取的数据量。

<div id="controlling-prewhere-manually">
  ## 手动控制 PREWHERE
</div>

该子句与 `WHERE` 子句含义相同。区别在于从表中读取哪些数据。手动控制 `PREWHERE` 时，它适用于查询中仅少数列会用到、但能够显著过滤数据的过滤条件。这样可以减少需要读取的数据量。

一个查询可以同时指定 `PREWHERE` 和 `WHERE`。在这种情况下，`PREWHERE` 会先于 `WHERE` 执行。

如果 [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) 设置为 0，则会禁用自动将部分表达式从 `WHERE` 移动到 `PREWHERE` 的启发式规则。

如果查询带有 [FINAL](/zh/sql-reference/statements/select/from#final-modifier) modifier，则 `PREWHERE` 优化并不总是正确的。只有当 [optimize&#95;move&#95;to&#95;prewhere](../../../operations/settings/settings.md#optimize_move_to_prewhere) 和 [optimize&#95;move&#95;to&#95;prewhere&#95;if&#95;final](../../../operations/settings/settings.md#optimize_move_to_prewhere_if_final) 这两个设置都启用时，它才会生效。

:::note
`PREWHERE` 会在 `FINAL` 之前执行，因此对于 `FROM ... FINAL` 查询，如果 `PREWHERE` 使用了不在表 `ORDER BY` 部分中的字段，结果可能会出现偏差。
:::

<div id="limitations">
  ## 局限性
</div>

只有 [*MergeTree](../../../engines/table-engines/mergetree-family/index.md) 家族的表支持 `PREWHERE`。

<div id="example">
  ## 示例
</div>

```sql
CREATE TABLE mydata
(
    `A` Int64,
    `B` Int8,
    `C` String
)
ENGINE = MergeTree
ORDER BY A AS
SELECT
    number,
    0,
    if(number between 1000 and 2000, 'x', toString(number))
FROM numbers(10000000);

SELECT count()
FROM mydata
WHERE (B = 0) AND (C = 'x');

1 row in set. Elapsed: 0.074 sec. Processed 10.00 million rows, 168.89 MB (134.98 million rows/s., 2.28 GB/s.)

-- let's enable tracing to see which predicate are moved to PREWHERE
set send_logs_level='debug';

MergeTreeWhereOptimizer: condition "B = 0" moved to PREWHERE  
-- Clickhouse moves automatically `B = 0` to PREWHERE, but it has no sense because B is always 0.

-- Let's move other predicate `C = 'x'` 

SELECT count()
FROM mydata
PREWHERE C = 'x'
WHERE B = 0;

1 row in set. Elapsed: 0.069 sec. Processed 10.00 million rows, 158.89 MB (144.90 million rows/s., 2.30 GB/s.)

-- This query with manual `PREWHERE` processes slightly less data: 158.89 MB VS 168.89 MB
```