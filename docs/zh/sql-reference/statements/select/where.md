---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: WHERE
---

# WHERE条款 {#select-where}

`WHERE` 子句允许过滤来自 [FROM](../../../sql-reference/statements/select/from.md) 的条款 `SELECT`.

如果有一个 `WHERE` 子句，它必须包含一个表达式与 `UInt8` 类型。 这通常是一个带有比较和逻辑运算符的表达式。 此表达式计算结果为0的行将从进一步的转换或结果中解释出来。

`WHERE` 如果基础表引擎支持，则根据使用索引和分区修剪的能力评估expression。

!!! note "注"
    有一个叫做过滤优化 [去哪里](../../../sql-reference/statements/select/prewhere.md).
