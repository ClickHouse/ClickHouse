---
toc_title: WHERE
---

# WHERE {#select-where}

`WHERE` 子句允许过滤从 [FROM](../../../sql-reference/statements/select/from.md) 子句 `SELECT`.

如果有一个 `WHERE` 子句，它必须包含一个表达式与 `UInt8` 类型。 这通常是一个带有比较和逻辑运算符的表达式。 此表达式计算结果为0的行将从进一步的转换或结果中解释出来。

`WHERE` 如果基础表引擎支持，则根据使用索引和分区修剪的能力评估表达式。

!!! note "注"
    有一个叫做过滤优化 [prewhere](../../../sql-reference/statements/select/prewhere.md) 的东西.
