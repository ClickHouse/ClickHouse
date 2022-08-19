---
toc_title: WHERE
---

# WHERE {#select-where}

`WHERE` 子句允许过滤来自`SELECT`的子句 [FROM](../../../sql-reference/statements/select/from.md) 的数据.

如果有一个 `WHERE` 子句，它必须包含一个表达式与 `UInt8` 类型。 这通常是一个带有比较和逻辑运算符的表达式。 表达式计算结果为0的行将被排除在在进一步的转换或结果之外。

如果基础表引擎支持，`WHERE`表达式会使用索引和分区进行剪枝。

!!! note "注"
    有一个叫做过滤优化 [prewhere](../../../sql-reference/statements/select/prewhere.md) 的东西.
    
如果需要测试一个 [NULL](../../../sql-reference/syntax.md#null-literal) 值，请使用 [IS NULL](../../operators/index.md#operator-is-null) and [IS NOT NULL](../../operators/index.md#is-not-null) 运算符或 [isNull](../../../sql-reference/functions/functions-for-nulls.md#isnull) 和 [isNotNull](../../../sql-reference/functions/functions-for-nulls.md#isnotnull) 函数。否则带有 NULL 的表达式永远不会通过。

**示例**

在 [numbers table](../../../sql-reference/table-functions/numbers.md) 表上执行下述语句以找到为3的倍数且大于10的数字：
To find numbers that are multiples of 3 and are greater than 10 execute the following query on the :

``` sql
SELECT number FROM numbers(20) WHERE (number > 10) AND (number % 3 == 0);
```

结果:

``` text
┌─number─┐
│     12 │
│     15 │
│     18 │
└────────┘
```

带有 `NULL` 值的查询:

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE=MergeTree() ORDER BY x;
INSERT INTO t_null VALUES (1, NULL), (2, 3);

SELECT * FROM t_null WHERE y IS NULL;
SELECT * FROM t_null WHERE y != 0;
```

结果:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
┌─x─┬─y─┐
│ 2 │ 3 │
└───┴───┘
```
