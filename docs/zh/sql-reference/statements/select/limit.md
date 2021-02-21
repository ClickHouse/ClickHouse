---
toc_title: LIMIT
---

# LIMIT {#limit-clause}

`LIMIT m` 允许选择结果中起始的 `m` 行。

`LIMIT n, m` 允许选择个 `m` 从跳过第一个结果后的行 `n` 行。 与 `LIMIT m OFFSET n` 语法是等效的。

`n` 和 `m` 必须是非负整数。

如果没有 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 子句显式排序结果，结果的行选择可能是任意的和非确定性的。

## LIMIT … WITH TIES 修饰符 {#limit-with-ties}

如果为 `LIMIT n[,m]` 设置了 `WITH TIES` ，并且声明了 `ORDER BY expr_list`, you will get in result first `n` or `n,m` rows and all rows with same `ORDER BY` fields values equal to row at position `n` for `LIMIT n` and `m` for `LIMIT n,m`.

此修饰符可以与： [ORDER BY … WITH FILL modifier](../../../sql-reference/statements/select/order-by.md#orderby-with-fill) 组合使用.

例如以下查询：

``` sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5
```

返回

``` text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
└───┘
```

单子执行了 `WITH TIES` 修饰符后

``` sql
SELECT * FROM (
    SELECT number%50 AS n FROM numbers(100)
) ORDER BY n LIMIT 0,5 WITH TIES
```

则返回了以下的数据行

``` text
┌─n─┐
│ 0 │
│ 0 │
│ 1 │
│ 1 │
│ 2 │
│ 2 │
└───┘
```

cause row number 6 have same value “2” for field `n` as row number 5
