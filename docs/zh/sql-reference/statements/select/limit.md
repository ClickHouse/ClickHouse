---
sidebar_label: LIMIT
---

# LIMIT {#limit-clause}

`LIMIT m` 允许选择结果中起始的 `m` 行。

`LIMIT n, m` 允许选择个 `m` 从跳过第一个结果后的行 `n` 行。 与 `LIMIT m OFFSET n` 语法是等效的。

`n` 和 `m` 必须是非负整数。

如果没有 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 子句显式排序结果，结果的行选择可能是任意的和非确定性的。

## LIMIT … WITH TIES 修饰符 {#limit-with-ties}

如果为 `LIMIT n[,m]` 设置了 `WITH TIES` ，并且声明了 `ORDER BY expr_list`, 除了得到无修饰符的结果（正常情况下的 `limit n`, 前n行数据), 还会返回与第`n`行具有相同排序字段的行(即如果第n+1行的字段与第n行 拥有相同的排序字段，同样返回该结果.

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

添加 `WITH TIES` 修饰符后

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

虽然指定了`LIMIT 5`, 但第6行的`n`字段值为2，与第5行相同，因此也作为满足条件的记录返回。
简而言之，该修饰符可理解为是否增加“并列行”的数据。

``` sql，
``` sql
