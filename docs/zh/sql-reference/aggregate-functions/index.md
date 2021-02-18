---
toc_priority: 33
toc_title: 简介
---

# 聚合函数 {#aggregate-functions}

聚合函数在 [正常](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) 方式如预期的数据库专家。

ClickHouse还支持:

-   [参数聚合函数](parametric-functions.md#aggregate_functions_parametric)，它接受除列之外的其他参数。
-   [组合器](combinators.md#aggregate_functions_combinators)，这改变了聚合函数的行为。

## 空处理 {#null-processing}

在聚合过程中，所有 `NULL`s被跳过。

**例:**

考虑这个表:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

比方说，你需要在总的值 `y` 列:

``` sql
SELECT sum(y) FROM t_null_big
```

    ┌─sum(y)─┐
    │      7 │
    └────────┘

该 `sum` 函数解释 `NULL` 作为 `0`. 特别是，这意味着，如果函数接收输入的选择，其中所有的值 `NULL`，那么结果将是 `0`，不 `NULL`.

现在你可以使用 `groupArray` 函数从创建一个数组 `y` 列:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` 不包括 `NULL` 在生成的数组中。

[原始文章](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
