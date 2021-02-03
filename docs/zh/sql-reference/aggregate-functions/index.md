---
toc_folder_title: 聚合函数
toc_priority: 33
toc_title: 简介
---

# 聚合函数 {#aggregate-functions}

聚合函数如数据库专家预期的方式 [正常](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) 工作。

ClickHouse还支持:

-   [参数聚合函数](parametric-functions.md#aggregate_functions_parametric)，它接受除列之外的其他参数。
-   [组合器](combinators.md#aggregate_functions_combinators)，这改变了聚合函数的行为。

## 空处理 {#null-processing}

在聚合过程中，所有 `NULL` 被跳过。

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

比方说，你需要计算 `y` 列的总数:

``` sql
SELECT sum(y) FROM t_null_big
```

    ┌─sum(y)─┐
    │      7 │
    └────────┘


现在你可以使用 `groupArray` 函数用 `y` 列创建一个数组:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

在 `groupArray` 生成的数组中不包括 `NULL`。

[原始文章](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
