---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "\u6570\u5B57"
---

# 数字 {#numbers}

`numbers(N)` – Returns a table with the single ‘number’ 包含从0到N-1的整数的列(UInt64)。
`numbers(N, M)` -返回一个表与单 ‘number’ 包含从N到(N+M-1)的整数的列(UInt64)。

类似于 `system.numbers` 表，它可以用于测试和生成连续的值, `numbers(N, M)` 比 `system.numbers`.

以下查询是等效的:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
```

例:

``` sql
-- Generate a sequence of dates from 2010-01-01 to 2010-12-31
select toDate('2010-01-01') + number as d FROM numbers(365);
```

[原始文章](https://clickhouse.tech/docs/en/query_language/table_functions/numbers/) <!--hide-->
