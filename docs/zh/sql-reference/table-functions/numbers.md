---
toc_priority: 39
toc_title: numbers
---

# numbers {#numbers}

`numbers(N)` – 返回一个包含单个 ‘number’ 列(UInt64)的表，其中包含从0到N-1的整数。
`numbers(N, M)` - 返回一个包含单个 ‘number’ 列(UInt64)的表，其中包含从N到(N+M-1)的整数。

类似于 `system.numbers` 表，它可以用于测试和生成连续的值, `numbers(N, M)` 比 `system.numbers`更有效。

以下查询是等价的:

``` sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM system.numbers LIMIT 10;
```

示例:

``` sql
-- 生成2010-01-01至2010-12-31的日期序列
select toDate('2010-01-01') + number as d FROM numbers(365);
```

[原始文章](https://clickhouse.tech/docs/en/query_language/table_functions/numbers/) <!--hide-->
