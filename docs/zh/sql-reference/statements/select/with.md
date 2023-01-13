---
toc_title: WITH
---

# WITH子句 {#with-clause}

本节提供对公共表表达式的支持 ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)），所以结果 `WITH` 子句可以在其余部分中使用 `SELECT` 查询。

## 限制 {#limitations}

1.  不支持递归查询。
2.  当在section中使用子查询时，它的结果应该是只有一行的标量。
3.  Expression的结果在子查询中不可用。

## 例 {#examples}

**示例1:** 使用常量表达式作为 “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

**示例2:** 从SELECT子句列表中逐出sum(bytes)表达式结果

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

**例3:** 使用标量子查询的结果

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

**例4:** 在子查询中重用表达式

作为子查询中表达式使用的当前限制的解决方法，您可以复制它。

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```
