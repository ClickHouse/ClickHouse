---
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
toc_title: LIMIT BY
---

# 限制条款 {#limit-by-clause}

与查询 `LIMIT n BY expressions` 子句选择第一个 `n` 每个不同值的行 `expressions`. 的关键 `LIMIT BY` 可以包含任意数量的 [表达式](../../../sql-reference/syntax.md#syntax-expressions).

ClickHouse支持以下语法变体:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

在查询处理过程中，ClickHouse会选择按排序键排序的数据。 排序键使用以下命令显式设置 [ORDER BY](../../../sql-reference/statements/select/order-by.md) 子句或隐式作为表引擎的属性。 然后ClickHouse应用 `LIMIT n BY expressions` 并返回第一 `n` 每个不同组合的行 `expressions`. 如果 `OFFSET` 被指定，则对于每个数据块属于一个不同的组合 `expressions`，ClickHouse跳过 `offset_value` 从块开始的行数，并返回最大值 `n` 行的结果。 如果 `offset_value` 如果数据块中的行数大于数据块中的行数，ClickHouse将从该块返回零行。

!!! note "注"
    `LIMIT BY` 是不相关的 [LIMIT](../../../sql-reference/statements/select/limit.md). 它们都可以在同一个查询中使用。

## 例 {#examples}

样品表:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by VALUES (1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

查询:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

该 `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` 查询返回相同的结果。

以下查询返回每个引用的前5个引用 `domain, device_type` 最多可与100行配对 (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```
