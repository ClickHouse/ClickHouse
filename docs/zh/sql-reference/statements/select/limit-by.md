---
sidebar_label: LIMIT BY
---

# LIMIT BY子句 {#limit-by-clause}

与查询 `LIMIT n BY expressions` 子句选择第一个 `n` 每个不同值的行 `expressions`.  `LIMIT BY` 可以包含任意数量的 [表达式](../../../sql-reference/syntax.md#syntax-expressions).

ClickHouse支持以下语法变体:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

在进行查询处理时，ClickHouse选择按排序键排序的数据。排序键设置显式地使用一个[ORDER BY](order-by.md#select-order-by)条款或隐式属性表的引擎(行顺序只是保证在使用[ORDER BY](order-by.md#select-order-by),否则不会命令行块由于多线程)。然后ClickHouse应用`LIMIT n BY 表达式`，并为每个不同的`表达式`组合返回前n行。如果指定了`OFFSET`，那么对于每个属于不同`表达式`组合的数据块，ClickHouse将跳过`offset_value`从块开始的行数，并最终返回最多`n`行的结果。如果`offset_value`大于数据块中的行数，则ClickHouse从数据块中返回零行。

!!! note "注"
    `LIMIT BY` 是不相关的 [LIMIT](../../../sql-reference/statements/select/limit.md). 它们都可以在同一个查询中使用。

## 例 {#examples}

样例表:

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
