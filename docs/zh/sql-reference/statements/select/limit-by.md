---
slug: /zh/sql-reference/statements/select/limit-by
sidebar_label: LIMIT BY
---

# LIMIT BY子句 {#limit-by-clause}

一个使用`LIMIT n BY expressions`从句的查询会以去重后的`expressions`结果分组，每一分组选择前`n`行。`LIMIT BY`指定的值可以是任意数量的[表达式](/sql-reference/syntax#expressions)。

ClickHouse支持以下语法变体:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

处理查询时，ClickHouse首先选择经由排序键排序过后的数据。排序键可以显式地使用[ORDER BY](/sql-reference/statements/select/order-by)从句指定，或隐式地使用表引擎使用的排序键（数据的顺序仅在使用[ORDER BY](/sql-reference/statements/select/order-by)时才可以保证，否则由于多线程处理，数据顺序会随机化）。然后ClickHouse执行`LIMIT n BY expressions`从句，将每一行按 `expressions` 的值进行分组，并对每一分组返回前`n`行。如果指定了`OFFSET`，那么对于每一分组，ClickHouse会跳过前`offset_value`行，接着返回前`n`行。如果`offset_value`大于某一分组的行数，ClickHouse会从分组返回0行。

:::note
`LIMIT BY`与[LIMIT](../../../sql-reference/statements/select/limit.md)没有关系。它们可以在同一个查询中使用。
:::

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

与 `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` 返回相同的结果。

以下查询返回每个`domain,device_type`组合的前5个refferrer，总计返回至多100行(`LIMIT n BY + LIMIT`)。

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
