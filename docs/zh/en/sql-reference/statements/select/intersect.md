---
description: 'INTERSECT 子句说明'
sidebar_label: 'INTERSECT'
slug: /sql-reference/statements/select/intersect
title: 'INTERSECT 子句'
doc_type: 'reference'
---

`INTERSECT` 子句仅返回同时出现在第一个和第二个查询结果中的行。两个查询的列数、顺序和类型必须一致。`INTERSECT` 的结果可以包含重复行。

如果未指定括号，多个 `INTERSECT` 语句将按从左到右的顺序执行。`INTERSECT` 运算符的优先级高于 `UNION` 和 `EXCEPT` 子句。

```sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

INTERSECT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```

该条件可根据你的需求使用任何表达式。

<div id="examples">
  ## 示例
</div>

下面是一个简单示例，对数字 1 到 10 与 3 到 8 求交集：

```sql title="Query"
SELECT number FROM numbers(1,10) INTERSECT SELECT number FROM numbers(3,8);
```

```response title="Response"
┌─number─┐
│      3 │
│      4 │
│      5 │
│      6 │
│      7 │
│      8 │
└────────┘
```

如果你有两个包含相同列 (或多列) 的表，`INTERSECT` 就非常有用。只要两个查询的结果包含相同的列，就可以对其结果求交集。例如，假设我们有几百万行历史加密货币数据，其中包含交易价格和交易量：

```sql title="Query"
CREATE TABLE crypto_prices
(
    trade_date Date,
    crypto_name String,
    volume Float32,
    price Float32,
    market_cap Float32,
    change_1_day Float32
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name, trade_date);

INSERT INTO crypto_prices
   SELECT *
   FROM s3(
    'https://learn-clickhouse.s3.us-east-2.amazonaws.com/crypto_prices.csv',
    'CSVWithNames'
);

SELECT * FROM crypto_prices
WHERE crypto_name = 'Bitcoin'
ORDER BY trade_date DESC
LIMIT 10;
```

```response title="Response"
┌─trade_date─┬─crypto_name─┬──────volume─┬────price─┬───market_cap─┬──change_1_day─┐
│ 2020-11-02 │ Bitcoin     │ 30771456000 │ 13550.49 │ 251119860000 │  -0.013585099 │
│ 2020-11-01 │ Bitcoin     │ 24453857000 │ 13737.11 │ 254569760000 │ -0.0031840964 │
│ 2020-10-31 │ Bitcoin     │ 30306464000 │ 13780.99 │ 255372070000 │   0.017308505 │
│ 2020-10-30 │ Bitcoin     │ 30581486000 │ 13546.52 │ 251018150000 │   0.008084608 │
│ 2020-10-29 │ Bitcoin     │ 56499500000 │ 13437.88 │ 248995320000 │   0.012552661 │
│ 2020-10-28 │ Bitcoin     │ 35867320000 │ 13271.29 │ 245899820000 │   -0.02804481 │
│ 2020-10-27 │ Bitcoin     │ 33749879000 │ 13654.22 │ 252985950000 │    0.04427984 │
│ 2020-10-26 │ Bitcoin     │ 29461459000 │ 13075.25 │ 242251000000 │  0.0033826586 │
│ 2020-10-25 │ Bitcoin     │ 24406921000 │ 13031.17 │ 241425220000 │ -0.0058658565 │
│ 2020-10-24 │ Bitcoin     │ 24542319000 │ 13108.06 │ 242839880000 │   0.013650347 │
└────────────┴─────────────┴─────────────┴──────────┴──────────────┴───────────────┘
```

现在假设我们有一个名为 `holdings` 的表，其中包含我们持有的加密货币列表以及对应的持币数量：

```sql title="Query"
CREATE TABLE holdings
(
    crypto_name String,
    quantity UInt64
)
ENGINE = MergeTree
PRIMARY KEY (crypto_name);

INSERT INTO holdings VALUES
   ('Bitcoin', 1000),
   ('Bitcoin', 200),
   ('Ethereum', 250),
   ('Ethereum', 5000),
   ('DOGEFI', 10);
   ('Bitcoin Diamond', 5000);
```

我们可以使用 `INTERSECT` 来回答这类问题：**&quot;我们持有哪些交易价格曾超过 100 美元的代币？&quot;**：

```sql title="Query"
SELECT crypto_name FROM holdings
INTERSECT
SELECT crypto_name FROM crypto_prices
WHERE price > 100
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Bitcoin     │
│ Ethereum    │
│ Ethereum    │
└─────────────┘
```

这意味着，在某个时点，比特币和以太坊的交易价格都曾高于 100 美元，而 DOGEFI 和 Bitcoin Diamond 的交易价格从未高于 100 美元 (至少根据本示例中的现有数据来看是这样) 。

<div id="intersect-distinct">
  ## INTERSECT DISTINCT
</div>

请注意，在前一个查询中，有多条比特币和以太坊持仓记录的交易价格高于 100 美元。去掉重复行会更合适一些 (因为它们只是重复了我们已经知道的信息) 。你可以为 `INTERSECT` 添加 `DISTINCT`，以从结果中消除重复行：

```sql title="Query"
SELECT crypto_name FROM holdings
INTERSECT DISTINCT
SELECT crypto_name FROM crypto_prices
WHERE price > 100;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Ethereum    │
└─────────────┘
```

**另请参阅**

* [UNION](/zh/sql-reference/statements/select/union)
* [EXCEPT](/zh/sql-reference/statements/select/except)