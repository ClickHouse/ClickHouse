---
slug: /en/sql-reference/statements/select/except
sidebar_label: EXCEPT
---

# EXCEPT Clause

The `EXCEPT` clause returns only those rows that result from the first query without the second. The queries must match the number of columns, order, and type. The result of `EXCEPT` can contain duplicate rows.

Multiple `EXCEPT` statements are executed left to right if parenthesis are not specified. The `EXCEPT` operator has the same priority as the `UNION` clause and lower priority than the `INTERSECT` clause.

``` sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

EXCEPT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```
The condition could be any expression based on your requirements.

## Examples

Here is a simple example that returns the numbers 1 to 10 that are _not_ a part of the numbers 3 to 8:

Query:

``` sql
SELECT number FROM numbers(1,10) EXCEPT SELECT number FROM numbers(3,6);
```

Result:

```response
┌─number─┐
│      1 │
│      2 │
│      9 │
│     10 │
└────────┘
```

`EXCEPT` and `INTERSECT` can often be used interchangeably with different Boolean logic, and they are both useful if you have two tables that share a common column (or columns). For example, suppose we have a few million rows of historical cryptocurrency data that contains trade prices and volume:

```sql
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

```response
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

Now suppose we have a table named `holdings` that contains a list of cryptocurrencies that we own, along with the number of coins:

```sql
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
   ('DOGEFI', 10),
   ('Bitcoin Diamond', 5000);
```

We can use `EXCEPT` to answer a question like **"Which coins do we own have never traded below $10?"**:

```sql
SELECT crypto_name FROM holdings
EXCEPT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

Result:

```response
┌─crypto_name─┐
│ Bitcoin     │
│ Bitcoin     │
└─────────────┘
```

This means of the four cryptocurrencies we own, only Bitcoin has never dropped below $10 (based on the limited data we have here in this example).

## EXCEPT DISTINCT

Notice in the previous query we had multiple Bitcoin holdings in the result. You can add `DISTINCT` to `EXCEPT` to eliminate duplicate rows from the result:

```sql
SELECT crypto_name FROM holdings
EXCEPT DISTINCT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

Result:

```response
┌─crypto_name─┐
│ Bitcoin     │
└─────────────┘
```


**See Also**

- [UNION](union.md#union-clause)
- [INTERSECT](intersect.md#intersect-clause)
