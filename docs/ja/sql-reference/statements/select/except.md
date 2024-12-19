---
slug: /ja/sql-reference/statements/select/except
sidebar_label: EXCEPT
---

# EXCEPT句

`EXCEPT`句は、最初のクエリの結果から2番目のクエリの結果を除いた行のみを返します。クエリは、カラムの数、順序、型が一致している必要があります。`EXCEPT`の結果には重複する行が含まれることがあります。

括弧が指定されていない場合、複数の`EXCEPT`文は左から右に実行されます。`EXCEPT`演算子は`UNION`句と同じ優先度を持ち、`INTERSECT`句より低い優先度を持っています。

``` sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

EXCEPT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```
条件は要件に基づく任意の式を指定できます。

## 例

ここでは、1から10までの数字で、3から8までの数字に_含まれない_ものを返す簡単な例を示します：

クエリ:

``` sql
SELECT number FROM numbers(1,10) EXCEPT SELECT number FROM numbers(3,6);
```

結果:

```response
┌─number─┐
│      1 │
│      2 │
│      9 │
│     10 │
└────────┘
```

`EXCEPT`と`INTERSECT`は異なるブール論理を用いてしばしば相互に使われ、共通のカラム(またはカラム)を持つ2つのテーブルがある場合に便利です。例えば、トレード価格とボリュームを含む数百万行の仮想通貨の歴史データがあるとします:

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

今、保有する仮想通貨のリストとコインの枚数を含む`holdings`というテーブルがあると仮定します:

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

**「$10を下回ったことがない持っているコインはどれですか？」**という質問に答えるために`EXCEPT`を使用できます:

```sql
SELECT crypto_name FROM holdings
EXCEPT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

結果:

```response
┌─crypto_name─┐
│ Bitcoin     │
│ Bitcoin     │
└─────────────┘
```

このことは、私たちが所有する四つの仮想通貨のうち、ここで示された限られたデータベースで、Bitcoinのみが$10以下に下がったことがないことを意味します。

## EXCEPT DISTINCT

前のクエリでは、結果に複数のBitcoin保有が含まれていました。`EXCEPT`に`DISTINCT`を追加して、結果から重複する行を取り除くことができます:

```sql
SELECT crypto_name FROM holdings
EXCEPT DISTINCT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

結果:

```response
┌─crypto_name─┐
│ Bitcoin     │
└─────────────┘
```


**関連項目**

- [UNION](union.md#union-clause)
- [INTERSECT](intersect.md#intersect-clause)
