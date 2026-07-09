---
description: 'INTERSECT 句のドキュメント'
sidebar_label: 'INTERSECT'
slug: /sql-reference/statements/select/intersect
title: 'INTERSECT 句'
doc_type: 'reference'
---

`INTERSECT` 句は、1 つ目と 2 つ目のクエリの両方に含まれる行のみを返します。各クエリは、カラム数、順序、型が一致している必要があります。`INTERSECT` の結果には、重複した行が含まれる場合があります。

かっこが指定されていない場合、複数の `INTERSECT` ステートメントは左から右の順に実行されます。`INTERSECT` 演算子は、`UNION` 句および `EXCEPT` 句よりも優先順位が高くなります。

```sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

INTERSECT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]

```

条件には、要件に応じた任意の式を指定できます。

<div id="examples">
  ## 例
</div>

以下は、1 から 10 までの数値と 3 から 8 までの数値の積集合を求める簡単な例です。

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

`INTERSECT` は、共通のカラム (または複数のカラム) を持つ 2 つのテーブルがある場合に便利です。結果に同じカラムが含まれていれば、2 つのクエリ結果の積集合を取ることができます。たとえば、取引価格と出来高を含む過去の暗号資産データが数百万行あるとします。

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

では、`holdings` という名前のテーブルがあり、そこに保有している暗号通貨の一覧とコイン数が含まれているとしましょう。

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

`INTERSECT` を使うと、**&quot;保有しているコインのうち、100ドルを超える価格で取引されたことがあるものはどれか？&quot;**のような質問に答えられます:

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

これは、Bitcoin と Ethereum はある時点で 100 ドルを超えて取引された一方、DOGEFI と Bitcoin Diamond は一度も 100 ドルを超えて取引されたことがないことを意味します (少なくとも、この例で使用しているデータの範囲では) 。

<div id="intersect-distinct">
  ## INTERSECT DISTINCT
</div>

前のクエリでは、$100 を超えて取引された Bitcoin と Ethereum の保有が複数ありました。重複した行は、すでにわかっている内容を繰り返しているだけなので、取り除くとよいでしょう。`INTERSECT` に `DISTINCT` を追加すると、結果から重複行を削除できます。

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

**関連項目**

* [UNION](/ja/sql-reference/statements/select/union)
* [EXCEPT](/ja/sql-reference/statements/select/except)