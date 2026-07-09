---
description: '2 番目のクエリの結果を除いた、1 番目のクエリの結果に含まれる行だけを返す EXCEPT 句のドキュメント。'
sidebar_label: 'EXCEPT'
slug: /sql-reference/statements/select/except
title: 'EXCEPT 句'
keywords: ['EXCEPT', 'clause']
doc_type: 'reference'
---

> `EXCEPT` 句は、2 番目のクエリの結果を除いた、1 番目のクエリの結果に含まれる行だけを返します。

* 両方のクエリは、同じ順序で同じ数のカラムを持ち、対応するカラムのデータ型も同じである必要があります。
* `EXCEPT` の結果には重複した行が含まれる場合があります。これが望ましくない場合は、`EXCEPT DISTINCT` を使用してください。
* 括弧が指定されていない場合、複数の `EXCEPT` は左から右に評価されます。
* `EXCEPT` 演算子は `UNION` 句と同じ優先順位を持ち、`INTERSECT` 句よりも優先順位が低くなります。

<div id="syntax">
  ## 構文
</div>

```sql
SELECT column1 [, column2 ]
FROM table1
[WHERE condition]

EXCEPT

SELECT column1 [, column2 ]
FROM table2
[WHERE condition]
```

条件には、要件に応じて任意の式を指定できます。

さらに、次の構文を使用すると、BigQuery (Google Cloud) と同様に、`EXCEPT()` を使って同じテーブルの結果からカラムを除外できます。

```sql
SELECT column1 [, column2 ] EXCEPT (column3 [, column4]) 
FROM table1 
[WHERE condition]
```

<div id="examples">
  ## 例
</div>

このセクションでは、`EXCEPT` 句の使用例を紹介します。

<div id="filtering-numbers-using-the-except-clause">
  ### `EXCEPT` 句を使用して数値をフィルタリングする
</div>

以下は、1 から 10 までの数値のうち、3 から 8 までに *含まれない* 数値を返す簡単な例です:

```sql title="Query"
SELECT number
FROM numbers(1, 10)
EXCEPT
SELECT number
FROM numbers(3, 6)
```

```response title="Response"
┌─number─┐
│      1 │
│      2 │
│      9 │
│     10 │
└────────┘
```

<div id="excluding-specific-columns-using-except">
  ### `EXCEPT()` を使って特定のカラムを除外する
</div>

`EXCEPT()` を使うと、結果から特定のカラムをすばやく除外できます。たとえば、以下の例のように、テーブル内の一部のカラムを除くすべてのカラムを選択したい場合に便利です。

```sql title="Query"
SHOW COLUMNS IN system.settings

SELECT * EXCEPT (default, alias_for, readonly, description)
FROM system.settings
LIMIT 5
```

```response title="Response"
    ┌─field───────┬─type─────────────────────────────────────────────────────────────────────┬─null─┬─key─┬─default─┬─extra─┐
 1. │ alias_for   │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 2. │ changed     │ UInt8                                                                    │ NO   │     │ ᴺᵁᴸᴸ    │       │
 3. │ default     │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 4. │ description │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 5. │ is_obsolete │ UInt8                                                                    │ NO   │     │ ᴺᵁᴸᴸ    │       │
 6. │ max         │ Nullable(String)                                                         │ YES  │     │ ᴺᵁᴸᴸ    │       │
 7. │ min         │ Nullable(String)                                                         │ YES  │     │ ᴺᵁᴸᴸ    │       │
 8. │ name        │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
 9. │ readonly    │ UInt8                                                                    │ NO   │     │ ᴺᵁᴸᴸ    │       │
10. │ tier        │ Enum8('Production' = 0, 'Obsolete' = 4, 'Experimental' = 8, 'Beta' = 12) │ NO   │     │ ᴺᵁᴸᴸ    │       │
11. │ type        │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
12. │ value       │ String                                                                   │ NO   │     │ ᴺᵁᴸᴸ    │       │
    └─────────────┴──────────────────────────────────────────────────────────────────────────┴──────┴─────┴─────────┴───────┘

   ┌─name────────────────────┬─value──────┬─changed─┬─min──┬─max──┬─type────┬─is_obsolete─┬─tier───────┐
1. │ dialect                 │ clickhouse │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ Dialect │           0 │ Production │
2. │ min_compress_block_size │ 65536      │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
3. │ max_compress_block_size │ 1048576    │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
4. │ max_block_size          │ 65409      │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
5. │ max_insert_block_size   │ 1048449    │       0 │ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │ UInt64  │           0 │ Production │
   └─────────────────────────┴────────────┴─────────┴──────┴──────┴─────────┴─────────────┴────────────┘
```

<div id="using-except-and-intersect-with-cryptocurrency-data">
  ### 暗号通貨データで `EXCEPT` と `INTERSECT` を使う
</div>

`EXCEPT` と `INTERSECT` は、ブール値論理の違いはあるものの、同様の用途で使えることが多く、共通のカラム (または複数のカラム) を持つ 2 つのテーブルがある場合は、どちらも便利です。
たとえば、取引価格と出来高を含む暗号通貨の過去データが数百万行あるとします。

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

ここで、保有している暗号通貨の一覧と各コインの数量を含む `holdings` というテーブルがあるとします。

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
   ('DOGEFI', 10),
   ('Bitcoin Diamond', 5000);
```

`EXCEPT` を使うと、**「保有しているコインのうち、一度も $10 未満で取引されたことがないものはどれか？」** のような問いに答えられます。

```sql title="Query"
SELECT crypto_name FROM holdings
EXCEPT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
│ Bitcoin     │
└─────────────┘
```

これは、私たちが保有する4種類の暗号通貨のうち、Bitcoinだけが一度も10ドルを下回っていないことを意味します (この例で使用している限られたデータに基づく) 。

<div id="using-except-distinct">
  ### `EXCEPT DISTINCT` を使用する
</div>

前のクエリでは、結果に Bitcoin の保有が複数回含まれていました。`EXCEPT` に `DISTINCT` を追加すると、結果から重複した行を除去できます。

```sql title="Query"
SELECT crypto_name FROM holdings
EXCEPT DISTINCT
SELECT crypto_name FROM crypto_prices
WHERE price < 10;
```

```response title="Response"
┌─crypto_name─┐
│ Bitcoin     │
└─────────────┘
```

**関連項目**

* [UNION](/ja/sql-reference/statements/select/union)
* [INTERSECT](/ja/sql-reference/statements/select/intersect)