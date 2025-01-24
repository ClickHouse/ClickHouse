---
slug: /ja/sql-reference/table-functions/generate_series
sidebar_position: 146
sidebar_label: generate_series
---

# generate_series

`generate_series(START, STOP)` - 開始から停止までの整数を含む単一の 'generate_series' カラム (UInt64) を持つテーブルを返します。

`generate_series(START, STOP, STEP)` - 開始から停止までの整数を含む単一の 'generate_series' カラム (UInt64) を持つテーブルを、指定されたステップ間隔で返します。

以下のクエリは、異なるカラム名を持ちながらも同じ内容のテーブルを返します。

``` sql
SELECT * FROM numbers(10, 5);
SELECT * FROM generate_series(10, 14);
```

また、以下のクエリも異なるカラム名で同じ内容のテーブルを返しますが、二番目のオプションの方が効率的です。

``` sql
SELECT * FROM numbers(10, 11) WHERE number % 3 == (10 % 3);
SELECT * FROM generate_series(10, 20, 3);
```
