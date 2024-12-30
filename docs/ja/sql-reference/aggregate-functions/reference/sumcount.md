---
slug: /ja/sql-reference/aggregate-functions/reference/sumcount
sidebar_position: 196
title: sumCount
---

数値の合計を計算し、同時に行数をカウントします。この関数はClickHouseのクエリオプティマイザによって使用されます。クエリ内に複数の`sum`、`count`、または`avg`関数がある場合、それらを単一の`sumCount`関数に置き換えて計算を再利用することができます。この関数を明示的に使用する必要はほとんどありません。

**構文**

``` sql
sumCount(x)
```

**引数**

- `x` — 入力値。データ型は[Integer](../../../sql-reference/data-types/int-uint.md)、[Float](../../../sql-reference/data-types/float.md)、または[Decimal](../../../sql-reference/data-types/decimal.md)でなければなりません。

**戻り値**

- タプル `(sum, count)`。ここで`sum`は数値の合計であり、`count`はNULLでない値を持つ行の数です。

型: [Tuple](../../../sql-reference/data-types/tuple.md)。

**例**

クエリ:

``` sql
CREATE TABLE s_table (x Int8) Engine = Log;
INSERT INTO s_table SELECT number FROM numbers(0, 20);
INSERT INTO s_table VALUES (NULL);
SELECT sumCount(x) from s_table;
```

結果:

``` text
┌─sumCount(x)─┐
│ (190,20)    │
└─────────────┘
```

**関連項目**

- [optimize_syntax_fuse_functions](../../../operations/settings/settings.md#optimize_syntax_fuse_functions) 設定。
