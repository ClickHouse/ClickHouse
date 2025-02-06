---
slug: /ja/sql-reference/aggregate-functions/reference/theilsu
sidebar_position: 201
---

# theilsU

`theilsU` 関数は、[Theil's U 不確実性係数](https://en.wikipedia.org/wiki/Contingency_table#Uncertainty_coefficient)を計算します。この値は、テーブル内の2つのカラム間の関連性を測定します。この値は−1.0（100%の負の関連性、または完全な逆転）から+1.0（100%の正の関連性、または完全な一致）までの範囲です。0.0 の値は関連性がないことを示します。

**構文**

``` sql
theilsU(column1, column2)
```

**引数**

- `column1` と `column2` は比較するカラムです

**戻り値**

- -1 と 1 の間の値

**戻り値の型** は常に [Float64](../../../sql-reference/data-types/float.md) です。

**例**

以下の比較される2つのカラムはお互いに少しの関連性しかないため、`theilsU` の値は負になります：

``` sql
SELECT
    theilsU(a, b)
FROM
    (
        SELECT
            number % 10 AS a,
            number % 4 AS b
        FROM
            numbers(150)
    );
```

結果：

```response
┌────────theilsU(a, b)─┐
│ -0.30195720557678846 │
└──────────────────────┘
```
