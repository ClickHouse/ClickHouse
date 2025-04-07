---
slug: /ja/sql-reference/aggregate-functions/reference/avgweighted
sidebar_position: 113
---

# avgWeighted

[加重算術平均](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean)を計算します。

**構文**

``` sql
avgWeighted(x, weight)
```

**引数**

- `x` — 値。
- `weight` — 値の重み。

`x` と `weight` の両方が
[整数](../../../sql-reference/data-types/int-uint.md)または[浮動小数点](../../../sql-reference/data-types/float.md)
である必要がありますが、異なる型を持つことが可能です。

**返される値**

- すべての重みが0または指定された重みパラメータが空の場合は`NaN`。
- それ以外の場合は加重平均。

**戻り値の型**は常に[Float64](../../../sql-reference/data-types/float.md)です。

**例**

クエリ:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

結果:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

**例**

クエリ:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Float64', (4, 1), (1, 0), (10, 2))
```

結果:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

**例**

クエリ:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (0, 0), (1, 0), (10, 0))
```

結果:

``` text
┌─avgWeighted(x, weight)─┐
│                    nan │
└────────────────────────┘
```

**例**

クエリ:

``` sql
CREATE table test (t UInt8) ENGINE = Memory;
SELECT avgWeighted(t) FROM test
```

結果:

``` text
┌─avgWeighted(x, weight)─┐
│                    nan │
└────────────────────────┘
```
