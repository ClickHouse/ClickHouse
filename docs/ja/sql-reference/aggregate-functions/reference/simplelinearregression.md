---
slug: /ja/sql-reference/aggregate-functions/reference/simplelinearregression
sidebar_position: 183
---

# simpleLinearRegression

シンプル（一次元）の線形回帰を実行します。

``` sql
simpleLinearRegression(x, y)
```

パラメータ:

- `x` — 説明変数の値を持つカラム。
- `y` — 従属変数の値を持つカラム。

返される値:

結果の直線 `y = k*x + b` の定数 `(k, b)`。

**例**

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])─┐
│ (1,0)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])─┐
│ (1,3)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

