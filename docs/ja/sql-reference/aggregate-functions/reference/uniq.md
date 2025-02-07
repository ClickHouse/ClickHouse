---
slug: /ja/sql-reference/aggregate-functions/reference/uniq
sidebar_position: 204
---

# uniq

引数の異なる値の概算数を計算します。

``` sql
uniq(x[, ...])
```

**引数**

この関数は可変数のパラメータを受け取ります。パラメータは `Tuple`、`Array`、`Date`、`DateTime`、`String`、または数値型です。

**返される値**

- [UInt64](../../../sql-reference/data-types/int-uint.md) 型の数値。

**実装の詳細**

この関数は以下を行います：

- 集計中のすべてのパラメータのハッシュを計算し、それをもとに計算を行います。

- 適応的なサンプリングアルゴリズムを使用します。計算状態のため、最大65536の要素ハッシュ値のサンプルを使用します。このアルゴリズムは非常に正確で、CPU上で非常に効率的です。クエリに複数のこの関数が含まれている場合でも、`uniq` の使用は他の集計関数を使用するのとほぼ同じ速さです。

- 結果を決定論的に提供します（クエリ処理の順序に依存しません）。

この関数はほとんどすべてのシナリオでの使用を推奨します。

**関連項目**

- [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
- [uniqCombined64](../../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)
- [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
- [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
