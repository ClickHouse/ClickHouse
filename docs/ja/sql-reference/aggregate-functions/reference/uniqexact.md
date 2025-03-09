---
slug: /ja/sql-reference/aggregate-functions/reference/uniqexact
sidebar_position: 207
---

# uniqExact

異なる引数の値の正確な数を計算します。

``` sql
uniqExact(x[, ...])
```

絶対に正確な結果が必要な場合に `uniqExact` 関数を使用します。それ以外の場合は [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) 関数を使用してください。

`uniqExact` 関数は、`uniq` よりも多くのメモリを使用します。なぜなら、異なる値の数が増えるにつれて、状態のサイズが無制限に増大するためです。

**引数**

この関数は可変個のパラメータを取ります。パラメータは `Tuple`、`Array`、`Date`、`DateTime`、`String`、または数値型であることができます。

**関連項目**

- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniqcombined)
- [uniqHLL12](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniqhll12)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
