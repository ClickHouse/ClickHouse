---
slug: /ja/sql-reference/aggregate-functions/reference/uniqhll12
sidebar_position: 208
---

# uniqHLL12

[HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) アルゴリズムを使用して、異なる引数値の概算数を計算します。

``` sql
uniqHLL12(x[, ...])
```

**引数**

この関数は可変数のパラメータを取ります。パラメータは `Tuple`、`Array`、`Date`、`DateTime`、`String`、または数値型である可能性があります。

**戻り値**

- [UInt64](../../../sql-reference/data-types/int-uint.md)型の数値。

**実装の詳細**

この関数は次のように実装されています：

- 集計内のすべてのパラメータに対しハッシュ計算を行い、それを用いて計算を実行します。

- HyperLogLog アルゴリズムを使用して、異なる引数値の数を概算します。

        2^12 の5ビットセルが使用されます。状態のサイズはわずかに2.5KBを超えます。小さなデータセット（<10K要素）の場合、結果はあまり正確ではありません（最大約10％の誤差）。しかし、高いカーディナリティを持つデータセット（10K-100M）の場合、結果は非常に正確で、最大誤差は約1.6％です。100M以上では、推定誤差が増加し、非常に高いカーディナリティを持つデータセット（1B+要素）に対しては非常に不正確な結果を返します。

- 決定的な結果を提供します（クエリ処理の順序に依存しない）。

この関数の使用はお勧めしません。ほとんどの場合、[uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) または [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined) 関数を使用してください。

**関連項目**

- [uniq](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
- [uniqCombined](../../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
- [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)
- [uniqTheta](../../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
