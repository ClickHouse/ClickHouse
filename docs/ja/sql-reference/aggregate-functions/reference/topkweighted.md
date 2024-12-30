---
slug: /ja/sql-reference/aggregate-functions/reference/topkweighted
sidebar_position: 203
---

# topKWeighted

指定したカラム内で約最頻値を含む配列を返します。結果の配列は、値そのものではなく、約頻度に基づいて降順にソートされます。さらに、値の重みも考慮されます。

**構文**

``` sql
topKWeighted(N)(column, weight)
topKWeighted(N, load_factor)(column, weight)
topKWeighted(N, load_factor, 'counts')(column, weight)
```

**パラメータ**

- `N` — 返す要素の数。オプション。デフォルト値は10です。
- `load_factor` — 値のために予約されるセルの数を定義します。もし uniq(column) > N * load_factor なら、topK関数の結果は概算になります。オプション。デフォルト値は3です。
- `counts` — 結果に概算のカウントと誤差値を含めるかを定義します。

**引数**

- `column` — 値を表します。
- `weight` — 重みを表します。すべての値は、頻度計算のために`weight`回考慮されます。[UInt64](../../../sql-reference/data-types/int-uint.md).

**返される値**

最大の重みの概算合計を持つ値の配列を返します。

**例**

クエリ:

``` sql
SELECT topKWeighted(2)(k, w) FROM
VALUES('k Char, w UInt64', ('y', 1), ('y', 1), ('x', 5), ('y', 1), ('z', 10))
```

結果:

``` text
┌─topKWeighted(2)(k, w)──┐
│ ['z','x']              │
└────────────────────────┘
```

クエリ:

``` sql
SELECT topKWeighted(2, 10, 'counts')(k, w)
FROM VALUES('k Char, w UInt64', ('y', 1), ('y', 1), ('x', 5), ('y', 1), ('z', 10))
```

結果:

``` text
┌─topKWeighted(2, 10, 'counts')(k, w)─┐
│ [('z',10,0),('x',5,0)]              │
└─────────────────────────────────────┘
```

**参照**

- [topK](../../../sql-reference/aggregate-functions/reference/topk.md)
- [approx_top_k](../../../sql-reference/aggregate-functions/reference/approxtopk.md)
- [approx_top_sum](../../../sql-reference/aggregate-functions/reference/approxtopsum.md)
