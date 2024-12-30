---
slug: /ja/sql-reference/aggregate-functions/reference/approxtopsum
sidebar_position: 108
---

# approx_top_sum

指定されたカラム内で最も頻度の高い値とそのカウントを約算で返します。結果の配列は、値自体ではなく、その値の頻度の降順でソートされます。さらに、値の重みも考慮されます。

``` sql
approx_top_sum(N)(column, weight)
approx_top_sum(N, reserved)(column, weight)
```

この関数は保証された結果を提供しません。特定の状況では、エラーが発生する可能性があり、最も頻繁な値ではない値を返すことがあります。

`N < 10` の値を使用することを推奨します。`N` の値が大きいとパフォーマンスが低下します。`N` の最大値は 65536 です。

**パラメータ**

- `N` — 返す要素の数。オプション。デフォルト値: 10。
- `reserved` — 値のために予約されるセルの数を定義します。もし uniq(column) > reserved の場合、topK 関数の結果は概算になります。オプション。デフォルト値: N * 3。
 
**引数**

- `column` — 頻度を計算する値。
- `weight` — 重み。各値は頻度計算のために `weight` 回考慮されます。[UInt64](../../../sql-reference/data-types/int-uint.md)。

**例**

クエリ:

``` sql
SELECT approx_top_sum(2)(k, w)
FROM VALUES('k Char, w UInt64', ('y', 1), ('y', 1), ('x', 5), ('y', 1), ('z', 10))
```

結果:

``` text
┌─approx_top_sum(2)(k, w)─┐
│ [('z',10,0),('x',5,0)]  │
└─────────────────────────┘
```

**関連項目**

- [topK](../../../sql-reference/aggregate-functions/reference/topk.md)
- [topKWeighted](../../../sql-reference/aggregate-functions/reference/topkweighted.md)
- [approx_top_k](../../../sql-reference/aggregate-functions/reference/approxtopk.md)
