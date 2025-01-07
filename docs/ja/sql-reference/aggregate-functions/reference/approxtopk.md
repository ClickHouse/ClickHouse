---
slug: /ja/sql-reference/aggregate-functions/reference/approxtopk
sidebar_position: 107
---

# approx_top_k

指定されたカラムで最も頻度の高い値とそのカウントを近似的に配列で返します。結果の配列は、値自体ではなく、値の近似頻度の降順でソートされます。

``` sql
approx_top_k(N)(column)
approx_top_k(N, reserved)(column)
```

この関数は保証された結果を提供するものではありません。特定の状況では、エラーが発生し、最も頻度の高い値ではない頻繁な値を返すことがあります。

`N < 10` の値を使用することをお勧めします。`N` の値が大きいとパフォーマンスが低下します。`N` の最大値は 65536 です。

**パラメーター**

- `N` — 返す要素の数。オプション。デフォルト値: 10。
- `reserved` — 値のために予約されるセルの数を定義します。もし uniq(column) > reserved であれば、topK 関数の結果は近似的なものになります。オプション。デフォルト値: N * 3。
 
**引数**

- `column` — 頻度を計算する値。

**例**

クエリ:

``` sql
SELECT approx_top_k(2)(k)
FROM VALUES('k Char, w UInt64', ('y', 1), ('y', 1), ('x', 5), ('y', 1), ('z', 10));
```

結果:

``` text
┌─approx_top_k(2)(k)────┐
│ [('y',3,0),('x',1,0)] │
└───────────────────────┘
```

# approx_top_count

`approx_top_k` 関数の別名です

**関連項目**

- [topK](../../../sql-reference/aggregate-functions/reference/topk.md)
- [topKWeighted](../../../sql-reference/aggregate-functions/reference/topkweighted.md)
- [approx_top_sum](../../../sql-reference/aggregate-functions/reference/approxtopsum.md)
