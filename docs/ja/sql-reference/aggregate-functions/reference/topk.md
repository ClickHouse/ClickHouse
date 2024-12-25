---
slug: /ja/sql-reference/aggregate-functions/reference/topk
sidebar_position: 202
---

# topK

指定されたカラム内で最も頻繁に出現する値を近似的に配列として返します。生成された配列は、値自体ではなく、近似頻度の降順でソートされます。

TopKを分析するための[Filtered Space-Saving](https://doi.org/10.1016/j.ins.2010.08.024)アルゴリズムを実装しており、[Parallel Space Saving](https://doi.org/10.1016/j.ins.2015.09.003)のreduce-and-combineアルゴリズムに基づいています。

``` sql
topK(N)(column)
topK(N, load_factor)(column)
topK(N, load_factor, 'counts')(column)
```

この関数は結果を保証するものではありません。特定の状況では、エラーが発生し、最も頻繁に出現する値ではない値が返されることがあります。

`N < 10`の値を使用することをお勧めします。`N`の値が大きい場合、パフォーマンスが低下します。`N` の最大値は 65536 です。

**パラメータ**

- `N` — 返す要素の数。オプション。デフォルト値: 10。
- `load_factor` — 値のために予約されるセルの数を定義します。uniq(column) > N * load_factorの場合、topK関数の結果は近似値になります。オプション。デフォルト値: 3。
- `counts` — 結果に近似的なカウントとエラー値を含めるかどうかを定義します。

**引数**

- `column` — 頻度を計算するための値。

**例**

[OnTime](../../../getting-started/example-datasets/ontime.md) データセットを使用し、`AirlineID` カラムで最も頻繁に出現する3つの値を選択します。

``` sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```

``` text
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```

**関連項目**

- [topKWeighted](../../../sql-reference/aggregate-functions/reference/topkweighted.md)
- [approx_top_k](../../../sql-reference/aggregate-functions/reference/approxtopk.md)
- [approx_top_sum](../../../sql-reference/aggregate-functions/reference/approxtopsum.md)
