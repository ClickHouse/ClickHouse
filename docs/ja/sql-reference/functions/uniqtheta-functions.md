---
slug: /ja/sql-reference/functions/uniqtheta-functions
sidebar_position: 210
sidebar_label: uniqTheta
---

# uniqTheta 関数

uniqTheta 関数は、2つの uniqThetaSketch オブジェクトに対して集合操作計算（∪ / ∩ / × のような操作）を実行し、その結果を含む新しい uniqThetaSketch オブジェクトを返します。

uniqThetaSketch オブジェクトは、-State を伴う集約関数 uniqTheta によって構築されます。

UniqThetaSketch は近似値セットのデータ構造ストレージです。
RoaringBitmap の詳細については、[Theta Sketch Framework](https://datasketches.apache.org/docs/Theta/ThetaSketchFramework.html) を参照してください。

## uniqThetaUnion

2つの uniqThetaSketch オブジェクトに対して union 計算（集合操作 ∪）を実行し、その結果は新しい uniqThetaSketch です。

``` sql
uniqThetaUnion(uniqThetaSketch,uniqThetaSketch)
```

**引数**

- `uniqThetaSketch` – uniqThetaSketch オブジェクト。

**例**

``` sql
select finalizeAggregation(uniqThetaUnion(a, b)) as a_union_b, finalizeAggregation(a) as a_cardinality, finalizeAggregation(b) as b_cardinality
from
(select arrayReduce('uniqThetaState',[1,2]) as a, arrayReduce('uniqThetaState',[2,3,4]) as b );
```

``` text
┌─a_union_b─┬─a_cardinality─┬─b_cardinality─┐
│         4 │             2 │             3 │
└───────────┴───────────────┴───────────────┘
```

## uniqThetaIntersect

2つの uniqThetaSketch オブジェクトに対して intersect 計算（集合操作 ∩）を実行し、その結果は新しい uniqThetaSketch です。

``` sql
uniqThetaIntersect(uniqThetaSketch,uniqThetaSketch)
```

**引数**

- `uniqThetaSketch` – uniqThetaSketch オブジェクト。

**例**

``` sql
select finalizeAggregation(uniqThetaIntersect(a, b)) as a_intersect_b, finalizeAggregation(a) as a_cardinality, finalizeAggregation(b) as b_cardinality
from
(select arrayReduce('uniqThetaState',[1,2]) as a, arrayReduce('uniqThetaState',[2,3,4]) as b );
```

``` text
┌─a_intersect_b─┬─a_cardinality─┬─b_cardinality─┐
│             1 │             2 │             3 │
└───────────────┴───────────────┴───────────────┘
```

## uniqThetaNot

2つの uniqThetaSketch オブジェクトに対して a_not_b 計算（集合操作 ×）を実行し、その結果は新しい uniqThetaSketch です。

``` sql
uniqThetaNot(uniqThetaSketch,uniqThetaSketch)
```

**引数**

- `uniqThetaSketch` – uniqThetaSketch オブジェクト。

**例**

``` sql
select finalizeAggregation(uniqThetaNot(a, b)) as a_not_b, finalizeAggregation(a) as a_cardinality, finalizeAggregation(b) as b_cardinality
from
(select arrayReduce('uniqThetaState',[2,3,4]) as a, arrayReduce('uniqThetaState',[1,2]) as b );
```

``` text
┌─a_not_b─┬─a_cardinality─┬─b_cardinality─┐
│       2 │             3 │             2 │
└─────────┴───────────────┴───────────────┘
```

**参照**

- [uniqThetaSketch](../../sql-reference/aggregate-functions/reference/uniqthetasketch.md#agg_function-uniqthetasketch)
