---
slug: /ja/sql-reference/functions/distance-functions
sidebar_position: 55
sidebar_label: 距離
---

# 距離関数

## L1Norm

ベクトルの絶対値の合計を計算します。

**構文**

```sql
L1Norm(vector)
```

別名: `normL1`.

**引数**

- `vector` — [Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。

**返される値**

- L1-ノルムまたは[タクシー幾何学](https://en.wikipedia.org/wiki/Taxicab_geometry)距離。[UInt](../data-types/int-uint.md)、[Float](../data-types/float.md)または[Decimal](../data-types/decimal.md)。

**例**

クエリ:

```sql
SELECT L1Norm((1, 2));
```

結果:

```text
┌─L1Norm((1, 2))─┐
│              3 │
└────────────────┘
```

## L2Norm

ベクトル値の平方和の平方根を計算します。

**構文**

```sql
L2Norm(vector)
```

別名: `normL2`.

**引数**

- `vector` — [Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。

**返される値**

- L2-ノルムまたは[ユークリッド距離](https://en.wikipedia.org/wiki/Euclidean_distance)。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT L2Norm((1, 2));
```

結果:

```text
┌───L2Norm((1, 2))─┐
│ 2.23606797749979 │
└──────────────────┘
```

## L2SquaredNorm

ベクトル値の平方和の平方根（[L2Norm](#l2norm)）の平方を計算します。

**構文**

```sql
L2SquaredNorm(vector)
```

別名: `normL2Squared`.

***引数**

- `vector` — [Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。

**返される値**

- L2-ノルムの平方。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT L2SquaredNorm((1, 2));
```

結果:

```text
┌─L2SquaredNorm((1, 2))─┐
│                     5 │
└───────────────────────┘
```

## LinfNorm

ベクトルの絶対値の最大値を計算します。

**構文**

```sql
LinfNorm(vector)
```

別名: `normLinf`.

**引数**

- `vector` — [Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。

**返される値**

- Linf-ノルムまたは絶対値の最大値。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT LinfNorm((1, -2));
```

結果:

```text
┌─LinfNorm((1, -2))─┐
│                 2 │
└───────────────────┘
```

## LpNorm

ベクトル内の絶対値の合計を `p` 乗したものの `p` 乗根を計算します。

**構文**

```sql
LpNorm(vector, p)
```

別名: `normLp`.

**引数**

- `vector` — [Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。
- `p` — 指数。可能な値: [1; inf) の範囲の実数。[UInt](../data-types/int-uint.md) または [Float](../data-types/float.md)。

**返される値**

- [Lp-ノルム](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT LpNorm((1, -2), 2);
```

結果:

```text
┌─LpNorm((1, -2), 2)─┐
│   2.23606797749979 │
└────────────────────┘
```

## L1Distance

2点間の距離を `L1` 空間（1-ノルム、[タクシー幾何学](https://en.wikipedia.org/wiki/Taxicab_geometry)距離）で計算します。

**構文**

```sql
L1Distance(vector1, vector2)
```

別名: `distanceL1`.

**引数**

- `vector1` — 第1ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。
- `vector2` — 第2ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。

**返される値**

- 1-ノルム距離。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT L1Distance((1, 2), (2, 3));
```

結果:

```text
┌─L1Distance((1, 2), (2, 3))─┐
│                          2 │
└────────────────────────────┘
```

## L2Distance

2点間の距離をユークリッド空間（[ユークリッド距離](https://en.wikipedia.org/wiki/Euclidean_distance)）で計算します。

**構文**

```sql
L2Distance(vector1, vector2)
```

別名: `distanceL2`.

**引数**

- `vector1` — 第1ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。
- `vector2` — 第2ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。

**返される値**

- 2-ノルム距離。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT L2Distance((1, 2), (2, 3));
```

結果:

```text
┌─L2Distance((1, 2), (2, 3))─┐
│         1.4142135623730951 │
└────────────────────────────┘
```

## L2SquaredDistance

2つのベクトルの対応する要素間の差の平方和を計算します。

**構文**

```sql
L2SquaredDistance(vector1, vector2)
```

別名: `distanceL2Squared`.

**引数**

- `vector1` — 第1ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。
- `vector2` — 第2ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。

**返される値**

- 2つのベクトルの対応する要素間の差の平方和。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT L2SquaredDistance([1, 2, 3], [0, 0, 0])
```

結果:

```response
┌─L2SquaredDistance([1, 2, 3], [0, 0, 0])─┐
│                                      14 │
└─────────────────────────────────────────┘
```

## LinfDistance

2点間の距離を `L_{inf}` 空間（[最大ノルム](https://en.wikipedia.org/wiki/Norm_(mathematics)#Maximum_norm_(special_case_of:_infinity_norm,_uniform_norm,_or_supremum_norm)））で計算します。

**構文**

```sql
LinfDistance(vector1, vector2)
```

別名: `distanceLinf`.

**引数**

- `vector1` — 第1ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。
- `vector2` — 第2ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。

**返される値**

- 無限ノルム距離。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT LinfDistance((1, 2), (2, 3));
```

結果:

```text
┌─LinfDistance((1, 2), (2, 3))─┐
│                            1 │
└──────────────────────────────┘
```

## LpDistance

2点間の距離を `Lp` 空間（[p-ノルム距離](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)）で計算します。

**構文**

```sql
LpDistance(vector1, vector2, p)
```

別名: `distanceLp`.

**引数**

- `vector1` — 第1ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。
- `vector2` — 第2ベクトル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。
- `p` — 指数。可能な値: [1;inf) の範囲の任意の数。[UInt](../data-types/int-uint.md) または [Float](../data-types/float.md)。

**返される値**

- p-ノルム距離。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT LpDistance((1, 2), (2, 3), 3);
```

結果:

```text
┌─LpDistance((1, 2), (2, 3), 3)─┐
│            1.2599210498948732 │
└───────────────────────────────┘
```

## L1Normalize

指定されたベクトルの単位ベクトルを `L1` 空間（[タクシー幾何学](https://en.wikipedia.org/wiki/Taxicab_geometry)）で計算します。

**構文**

```sql
L1Normalize(tuple)
```

別名: `normalizeL1`.

**引数**

- `tuple` — [Tuple](../data-types/tuple.md)。

**返される値**

- 単位ベクトル。[Float](../data-types/float.md)の[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT L1Normalize((1, 2));
```

結果:

```text
┌─L1Normalize((1, 2))─────────────────────┐
│ (0.3333333333333333,0.6666666666666666) │
└─────────────────────────────────────────┘
```

## L2Normalize

指定されたベクトルの単位ベクトルをユークリッド空間（[ユークリッド距離](https://en.wikipedia.org/wiki/Euclidean_distance)を使用）で計算します。

**構文**

```sql
L2Normalize(tuple)
```

別名: `normalizeL1`.

**引数**

- `tuple` — [Tuple](../data-types/tuple.md)。

**返される値**

- 単位ベクトル。[Float](../data-types/float.md)の[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT L2Normalize((3, 4));
```

結果:

```text
┌─L2Normalize((3, 4))─┐
│ (0.6,0.8)           │
└─────────────────────┘
```

## LinfNormalize

指定されたベクトルの単位ベクトルを `L_{inf}` 空間（[最大ノルム](https://en.wikipedia.org/wiki/Norm_(mathematics)#Maximum_norm_(special_case_of:_infinity_norm,_uniform_norm,_or_supremum_norm)を使用）で計算します。

**構文**

```sql
LinfNormalize(tuple)
```

別名: `normalizeLinf `.

**引数**

- `tuple` — [Tuple](../data-types/tuple.md)。

**返される値**

- 単位ベクトル。[Float](../data-types/float.md)の[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT LinfNormalize((3, 4));
```

結果:

```text
┌─LinfNormalize((3, 4))─┐
│ (0.75,1)              │
└───────────────────────┘
```

## LpNormalize

指定されたベクトルの単位ベクトルを `Lp` 空間（[p-ノルム](https://en.wikipedia.org/wiki/Norm_(mathematics)#p-norm)を使用）で計算します。

**構文**

```sql
LpNormalize(tuple, p)
```

別名: `normalizeLp `.

**引数**

- `tuple` — [Tuple](../data-types/tuple.md)。
- `p` — 指数。可能な値: [1;inf) の範囲の任意の数。[UInt](../data-types/int-uint.md) または [Float](../data-types/float.md)。

**返される値**

- 単位ベクトル。[Float](../data-types/float.md)の[Tuple](../data-types/tuple.md)。

**例**

クエリ:

```sql
SELECT LpNormalize((3, 4),5);
```

結果:

```text
┌─LpNormalize((3, 4), 5)──────────────────┐
│ (0.7187302630182624,0.9583070173576831) │
└─────────────────────────────────────────┘
```

## cosineDistance

2つのベクトル間のコサイン距離を計算します（タプルの値は座標です）。返される値が小さいほど、ベクトルは類似しています。

**構文**

```sql
cosineDistance(vector1, vector2)
```

**引数**

- `vector1` — 第1タプル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。
- `vector2` — 第2タプル。[Tuple](../data-types/tuple.md) または [Array](../data-types/array.md)。

**返される値**

- 2つのベクトル間の角度の余弦を1から減じたもの。[Float](../data-types/float.md)。

**例**

クエリ:

```sql
SELECT cosineDistance((1, 2), (2, 3));
```

結果:

```text
┌─cosineDistance((1, 2), (2, 3))─┐
│           0.007722123286332261 │
└────────────────────────────────┘
```

