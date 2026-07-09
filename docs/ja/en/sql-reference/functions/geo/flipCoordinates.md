---
description: 'flipCoordinates のドキュメント'
sidebar_label: '座標の入れ替え'
sidebar_position: 63
slug: /sql-reference/functions/geo/flipCoordinates
title: '座標の入れ替え'
doc_type: 'reference'
---

<div id="flipcoordinates">
  ## flipCoordinates
</div>

`flipCoordinates` 関数は、`Point`、`Ring`、`Polygon`、または `Multipolygon` の座標を入れ替えます。これは、たとえば緯度と経度の順序が異なる座標系どうしで変換する際に便利です。

```sql
flipCoordinates(coordinates)
```

<div id="input-parameters">
  ### 入力パラメータ
</div>

* `coordinates` — Point を表すタプル `(x, y)`、または Ring、Polygon、Multipolygon を表す、そのようなタプルの配列です。サポートされる入力型は次のとおりです。
  * [**Point**](../../data-types/geo.md#point): `x` と `y` が [Float64](../../data-types/float.md) 値であるタプル `(x, y)`。
  * [**Ring**](../../data-types/geo.md#ring): Point の配列 `[(x1, y1), (x2, y2), ...]`。
  * [**Polygon**](../../data-types/geo.md#polygon): Ring の配列 `[ring1, ring2, ...]`。各 Ring は Point の配列です。
  * [**Multipolygon**](../../data-types/geo.md#multipolygon): Polygon の配列 `[polygon1, polygon2, ...]`。

<div id="returned-value">
  ### 戻り値
</div>

この関数は、入力された座標の順序を入れ替えた値を返します。たとえば、次のようになります。

* point `(x, y)` は `(y, x)` になります。
* ring `[(x1, y1), (x2, y2)]` は `[(y1, x1), (y2, x2)]` になります。
* polygon や multipolygon のようなネストされた構造は、再帰的に処理されます。

<div id="examples">
  ### 例
</div>

<div id="example-1">
  #### 例1: 1つのPointの座標を反転
</div>

```sql
SELECT flipCoordinates((10, 20)) AS flipped_point
```

```text
┌─flipped_point─┐
│ (20,10)       │
└───────────────┘
```

<div id="example-2">
  #### 例 2: Point の Array (Ring) の座標を入れ替える
</div>

```sql
SELECT flipCoordinates([(10, 20), (30, 40)]) AS flipped_ring
```

```text
┌─flipped_ring──────────────┐
│ [(20,10),(40,30)]         │
└───────────────────────────┘
```

<div id="example-3">
  #### 例 3: Polygonの座標順を反転
</div>

```sql
SELECT flipCoordinates([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]) AS flipped_polygon
```

```text
┌─flipped_polygon──────────────────────────────┐
│ [[(20,10),(40,30)],[(60,50),(80,70)]]        │
└──────────────────────────────────────────────┘
```

<div id="example-4">
  #### 例 4: Multipolygon の座標を反転する
</div>

```sql
SELECT flipCoordinates([[[10, 20], [30, 40]], [[50, 60], [70, 80]]]) AS flipped_multipolygon
```

```text
┌─flipped_multipolygon──────────────────────────────┐
│ [[[20,10],[40,30]],[[60,50],[80,70]]]             │
└───────────────────────────────────────────────────┘
```