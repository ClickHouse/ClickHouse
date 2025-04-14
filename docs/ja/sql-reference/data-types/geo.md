---
slug: /ja/sql-reference/data-types/geo
sidebar_position: 54
sidebar_label: Geo
title: "Geometric"
---

ClickHouseは、地理的オブジェクト（位置、土地など）を表現するためのデータ型をサポートしています。

**関連リンク**
- [簡単な地理的特徴の表現方法](https://en.wikipedia.org/wiki/GeoJSON).

## Point

`Point` は、その X座標と Y座標で表され、[Tuple](tuple.md)([Float64](float.md), [Float64](float.md)) として格納されます。

**例**

クエリ:

```sql
CREATE TABLE geo_point (p Point) ENGINE = Memory();
INSERT INTO geo_point VALUES((10, 10));
SELECT p, toTypeName(p) FROM geo_point;
```
結果:

``` text
┌─p───────┬─toTypeName(p)─┐
│ (10,10) │ Point         │
└─────────┴───────────────┘
```

## Ring

`Ring` は穴のない単純な多角形で、ポイントの配列として保存されます: [Array](array.md)([Point](#point))。

**例**

クエリ:

```sql
CREATE TABLE geo_ring (r Ring) ENGINE = Memory();
INSERT INTO geo_ring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT r, toTypeName(r) FROM geo_ring;
```
結果:

``` text
┌─r─────────────────────────────┬─toTypeName(r)─┐
│ [(0,0),(10,0),(10,10),(0,10)] │ Ring          │
└───────────────────────────────┴───────────────┘
```

## LineString

`LineString` は線で、ポイントの配列として保存されます: [Array](array.md)([Point](#point))。

**例**

クエリ:

```sql
CREATE TABLE geo_linestring (l LineString) ENGINE = Memory();
INSERT INTO geo_linestring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT l, toTypeName(l) FROM geo_linestring;
```
結果:

``` text
┌─r─────────────────────────────┬─toTypeName(r)─┐
│ [(0,0),(10,0),(10,10),(0,10)] │ LineString    │
└───────────────────────────────┴───────────────┘
```

## MultiLineString

`MultiLineString` は複数のラインで、`LineString` の配列として保存されます: [Array](array.md)([LineString](#linestring))。

**例**

クエリ:

```sql
CREATE TABLE geo_multilinestring (l MultiLineString) ENGINE = Memory();
INSERT INTO geo_multilinestring VALUES([[(0, 0), (10, 0), (10, 10), (0, 10)], [(1, 1), (2, 2), (3, 3)]]);
SELECT l, toTypeName(l) FROM geo_multilinestring;
```
結果:

``` text
┌─l───────────────────────────────────────────────────┬─toTypeName(l)───┐
│ [[(0,0),(10,0),(10,10),(0,10)],[(1,1),(2,2),(3,3)]] │ MultiLineString │
└─────────────────────────────────────────────────────┴─────────────────┘
```

## Polygon

`Polygon` は穴を持つ多角形で、リングの配列として保存されます: [Array](array.md)([Ring](#ring))。外側の配列の最初の要素は多角形の外側の形状で、後続の要素はすべて穴です。

**例**

これは1つの穴を持つ多角形です:

```sql
CREATE TABLE geo_polygon (pg Polygon) ENGINE = Memory();
INSERT INTO geo_polygon VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]]);
SELECT pg, toTypeName(pg) FROM geo_polygon;
```

結果:

``` text
┌─pg────────────────────────────────────────────────────────────┬─toTypeName(pg)─┐
│ [[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]] │ Polygon        │
└───────────────────────────────────────────────────────────────┴────────────────┘
```

## MultiPolygon

`MultiPolygon` は複数の多角形から成り、ポリゴンの配列として保存されます: [Array](array.md)([Polygon](#polygon))。

**例**

このマルチポリゴンは、1つは穴のないポリゴン、もう1つは1つの穴を持つ2つの別々のポリゴンで構成されています:

```sql
CREATE TABLE geo_multipolygon (mpg MultiPolygon) ENGINE = Memory();
INSERT INTO geo_multipolygon VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]);
SELECT mpg, toTypeName(mpg) FROM geo_multipolygon;
```
結果:

``` text
┌─mpg─────────────────────────────────────────────────────────────────────────────────────────────┬─toTypeName(mpg)─┐
│ [[[(0,0),(10,0),(10,10),(0,10)]],[[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]]] │ MultiPolygon    │
└─────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────┘
```

## 関連コンテンツ

- [膨大な実世界のデータセットの探求: ClickHouseにおける100年以上の天気記録](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
