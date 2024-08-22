---
slug: /en/sql-reference/data-types/geo
sidebar_position: 54
sidebar_label: Geo
title: "Geometric"
---

ClickHouse supports data types for representing geographical objects — locations, lands, etc.

**See Also**
- [Representing simple geographical features](https://en.wikipedia.org/wiki/GeoJSON).

## Point

`Point` is represented by its X and Y coordinates, stored as a [Tuple](tuple.md)([Float64](float.md), [Float64](float.md)).

**Example**

Query:

```sql
CREATE TABLE geo_point (p Point) ENGINE = Memory();
INSERT INTO geo_point VALUES((10, 10));
SELECT p, toTypeName(p) FROM geo_point;
```
Result:

``` text
┌─p───────┬─toTypeName(p)─┐
│ (10,10) │ Point         │
└─────────┴───────────────┘
```

## Ring

`Ring` is a simple polygon without holes stored as an array of points: [Array](array.md)([Point](#point)).

**Example**

Query:

```sql
CREATE TABLE geo_ring (r Ring) ENGINE = Memory();
INSERT INTO geo_ring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT r, toTypeName(r) FROM geo_ring;
```
Result:

``` text
┌─r─────────────────────────────┬─toTypeName(r)─┐
│ [(0,0),(10,0),(10,10),(0,10)] │ Ring          │
└───────────────────────────────┴───────────────┘
```

## LineString

`LineString` is a line stored as an array of points: [Array](array.md)([Point](#point)).

**Example**

Query:

```sql
CREATE TABLE geo_linestring (l LineString) ENGINE = Memory();
INSERT INTO geo_linestring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT l, toTypeName(l) FROM geo_linestring;
```
Result:

``` text
┌─r─────────────────────────────┬─toTypeName(r)─┐
│ [(0,0),(10,0),(10,10),(0,10)] │ LineString    │
└───────────────────────────────┴───────────────┘
```

## MultiLineString

`MultiLineString` is multiple lines stored as an array of `LineString`: [Array](array.md)([LineString](#linestring)).

**Example**

Query:

```sql
CREATE TABLE geo_multilinestring (l MultiLineString) ENGINE = Memory();
INSERT INTO geo_multilinestring VALUES([[(0, 0), (10, 0), (10, 10), (0, 10)], [(1, 1), (2, 2), (3, 3)]]);
SELECT l, toTypeName(l) FROM geo_multilinestring;
```
Result:

``` text
┌─l───────────────────────────────────────────────────┬─toTypeName(l)───┐
│ [[(0,0),(10,0),(10,10),(0,10)],[(1,1),(2,2),(3,3)]] │ MultiLineString │
└─────────────────────────────────────────────────────┴─────────────────┘
```

## Polygon

`Polygon` is a polygon with holes stored as an array of rings: [Array](array.md)([Ring](#ring)). First element of outer array is the outer shape of polygon and all the following elements are holes.

**Example**

This is a polygon with one hole:

```sql
CREATE TABLE geo_polygon (pg Polygon) ENGINE = Memory();
INSERT INTO geo_polygon VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]]);
SELECT pg, toTypeName(pg) FROM geo_polygon;
```

Result:

``` text
┌─pg────────────────────────────────────────────────────────────┬─toTypeName(pg)─┐
│ [[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]] │ Polygon        │
└───────────────────────────────────────────────────────────────┴────────────────┘
```

## MultiPolygon

`MultiPolygon` consists of multiple polygons and is stored as an array of polygons: [Array](array.md)([Polygon](#polygon)).

**Example**

This multipolygon consists of two separate polygons — the first one without holes, and the second with one hole:

```sql
CREATE TABLE geo_multipolygon (mpg MultiPolygon) ENGINE = Memory();
INSERT INTO geo_multipolygon VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]);
SELECT mpg, toTypeName(mpg) FROM geo_multipolygon;
```
Result:

``` text
┌─mpg─────────────────────────────────────────────────────────────────────────────────────────────┬─toTypeName(mpg)─┐
│ [[[(0,0),(10,0),(10,10),(0,10)]],[[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]]] │ MultiPolygon    │
└─────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────┘
```

## Related Content

- [Exploring massive, real-world data sets: 100+ Years of Weather Records in ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)
