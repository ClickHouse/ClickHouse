---
toc_priority: 62
toc_title: Geo
---

# Geo Data Types {#geo-data-types}

ClickHouse supports data types for representing geographical objects — locations, lands, etc. 

!!! warning "Warning"
    Currently geo data types are an experimental feature. To work with them you must set `allow_experimental_geo_types = 1`.

**See Also**
- [Representing simple geographical features](https://en.wikipedia.org/wiki/GeoJSON).
- [allow_experimental_geo_types](../../operations/settings/settings.md#allow-experimental-geo-types) setting.

## Point {#point-data-type}

`Point` is represented by its X and Y coordinates, stored as a [Tuple](tuple.md)([Float64](float.md), [Float64](float.md)).

**Example**

Query:

```sql
SET allow_experimental_geo_types = 1;
CREATE TABLE geo_point (p Point) ENGINE = Memory();
INSERT INTO geo_point VALUES((10, 10));
SELECT p, toTypeName(p) FROM geo_point;
```
Result: 

``` text
┌─p─────┬─toTypeName(p)─┐
│ (10,10) │ Point         │
└───────┴───────────────┘
```

## Ring {#ring-data-type}

`Ring` is a simple polygon without holes stored as an array of points: [Array](array.md)([Point](#point-data-type)).

**Example**

Query:

```sql
SET allow_experimental_geo_types = 1;
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

## Polygon {#polygon-data-type}

`Polygon` is a polygon with holes stored as an array of rings: [Array](array.md)([Ring](#ring-data-type)). First element of outer array is the outer shape of polygon and all the following elements are holes.

**Example**

This is a polygon with one hole:

```sql
SET allow_experimental_geo_types = 1;
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

## MultiPolygon {#multipolygon-data-type}

`MultiPolygon` consists of multiple polygons and is stored as an array of polygons: [Array](array.md)([Polygon](#polygon-data-type)). 

**Example**

This multipolygon consists of two separate polygons — the first one without holes, and the second with one hole:

```sql
SET allow_experimental_geo_types = 1;
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

[Original article](https://clickhouse.tech/docs/en/data-types/geo/) <!--hide-->
