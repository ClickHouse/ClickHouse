---
description: 'Computes the convex hull of all geometries in a group, returning a Ring.'
sidebar_label: 'groupConvexHull'
slug: /sql-reference/aggregate-functions/reference/groupconvexhull
title: 'groupConvexHull'
doc_type: 'reference'
---

Computes the [convex hull](https://en.wikipedia.org/wiki/Convex_hull) of all geometries in a group, producing a `Ring` (closed polygon without holes) that is the smallest convex polygon containing all input geometry points.

**Syntax**

```sql
groupConvexHull(geometry [, correct_geometry])
```

**Arguments**

- `geometry` — A column of type [Point](../../data-types/geo.md#point), [Ring](../../data-types/geo.md#ring), [Polygon](../../data-types/geo.md#polygon), [MultiPolygon](../../data-types/geo.md#multipolygon), [LineString](../../data-types/geo.md#linestring), or [MultiLineString](../../data-types/geo.md#multilinestring).
- `correct_geometry` — Optional. A [UInt8](../../data-types/int-uint.md) value that controls whether [`boost::geometry::correct`](https://www.boost.org/doc/libs/release/libs/geometry/doc/html/geometry/reference/algorithms/correct.html) is applied to input geometries (e.g. ensuring correct ring orientation and closure). `1` (default) enables correction, `0` disables it.

**Returned value**

- A [Ring](../../data-types/geo.md#ring) representing the outer boundary of the convex hull of all input geometries. [Ring](../../data-types/geo.md#ring).

**Details**

- If no rows are aggregated, an empty `Ring` is returned.
- The convex hull is the smallest convex polygon that encloses all points from all input geometries.
- All input geometry types are internally converted to polygons before the hull is computed. Specifically:
    - `Point` values become degenerate single-vertex polygons.
    - `Ring` and `LineString` values become the outer ring of a polygon.
    - `Polygon` values contribute only their outer ring (holes are ignored, as they do not affect the convex hull).
    - `MultiPolygon` and `MultiLineString` values are decomposed into individual polygons.
- The function uses [Boost.Geometry](https://www.boost.org/doc/libs/release/libs/geometry/) to compute the convex hull.

**Example**

Compute the convex hull of a set of points forming a square with an interior point:

```sql
CREATE TABLE geo_points (p Point) ENGINE = Memory;

INSERT INTO geo_points VALUES ((0, 0)), ((10, 0)), ((10, 10)), ((0, 10)), ((5, 5));

SELECT wkt(groupConvexHull(p)) AS hull FROM geo_points;
```

```response
┌─hull───────────────────────────────┐
│ POLYGON((0 0,0 10,10 10,10 0,0 0)) │
└────────────────────────────────────┘
```

The interior point `(5, 5)` does not affect the convex hull because it is already contained within the square.

Compute convex hulls per group:

```sql
CREATE TABLE geo_grouped (grp Int32, p Point) ENGINE = Memory;

INSERT INTO geo_grouped VALUES
    (1, (0, 0)), (1, (10, 0)), (1, (10, 10)), (1, (0, 10)),
    (2, (100, 100)), (2, (110, 100)), (2, (110, 110)), (2, (100, 110));

SELECT grp, wkt(groupConvexHull(p)) AS hull FROM geo_grouped GROUP BY grp ORDER BY grp;
```

```response
┌─grp─┬─hull───────────────────────────────────────────────┐
│   1 │ POLYGON((0 0,0 10,10 10,10 0,0 0))                 │
│   2 │ POLYGON((100 100,100 110,110 110,110 100,100 100)) │
└─────┴────────────────────────────────────────────────────┘
```

Compute the convex hull from polygon inputs (holes are ignored):

```sql
CREATE TABLE geo_polygons (polygon Polygon) ENGINE = Memory;

INSERT INTO geo_polygons VALUES
    ([[(0, 0), (0, 10), (10, 10), (10, 0), (0, 0)], [(2, 2), (3, 2), (3, 3), (2, 3), (2, 2)]]);

SELECT wkt(groupConvexHull(polygon)) AS hull FROM geo_polygons;
```

```response
┌─hull───────────────────────────────┐
│ POLYGON((0 0,0 10,10 10,10 0,0 0)) │
└────────────────────────────────────┘
```

Compute the convex hull from line string inputs:

```sql
SELECT wkt(groupConvexHull(ls)) AS hull
FROM (
    SELECT readWKTLineString(s) AS ls
    FROM (
        SELECT arrayJoin([
            'LINESTRING (0 0, 10 0, 10 10)',
            'LINESTRING (0 10, 5 15, 10 10)'
        ]) AS s
    )
);
```

```response
┌─hull────────────────────────────────────┐
│ POLYGON((0 0,0 10,5 15,10 10,10 0,0 0)) │
└─────────────────────────────────────────┘
```
