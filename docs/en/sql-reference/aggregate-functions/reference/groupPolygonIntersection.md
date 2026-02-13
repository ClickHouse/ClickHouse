---
description: 'Computes the geometric intersection of all polygons in a group, returning a single MultiPolygon.'
sidebar_label: 'groupPolygonIntersection'
slug: /sql-reference/aggregate-functions/reference/grouppolygonintersection
title: 'groupPolygonIntersection'
doc_type: 'reference'
---

Computes the geometric intersection of all polygons in a group, producing a single `MultiPolygon` that covers the area shared by all input geometries.

**Syntax**

```sql
groupPolygonIntersection(geometry [, correct_geometry])
```

**Arguments**

- `geometry` — A column of type [Ring](../../data-types/geo.md#ring), [Polygon](../../data-types/geo.md#polygon), or [MultiPolygon](../../data-types/geo.md#multipolygon).
- `correct_geometry` — Optional. A [UInt8](../../data-types/int-uint.md) value that controls whether [`boost::geometry::correct`](https://www.boost.org/doc/libs/release/libs/geometry/doc/html/geometry/reference/algorithms/correct.html) is applied to input geometries (e.g. ensuring correct ring orientation and closure). `1` (default) enables correction, `0` disables it.

**Returned value**

- A [MultiPolygon](../../data-types/geo.md#multipolygon) representing the intersection of all input geometries. [MultiPolygon](../../data-types/geo.md#multipolygon).

**Details**

- If no rows are aggregated, an empty `MultiPolygon` is returned.
- If any row's geometry does not overlap with the accumulated intersection, the result becomes an empty `MultiPolygon`.
- Input geometries of type `Ring` or `Polygon` are internally upcast to `MultiPolygon` before the intersection is computed.
- The function uses [Boost.Geometry](https://www.boost.org/doc/libs/release/libs/geometry/) to compute the geometric intersection.

**Example**

Compute the intersection of two overlapping squares of size length 10, with their bottom-left corners at (0,0) and (5,5):

```sql
CREATE TABLE test_polygons (geom Polygon) ENGINE = Memory;

INSERT INTO test_polygons VALUES (readWKTPolygon('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));
INSERT INTO test_polygons VALUES (readWKTPolygon('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))'));

SELECT wkt(groupPolygonIntersection(geom)) AS result FROM test_polygons;
```

```response
┌─result─────────────────────────────────────┐
│ MULTIPOLYGON(((5 10,10 10,10 5,5 5,5 10))) │
└────────────────────────────────────────────┘
```
