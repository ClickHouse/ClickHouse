---
description: 'Computes the geometric union of all polygons in a group, returning a single MultiPolygon.'
sidebar_label: 'groupPolygonUnion'
slug: /sql-reference/aggregate-functions/reference/grouppolygonunion
title: 'groupPolygonUnion'
doc_type: 'reference'
---

Computes the geometric union of all polygons in a group, producing a single `MultiPolygon` that covers the combined area of all input geometries.

**Syntax**

```sql
groupPolygonUnion(geometry)
```

**Arguments**

- `geometry` — A column of type [Ring](../../data-types/geo.md#ring), [Polygon](../../data-types/geo.md#polygon), or [MultiPolygon](../../data-types/geo.md#multipolygon).

**Returned value**

- A [MultiPolygon](../../data-types/geo.md#multipolygon) representing the union of all input geometries. [MultiPolygon](../../data-types/geo.md#multipolygon).

**Details**

- If no rows are aggregated, an empty `MultiPolygon` is returned.
- Input geometries of type `Ring` or `Polygon` are internally upcast to `MultiPolygon` before the union is computed.
- The function uses [Boost.Geometry](https://www.boost.org/doc/libs/release/libs/geometry/) to compute the geometric union.

**Example**

Compute the union of two overlapping squares of size length 10, with their bottom-left corners at (0,0) and (5,5):

```sql
CREATE TABLE test_polygons (geom Polygon) ENGINE = Memory;

INSERT INTO test_polygons VALUES (readWKTPolygon('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))'));
INSERT INTO test_polygons VALUES (readWKTPolygon('POLYGON((5 5, 15 5, 15 15, 5 15, 5 5))'));

SELECT wkt(groupPolygonUnion(geom)) AS result FROM test_polygons;
```

```response
┌─result─────────────────────────────────────────────────────────┐
│ MULTIPOLYGON(((5 10,5 15,15 15,15 5,10 5,10 0,0 0,0 10,5 10))) │
└────────────────────────────────────────────────────────────────┘
```