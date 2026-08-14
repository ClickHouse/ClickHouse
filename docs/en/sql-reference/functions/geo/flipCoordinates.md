---
description: 'Documentation for flipCoordinates'
sidebar_label: 'Flip Coordinates'
sidebar_position: 63
slug: /sql-reference/functions/geo/flipCoordinates
title: 'Flipping Coordinates'
doc_type: 'reference'
---

## flipCoordinates {#flipcoordinates}

The `flipCoordinates` function swaps the coordinates of a point, ring, polygon, or multipolygon. This is useful, for example, when converting between coordinate systems where the order of latitude and longitude differs.

```sql
flipCoordinates(coordinates)
```

### Input Parameters {#input-parameters}

- `coordinates` — A tuple representing a point `(x, y)`, or an array of such tuples representing a ring, polygon, or multipolygon. Supported input types include:
  - [**Point**](../../data-types/geo.md#point): A tuple `(x, y)` where `x` and `y` are [Float64](../../data-types/float.md) values.
  - [**Ring**](../../data-types/geo.md#ring): An array of points `[(x1, y1), (x2, y2), ...]`.
  - [**Polygon**](../../data-types/geo.md#polygon): An array of rings `[ring1, ring2, ...]`, where each ring is an array of points.
  - [**Multipolygon**](../../data-types/geo.md#multipolygon): An array of polygons `[polygon1, polygon2, ...]`.

### Returned Value {#returned-value}

The function returns the input with the coordinates flipped. For example:
- A point `(x, y)` becomes `(y, x)`.
- A ring `[(x1, y1), (x2, y2)]` becomes `[(y1, x1), (y2, x2)]`.
- Nested structures like polygons and multipolygons are processed recursively.

### Examples {#examples}

#### Example 1: Flipping a Single Point {#example-1}
```sql
SELECT flipCoordinates((10, 20)) AS flipped_point
```

```text
┌─flipped_point─┐
│ (20,10)       │
└───────────────┘
```

#### Example 2: Flipping an Array of Points (Ring) {#example-2}
```sql
SELECT flipCoordinates([(10, 20), (30, 40)]) AS flipped_ring
```

```text
┌─flipped_ring──────────────┐
│ [(20,10),(40,30)]         │
└───────────────────────────┘
```

#### Example 3: Flipping a Polygon {#example-3}
```sql
SELECT flipCoordinates([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]) AS flipped_polygon
```

```text
┌─flipped_polygon──────────────────────────────┐
│ [[(20,10),(40,30)],[(60,50),(80,70)]]        │
└──────────────────────────────────────────────┘
```

#### Example 4: Flipping a Multipolygon {#example-4}
```sql
SELECT flipCoordinates([[[10, 20], [30, 40]], [[50, 60], [70, 80]]]) AS flipped_multipolygon
```

```text
┌─flipped_multipolygon──────────────────────────────┐
│ [[[20,10],[40,30]],[[60,50],[80,70]]]             │
└───────────────────────────────────────────────────┘
```
