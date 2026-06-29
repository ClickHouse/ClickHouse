---
description: 'Documentation for Geometry Functions'
sidebar_label: 'Geometry'
slug: /sql-reference/functions/geo/geometry
title: 'Functions for Working with Geometry'
doc_type: 'reference'
---

## Geometry {#geometry}

Geometry functions allow you to calculate perimeter and area for geometric types such as POLYGON, LINESTRING, MULTIPOLYGON, MULTILINESTRING, RING, and POINT. Use geometries in Geometry type. If the input value is `NULL`, all functions below will return 0.

## perimeterCartesian {#perimetercartesian}

Calculates the perimeter of the given Geometry object in the Cartesian (flat) coordinate system.

**Syntax**

```sql
perimeterCartesian(geom)
```

**Arguments**
- `geom` — Geometry object. [Geometry](../../data-types/geo.md).

**Returned values**
- Number — Perimeter of the object in the coordinate system units. [Float64](../../data-types/float.md).

**Example**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWKT('POLYGON((0 0,1 0,1 1,0 1,0 0))');
SELECT perimeterCartesian(geom) FROM geo_dst;
```

```response title="Response"
┌─perimeterCartesian(geom)─┐
│ 4.0                      │
└──────────────────────────┘
```

## areaCartesian {#areacartesian}

Calculates the area of the given Geometry object in the Cartesian coordinate system.

**Syntax**

```sql
areaCartesian(geom)
```

**Arguments**
- `geom` — Geometry object. [Geometry](../../data-types/geo.md).

**Returned values**
- Number — Area of the object in coordinate system units. [Float64](../../data-types/float.md).

**Example**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWKT('POLYGON((0 0,1 0,1 1,0 1,0 0))');
SELECT areaCartesian(geom) FROM geo_dst;
```

```response title="Response"
┌─areaCartesian(geom)─┐
│ -1                  │
└─────────────────────┘
```

## perimeterSpherical {#perimeterspherical}

Calculates the perimeter of a Geometry object on the surface of a sphere.

**Syntax**

```sql
perimeterSpherical(geom)
```

**Arguments**
- `geom` — Geometry object. [Geometry](../../data-types/geo.md).

**Returned values**
- Number — Perimeter. [Float64](../../data-types/float.md).

**Example**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWKT('LINESTRING(0 0,1 0,1 1,0 1,0 0)');
SELECT perimeterSpherical(geom) FROM geo_dst;
```

```response title="Response"
┌─perimeterSpherical(geom)─┐
│ 0                        │
└──────────────────────────┘
```

## areaSpherical {#areaspherical}

Calculates the area of a Geometry object on the surface of a sphere.

**Syntax**

```sql
areaSpherical(geom)
```

**Arguments**
- `geom` — Geometry. [Geometry](../../data-types/geo.md).

**Returned values**
- Number — Area. [Float64](../../data-types/float.md).

**Example**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWKT('POLYGON((0 0,1 0,1 1,0 1,0 0))');
SELECT areaSpherical(geom) FROM geo_dst;
```

```response title="Response"
┌─areaSpherical(geom)────┐
│ -0.0003046096848622019 │
└────────────────────────┘
```
