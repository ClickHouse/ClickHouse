---
description: 'Documentation for Geometry Functions'
sidebar_label: 'Geometry'
slug: /sql-reference/functions/geo/geometry
title: 'Functions for Working with Geometry'
doc_type: 'reference'
---

## Geometry {#geometry}

Geometry functions allow you to calculate perimeter and area for geometric types such as POLYGON, LINESTRING, MULTIPOLYGON, MULTILINESTRING, and POINT. Use geometries in Geometry type.

## perimeterCartesian {#perimetercartesian}

Calculates the perimeter of the given Geometry object in the Cartesian (flat) coordinate system.

**Syntax**
perimeterCartesian(geom)

**Input values**
- `geom` — Geometry object (or WKT). Supported types: POLYGON, LINESTRING, MULTIPOLYGON, MULTILINESTRING. [Geometry](../../data-types/geometry.md).

**Returned values**
- Number — Perimeter of the object in the coordinate system units. [Float64](../../data-types/float.md).

**Example**
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWkt('POLYGON((0 0,1 0,1 1,0 1,0 0))');
SELECT perimeterCartesian(geom) FROM geo_dst;

Result:
┌─perimeterCartesian(geom)─┐
│ 4.0 │
└──────────────────────────┘

## areaCartesian {#areacartesian}

Calculates the area of the given Geometry object in the Cartesian coordinate system.

**Syntax**
areaCartesian(geom)

**Input values**
- `geom` — Geometry object (or WKT). Supported types: POLYGON, MULTIPOLYGON. [Geometry](../../data-types/geometry.md).

**Returned values**
- Number — Area of the object in coordinate system units. [Float64](../../data-types/float.md).

**Example**
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWkt('POLYGON((0 0,1 0,1 1,0 1,0 0))');
SELECT areaCartesian(geom) FROM geo_dst;

Result:
┌─areaCartesian(geom)─┐
│ -1 │
└─────────────────────┘

## perimeterSpherical {#perimeterspherical}

Calculates the perimeter of the given Geometry object on a sphere (Earth), using latitude and longitude coordinates.

**Syntax**
perimeterSpherical(geom)

**Input values**
- `geom` — Geometry object with latitude/longitude coordinates (degrees). [Geometry](../../data-types/geometry.md).

**Returned values**
- Number — Perimeter in meters (default sphere radius: 6372796.81 m). [Float64](../../data-types/float.md).

**Example**
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWkt('LINESTRING(0 0,1 0,1 1,0 1,0 0)');
SELECT perimeterSpherical(geom) FROM geo_dst;

Result:
┌─perimeterSpherical(geom)─┐
│ 0 │
└──────────────────────────┘

## areaSpherical {#areaspherical}

Calculates the area of a Geometry object on a sphere (Earth), using latitude and longitude coordinates.

**Syntax**
areaSpherical(geom)

**Input values**
- `geom` — Geometry object with latitude/longitude coordinates (degrees). Supported types: POLYGON, MULTIPOLYGON. [Geometry](../../data-types/geometry.md).

**Returned values**
- Number — Area in square meters (default sphere radius: 6372796.81 m). [Float64](../../data-types/float.md).

**Example**
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();
INSERT INTO geo_dst SELECT readWkt('POLYGON((0 0,1 0,1 1,0 1,0 0))');
SELECT areaSpherical(geom) FROM geo_dst;

Result:
┌─areaSpherical(geom)─┐
│ -0.0003046096848622019 │
└──────────────────────┘

### Notes

- If the geometry is invalid or empty, the function returns NULL or throws an error.
- Spherical functions use the default Earth radius: 6372796.81 m.
- All coordinates must be in consistent types (e.g., Float64 or Float32), otherwise conversion or an error may occur.
