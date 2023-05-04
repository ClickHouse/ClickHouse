---
slug: /en/sql-reference/functions/geo/polygons
sidebar_label: Polygons
title: "Functions for Working with Polygons"
---

## readWKTMultiPolygon

Converts a WKT (Well Known Text) MultiPolygon into a MultiPolygon type.

### Example

``` sql
SELECT
    toTypeName(readWKTMultiPolygon('MULTIPOLYGON(((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))')) AS type,
    readWKTMultiPolygon('MULTIPOLYGON(((2 0,10 0,10 10,0 10,2 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))') AS output FORMAT Markdown

```
| type | output |
|:-|:-|
| MultiPolygon | [[[(2,0),(10,0),(10,10),(0,10),(2,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]],[[(-10,-10),(-10,-9),(-9,10),(-10,-10)]]] |


### Input parameters

String starting with `MULTIPOLYGON` 

### Returned value

MultiPolygon

## readWKTPolygon

Converts a WKT (Well Known Text) MultiPolygon into a Polygon type.

### Example

``` sql
SELECT
    toTypeName(readWKTPolygon('POLYGON((2 0,10 0,10 10,0 10,2 0))')) AS type,
    readWKTPolygon('POLYGON((2 0,10 0,10 10,0 10,2 0))') AS output
FORMAT Markdown
```
| type | output |
|:-|:-|
| Polygon | [[(2,0),(10,0),(10,10),(0,10),(2,0)]] |

### Input parameters

String starting with `POLYGON`

### Returned value

Polygon

## polygonsWithinSpherical

Returns true or false 

### Example

``` sql
select polygonsWithinSpherical([[[(4.3613577, 50.8651821), (4.349556, 50.8535879), (4.3602419, 50.8435626), (4.3830299, 50.8428851), (4.3904543, 50.8564867), (4.3613148, 50.8651279)]]], [[[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]]);
```
```response
0
```

### Input parameters

### Returned value

UInt8, 0 for false, 1 for true

## polygonsDistanceSpherical

Calculates 

### Example

``` sql
SELECT polygonsDistanceSpherical([[[(0, 0), (0, 0.1), (0.1, 0.1), (0.1, 0)]]], [[[(10., 10.), (10., 40.), (40., 40.), (40., 10.), (10., 10.)]]])
```
```response
0.24372872211133834
```

### Input parameters

Two polygons

### Returned value

Float64

## polygonsDistanceCartesian

Calculates distance between two polygons

### Example

``` sql
SELECT polygonsDistanceCartesian([[[(0, 0), (0, 0.1), (0.1, 0.1), (0.1, 0)]]], [[[(10., 10.), (10., 40.), (40., 40.), (40., 10.), (10., 10.)]]])
```
```response
14.000714267493642
```

### Input parameters

Two polygons

### Returned value

Float64

## polygonsEqualsCartesian

Returns true if two polygons are equal

### Example

``` sql
SELECT polygonsEqualsCartesian([[[(1., 1.), (1., 4.), (4., 4.), (4., 1.)]]], [[[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]]])
```
```response
1
```

### Input parameters

Two polygons

### Returned value

UInt8, 0 for false, 1 for true

## polygonsSymDifferenceSpherical

Calculates

### Example

``` sql
SELECT
```
```response
```

### Input parameters

### Returned value

## polygonsSymDifferenceCartesian

Calculates

### Example

``` sql
SELECT
```
```response
```

### Input parameters

### Returned value

## polygonsIntersectionSpherical

Calculates

### Example

``` sql
SELECT
```
```response
```

### Input parameters

### Returned value

## polygonsWithinCartesian

Returns true if the second polygon is within the first polygon.

### Example

``` sql
SELECT polygonsWithinCartesian([[[(2., 2.), (2., 3.), (3., 3.), (3., 2.)]]], [[[(1., 1.), (1., 4.), (4., 4.), (4., 1.), (1., 1.)]]])
```
```response
1
```

### Input parameters

Two polygons

### Returned value

UInt8, 0 for false, 1 for true

## polygonConvexHullCartesian

Calculates

### Example

``` sql
SELECT
```
```response
```

### Input parameters

### Returned value

## polygonAreaSpherical

Calculates

### Example

``` sql
SELECT
```
```response
```

### Input parameters

### Returned value

## polygonsUnionSpherical

Calculates

### Example

``` sql
SELECT
```
```response
```

### Input parameters

### Returned value

## polygonPerimeterSpherical

Calculates

### Example

``` sql
SELECT
```
```response
```

### Input parameters

### Returned value

## polygonsIntersectionCartesian

Calculates

### Example

``` sql
SELECT
```
```response
```

### Input parameters


### Returned value

## polygonAreaCartesian

Calculates the area of a polygon

### Example

``` sql
SELECT polygonAreaCartesian([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]])
```
```response
25
```

### Input parameters

One polygon

### Returned value

Float64

## polygonPerimeterCartesian

Calculates

### Example

``` sql
SELECT polygonPerimeterCartesian([[[(0., 0.), (0., 5.), (5., 5.), (5., 0.)]]])
```
```response
15
```

### Input parameters

One polygon

### Returned value

Float64

## polygonsUnionCartesian

Calculates

### Example

``` sql
SELECT
```
```response
```

### Input parameters

### Returned value

