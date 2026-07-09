---
description: 'Documentación sobre los tipos de datos geométricos de ClickHouse utilizados para representar
  objetos geográficos y ubicaciones'
sidebar_label: 'Geo'
sidebar_position: 54
slug: /sql-reference/data-types/geo
title: 'Geométricos'
doc_type: 'reference'
---

ClickHouse admite tipos de datos para representar objetos geográficos: ubicaciones, terrenos, etc.

**Véase también**

* [Representación de objetos geográficos simples](https://en.wikipedia.org/wiki/GeoJSON).

<div id="point">
  ## Point
</div>

`Point` se representa mediante sus coordenadas X e Y, almacenadas como un [Tuple](tuple.md)([Float64](float.md), [Float64](float.md)).

**Ejemplo**

```sql title="Query"
CREATE TABLE geo_point (p Point) ENGINE = Memory();
INSERT INTO geo_point VALUES((10, 10));
SELECT p, toTypeName(p) FROM geo_point;
```

```text title="Response"
┌─p───────┬─toTypeName(p)─┐
│ (10,10) │ Point         │
└─────────┴───────────────┘
```

<div id="ring">
  ## Ring
</div>

`Ring` es un polígono simple sin agujeros, almacenado como un array de puntos: [Array](array.md)([Point](#point)).

**Ejemplo**

```sql title="Query"
CREATE TABLE geo_ring (r Ring) ENGINE = Memory();
INSERT INTO geo_ring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT r, toTypeName(r) FROM geo_ring;
```

```text title="Response"
┌─r─────────────────────────────┬─toTypeName(r)─┐
│ [(0,0),(10,0),(10,10),(0,10)] │ Ring          │
└───────────────────────────────┴───────────────┘
```

<div id="linestring">
  ## LineString
</div>

`LineString` es una línea almacenada como un Array de puntos: [Array](array.md)([Point](#point)).

**Ejemplo**

```sql title="Query"
CREATE TABLE geo_linestring (l LineString) ENGINE = Memory();
INSERT INTO geo_linestring VALUES([(0, 0), (10, 0), (10, 10), (0, 10)]);
SELECT l, toTypeName(l) FROM geo_linestring;
```

```text title="Response"
┌─r─────────────────────────────┬─toTypeName(r)─┐
│ [(0,0),(10,0),(10,10),(0,10)] │ LineString    │
└───────────────────────────────┴───────────────┘
```

<div id="multilinestring">
  ## MultiLineString
</div>

`MultiLineString` consta de varias líneas almacenadas como un array de `LineString`: [Array](array.md)([LineString](#linestring)).

**Ejemplo**

```sql title="Query"
CREATE TABLE geo_multilinestring (l MultiLineString) ENGINE = Memory();
INSERT INTO geo_multilinestring VALUES([[(0, 0), (10, 0), (10, 10), (0, 10)], [(1, 1), (2, 2), (3, 3)]]);
SELECT l, toTypeName(l) FROM geo_multilinestring;
```

```text title="Response"
┌─l───────────────────────────────────────────────────┬─toTypeName(l)───┐
│ [[(0,0),(10,0),(10,10),(0,10)],[(1,1),(2,2),(3,3)]] │ MultiLineString │
└─────────────────────────────────────────────────────┴─────────────────┘
```

<div id="polygon">
  ## Polygon
</div>

`Polygon` es un polígono con agujeros almacenado como un array de anillos: [Array](array.md)([Ring](#ring)). El primer elemento del array exterior es la forma exterior del polígono y todos los elementos siguientes son agujeros.

**Ejemplo**

Este es un polígono con un agujero:

```sql title="Query"
CREATE TABLE geo_polygon (pg Polygon) ENGINE = Memory();
INSERT INTO geo_polygon VALUES([[(20, 20), (50, 20), (50, 50), (20, 50)], [(30, 30), (50, 50), (50, 30)]]);
SELECT pg, toTypeName(pg) FROM geo_polygon;
```

```text title="Response"
┌─pg────────────────────────────────────────────────────────────┬─toTypeName(pg)─┐
│ [[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]] │ Polygon        │
└───────────────────────────────────────────────────────────────┴────────────────┘
```

<div id="multipolygon">
  ## MultiPolygon
</div>

`MultiPolygon` consta de varios polígonos y se almacena como un array de polígonos: [Array](array.md)([Polygon](#polygon)).

**Ejemplo**

Este multipolígono consta de dos polígonos independientes: el primero sin agujeros y el segundo con un agujero:

```sql title="Query"
CREATE TABLE geo_multipolygon (mpg MultiPolygon) ENGINE = Memory();
INSERT INTO geo_multipolygon VALUES([[[(0, 0), (10, 0), (10, 10), (0, 10)]], [[(20, 20), (50, 20), (50, 50), (20, 50)],[(30, 30), (50, 50), (50, 30)]]]);
SELECT mpg, toTypeName(mpg) FROM geo_multipolygon;
```

```text title="Response"
┌─mpg─────────────────────────────────────────────────────────────────────────────────────────────┬─toTypeName(mpg)─┐
│ [[[(0,0),(10,0),(10,10),(0,10)]],[[(20,20),(50,20),(50,50),(20,50)],[(30,30),(50,50),(50,30)]]] │ MultiPolygon    │
└─────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────┘
```

<div id="geometry">
  ## Geometry
</div>

`Geometry` es un tipo común para todos los tipos mencionados anteriormente. Equivale a un `Variant` de esos tipos.

**Ejemplo**

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo (geom Geometry) ENGINE = Memory();
INSERT INTO geo VALUES ((1, 2));
SELECT * FROM geo;
```

```text title="Response"
   ┌─geom──┐
1. │ (1,2) │
   └───────┘
```

{/* */ }

```sql title="Query"
CREATE TABLE IF NOT EXISTS geo_dst (geom Geometry) ENGINE = Memory();

CREATE TABLE IF NOT EXISTS geo (geom String, id Int) ENGINE = Memory();
INSERT INTO geo VALUES ('POLYGON((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4))', 1);
INSERT INTO geo VALUES ('POINT(0 0)', 2);
INSERT INTO geo VALUES ('MULTIPOLYGON(((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4)),((-10 -10,-10 -9,-9 10,-10 -10)))', 3);
INSERT INTO geo VALUES ('LINESTRING(1 0,10 0,10 10,0 10,1 0)', 4);
INSERT INTO geo VALUES ('MULTILINESTRING((1 0,10 0,10 10,0 10,1 0),(4 4,5 4,5 5,4 5,4 4))', 5);
INSERT INTO geo_dst SELECT readWKT(geom) FROM geo ORDER BY id;

SELECT * FROM geo_dst;
```

```text title="Response"
   ┌─geom─────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
1. │ [[(1,0),(10,0),(10,10),(0,10),(1,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]]                                            │
2. │ (0,0)                                                                                                            │
3. │ [[[(1,0),(10,0),(10,10),(0,10),(1,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]],[[(-10,-10),(-10,-9),(-9,10),(-10,-10)]]] │
4. │ [(1,0),(10,0),(10,10),(0,10),(1,0)]                                                                              │
5. │ [[(1,0),(10,0),(10,10),(0,10),(1,0)],[(4,4),(5,4),(5,5),(4,5),(4,4)]]                                            │
   └──────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

<div id="related-content">
  ## Contenido relacionado
</div>

* [Explorando enormes conjuntos de datos del mundo real: más de 100 años de registros meteorológicos en ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)