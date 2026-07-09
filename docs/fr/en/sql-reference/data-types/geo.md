---
description: 'Documentation des types de données géométriques dans ClickHouse, utilisés pour représenter
  des objets et des lieux géographiques'
sidebar_label: 'Geo'
sidebar_position: 54
slug: /sql-reference/data-types/geo
title: 'Géométriques'
doc_type: 'reference'
---

ClickHouse prend en charge des types de données permettant de représenter des objets géographiques — lieux, terrains, etc.

**Voir aussi**

* [Représentation d’entités géographiques simples](https://en.wikipedia.org/wiki/GeoJSON).

<div id="point">
  ## Point
</div>

`Point` est représenté par ses coordonnées X et Y, stockées dans un [Tuple](tuple.md)([Float64](float.md), [Float64](float.md)).

**Exemple**

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

`Ring` est un polygone simple sans trous, stocké sous forme de tableau de points : [Array](array.md)([Point](#point)).

**Exemple**

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

`LineString` est une ligne stockée sous forme d’un tableau de points : [Array](array.md)([Point](#point)).

**Exemple**

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

`MultiLineString` représente plusieurs lignes stockées sous forme d&#39;un tableau de `LineString` : [Array](array.md)([LineString](#linestring)).

**Exemple**

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

`Polygon` est un polygone avec des trous, stocké sous la forme d&#39;un tableau d&#39;anneaux : [Array](array.md)([Ring](#ring)). Le premier élément du tableau externe correspond au contour extérieur du polygone, et tous les éléments suivants sont des trous.

**Exemple**

Il s&#39;agit d&#39;un polygone avec un trou :

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

`MultiPolygon` se compose de plusieurs polygones et est représenté sous forme de tableau de polygones : [Array](array.md)([Polygon](#polygon)).

**Exemple**

Ce multipolygone se compose de deux polygones distincts — le premier sans trou, et le second avec un trou :

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

`Geometry` est un type commun qui regroupe tous les types ci-dessus. Il équivaut à un Variant de ces types.

**Exemple**

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
  ## Contenu connexe
</div>

* [Explorer des jeux de données massifs en conditions réelles : plus de 100 ans de relevés météorologiques dans ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)