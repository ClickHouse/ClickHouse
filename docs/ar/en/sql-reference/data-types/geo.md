---
description: 'توثيق لأنواع البيانات الهندسية في ClickHouse المستخدمة لتمثيل
  العناصر والمواقع الجغرافية'
sidebar_label: 'Geo'
sidebar_position: 54
slug: /sql-reference/data-types/geo
title: 'هندسية'
doc_type: 'reference'
---

يدعم ClickHouse أنواع بيانات لتمثيل الكائنات الجغرافية — مثل المواقع والأراضي وغيرها.

**انظر أيضًا**

* [تمثيل المعالم الجغرافية البسيطة](https://en.wikipedia.org/wiki/GeoJSON).

<div id="point">
  ## Point
</div>

يُمثَّل `Point` بإحداثيي X وY، ويُخزَّن كـ [Tuple](tuple.md)([Float64](float.md), [Float64](float.md)).

**مثال**

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

`Ring` هو مضلع بسيط من دون ثقوب، ويُخزَّن كمصفوفة من النقاط: [Array](array.md)([Point](#point)).

**مثال**

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

`LineString` هو خط يُخزَّن كـ Array من النقاط: [Array](array.md)([Point](#point)).

**مثال**

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

يمثل `MultiLineString` عدة خطوط مخزَّنة ضمن مصفوفة من `LineString`: [Array](array.md)([LineString](#linestring)).

**مثال**

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

`Polygon` هو مضلع ذو ثقوب، يُخزَّن كمصفوفة من الحلقات: [Array](array.md)([Ring](#ring)). يمثّل العنصر الأول في المصفوفة الخارجية الحدّ الخارجي للمضلع، وتمثّل جميع العناصر التالية الثقوب.

**مثال**

هذا مضلع يحتوي على ثقب واحد:

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

يتكوّن `MultiPolygon` من عدة مضلعات، ويُخزَّن كمصفوفة من المضلعات: [Array](array.md)([Polygon](#polygon)).

**مثال**

يتكوّن هذا الـ `MultiPolygon` من مضلعين منفصلين — الأول بلا ثقوب، والثاني يحتوي على ثقب واحد:

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

`Geometry` هو نوع عام يشمل جميع الأنواع المذكورة أعلاه. وهو مكافئ لنوع Variant لهذه الأنواع.

**مثال**

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
  ## محتوى ذي صلة
</div>

* [استكشاف مجموعات بيانات واقعية ضخمة: أكثر من 100 عام من سجلات الطقس في ClickHouse](https://clickhouse.com/blog/real-world-data-noaa-climate-data)