---
description: 'وثائق دوال Geometry'
sidebar_label: 'Geometry'
slug: /sql-reference/functions/geo/geometry
title: 'دوال للتعامل مع Geometry'
doc_type: 'مرجع'
---

<div id="geometry">
  ## Geometry
</div>

تتيح لك دوال Geometry حساب المحيط والمساحة للأنواع الهندسية مثل POLYGON وLINESTRING وMULTIPOLYGON وMULTILINESTRING وRING وPOINT. تُستخدم هذه الأشكال الهندسية ضمن النوع Geometry. إذا كانت قيمة الإدخال `NULL`، فستُرجع جميع الدوال أدناه القيمة 0.

<div id="perimetercartesian">
  ## perimeterCartesian
</div>

يحسب محيط كائن Geometry المحدد في نظام الإحداثيات الديكارتي (المسطح).

**الصياغة**

```sql
perimeterCartesian(geom)
```

**المعاملات**

* `geom` — كائن من النوع Geometry. [Geometry](../../data-types/geo.md).

**القيم المُعادة**

* عدد — محيط الكائن بوحدات نظام الإحداثيات. [Float64](../../data-types/float.md).

**مثال**

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

<div id="areacartesian">
  ## areaCartesian
</div>

تحسب مساحة كائن Geometry المحدد في نظام الإحداثيات الديكارتي.

**الصيغة**

```sql
areaCartesian(geom)
```

**المعاملات**

* `geom` — كائن من النوع Geometry. [Geometry](../../data-types/geo.md).

**القيم المُعادة**

* عدد — مساحة الكائن بوحدات نظام الإحداثيات. [Float64](../../data-types/float.md).

**مثال**

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

<div id="perimeterspherical">
  ## perimeterSpherical
</div>

يحسب محيط كائن Geometry على سطح كرة.

**الصيغة**

```sql
perimeterSpherical(geom)
```

**الوسائط**

* `geom` — كائن من النوع Geometry. [Geometry](../../data-types/geo.md).

**القيم المُعادة**

* عدد — المحيط. [Float64](../../data-types/float.md).

**مثال**

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

<div id="areaspherical">
  ## areaSpherical
</div>

يحسب مساحة كائن Geometry على سطح كرة.

**الصيغة**

```sql
areaSpherical(geom)
```

**الوسائط**

* `geom` — Geometry. [Geometry](../../data-types/geo.md).

**القيم المُعادة**

* عدد — المساحة. [Float64](../../data-types/float.md).

**مثال**

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