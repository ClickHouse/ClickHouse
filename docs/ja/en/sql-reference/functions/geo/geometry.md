---
description: 'Geometry 関数のドキュメント'
sidebar_label: 'Geometry'
slug: /sql-reference/functions/geo/geometry
title: 'Geometry を操作する関数'
doc_type: 'reference'
---

<div id="geometry">
  ## Geometry
</div>

Geometry 関数では、POLYGON、LINESTRING、MULTIPOLYGON、MULTILINESTRING、RING、POINT などのジオメトリ型の周長と面積を計算できます。ジオメトリには Geometry 型を使用します。入力値が `NULL` の場合、以下のすべての関数は 0 を返します。

<div id="perimetercartesian">
  ## perimeterCartesian
</div>

与えられたGeometryオブジェクトの周長を、デカルト (平面) 座標系で計算します。

**構文**

```sql
perimeterCartesian(geom)
```

**引数**

* `geom` — Geometry オブジェクト。[Geometry](../../data-types/geo.md)。

**戻り値**

* 数値 — 座標系の単位で表したオブジェクトの周長。[Float64](../../data-types/float.md)。

**例**

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

指定されたGeometryオブジェクトの面積をデカルト座標系で計算します。

**構文**

```sql
areaCartesian(geom)
```

**引数**

* `geom` — Geometryオブジェクト。[Geometry](../../data-types/geo.md)。

**戻り値**

* 数値 — 座標系の単位で表したオブジェクトの面積。[Float64](../../data-types/float.md)。

**例**

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

球面上の Geometry オブジェクトの周長を計算します。

**構文**

```sql
perimeterSpherical(geom)
```

**引数**

* `geom` — Geometry オブジェクト。[Geometry](../../data-types/geo.md)。

**戻り値**

* 数値 — 周長。[Float64](../../data-types/float.md)。

**例**

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

球面上のGeometry オブジェクトの面積を計算します。

**構文**

```sql
areaSpherical(geom)
```

**引数**

* `geom` — Geometry。[Geometry](../../data-types/geo.md)。

**戻り値**

* 数値 — 面積。[Float64](../../data-types/float.md)。

**例**

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