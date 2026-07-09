---
description: 'Geometry 함수에 대한 문서'
sidebar_label: 'Geometry'
slug: /sql-reference/functions/geo/geometry
title: 'Geometry를 다루는 함수'
doc_type: 'reference'
---

<div id="geometry">
  ## Geometry
</div>

Geometry 함수는 POLYGON, LINESTRING, MULTIPOLYGON, MULTILINESTRING, RING, POINT와 같은 기하 타입의 둘레와 면적을 계산할 수 있습니다. 지오메트리에는 Geometry 타입을 사용합니다. 입력 값이 `NULL`이면 아래의 모든 함수는 0을 반환합니다.

<div id="perimetercartesian">
  ## perimeterCartesian
</div>

주어진 Geometry 객체의 둘레를 데카르트(평면) 좌표계에서 계산합니다.

**구문**

```sql
perimeterCartesian(geom)
```

**인수**

* `geom` — Geometry 객체. [Geometry](../../data-types/geo.md).

**반환 값**

* 숫자 — 좌표계 단위를 기준으로 한 객체의 둘레입니다. [Float64](../../data-types/float.md).

**예시**

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

주어진 Geometry 객체의 면적을 데카르트 좌표계에서 계산합니다.

**구문**

```sql
areaCartesian(geom)
```

**인수**

* `geom` — Geometry 객체. [Geometry](../../data-types/geo.md).

**반환 값**

* 숫자 — 좌표계 단위로 측정한 객체의 면적입니다. [Float64](../../data-types/float.md).

**예시**

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

구 표면 위에 있는 Geometry 객체의 둘레를 계산합니다.

**구문**

```sql
perimeterSpherical(geom)
```

**인수**

* `geom` — Geometry 객체. [Geometry](../../data-types/geo.md).

**반환 값**

* 숫자 — 둘레 길이. [Float64](../../data-types/float.md).

**예시**

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

구면 위에 있는 Geometry 객체의 면적을 계산합니다.

**구문**

```sql
areaSpherical(geom)
```

**인수**

* `geom` — Geometry. [Geometry](../../data-types/geo.md).

**반환 값**

* 숫자 — 면적. [Float64](../../data-types/float.md).

**예시**

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