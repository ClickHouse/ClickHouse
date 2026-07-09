---
description: '좌표 관련 문서'
sidebar_label: '지리 좌표'
slug: /sql-reference/functions/geo/coordinates
title: '지리 좌표를 다루는 함수'
doc_type: 'reference'
---

<div id="greatcircledistance">
  ## greatCircleDistance
</div>

[대권 공식](https://en.wikipedia.org/wiki/Great-circle_distance)을 사용하여 지구 표면의 두 지점 사이의 거리를 계산합니다.

```sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**입력 매개변수**

* `lon1Deg` — 첫 번째 지점의 경도(도)입니다. 범위: `[-180°, 180°]`.
* `lat1Deg` — 첫 번째 지점의 위도(도)입니다. 범위: `[-90°, 90°]`.
* `lon2Deg` — 두 번째 지점의 경도(도)입니다. 범위: `[-180°, 180°]`.
* `lat2Deg` — 두 번째 지점의 위도(도)입니다. 범위: `[-90°, 90°]`.

양수 값은 북위와 동경을, 음수 값은 남위와 서경을 나타냅니다.

**반환 값**

지구 표면의 두 지점 사이 거리이며, 단위는 미터입니다.

입력 매개변수 값이 범위를 벗어나면 예외가 발생합니다.

**예시**

```sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673) AS greatCircleDistance
```

```text
┌─greatCircleDistance─┐
│            14128352 │
└─────────────────────┘
```

<div id="geodistance">
  ## geoDistance
</div>

`greatCircleDistance`와 유사하지만, 구가 아니라 WGS-84 타원체에서 거리를 계산합니다. 이는 지구 지오이드(Geoid)를 더 정밀하게 근사합니다.
성능은 `greatCircleDistance`와 동일합니다(성능 저하 없음). 지구상의 거리를 계산할 때는 `geoDistance`를 사용하는 것이 좋습니다.

기술 참고: 충분히 가까운 Point의 경우 좌표 중점의 접평면 메트릭을 사용하는 평면 근사로 거리를 계산합니다.

```sql
geoDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**입력 매개변수**

* `lon1Deg` — 첫 번째 지점의 경도이며 단위는 도입니다. 범위: `[-180°, 180°]`.
* `lat1Deg` — 첫 번째 지점의 위도이며 단위는 도입니다. 범위: `[-90°, 90°]`.
* `lon2Deg` — 두 번째 지점의 경도이며 단위는 도입니다. 범위: `[-180°, 180°]`.
* `lat2Deg` — 두 번째 지점의 위도이며 단위는 도입니다. 범위: `[-90°, 90°]`.

양수는 북위와 동경을, 음수는 남위와 서경을 나타냅니다.

**반환 값**

지구 표면상의 두 지점 사이의 거리이며, 단위는 미터입니다.

입력 매개변수 값이 범위를 벗어나면 예외가 발생합니다.

**예시**

```sql
SELECT geoDistance(38.8976, -77.0366, 39.9496, -75.1503) AS geoDistance
```

```text
┌─geoDistance─┐
│   212458.73 │
└─────────────┘
```

<div id="greatcircleangle">
  ## greatCircleAngle
</div>

[대권 공식](https://en.wikipedia.org/wiki/Great-circle_distance)을 사용하여 지구 표면에 있는 두 Point 사이의 중심각을 계산합니다.

```sql
greatCircleAngle(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**입력 매개변수**

* `lon1Deg` — 첫 번째 점의 경도(도)입니다.
* `lat1Deg` — 첫 번째 점의 위도(도)입니다.
* `lon2Deg` — 두 번째 점의 경도(도)입니다.
* `lat2Deg` — 두 번째 점의 위도(도)입니다.

**반환 값**

두 점 사이의 중심각(도)입니다.

**예시**

```sql
SELECT greatCircleAngle(0, 0, 45, 0) AS arc
```

```text
┌─arc─┐
│  45 │
└─────┘
```

<div id="geotoutm">
  ## geoToUTM
</div>

WGS84 지리 좌표 `(longitude, latitude)`를 [Universal Transverse Mercator (UTM)](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) 좌표로 변환합니다.

UTM은 60개의 횡축 메르카토르 프로젝션으로 구성된 체계로, 각 프로젝션은 경도 방향으로 6° 너비의 구역을 담당하며 지리 좌표를 미터 단위의 평면 격자로 변환합니다. 명시적으로 `zone`을 지정하지 않으면, 노르웨이와 스발바르에 대한 표준 예외를 적용하여 경도값을 기준으로 구역을 자동 선택합니다. UTM은 위도 범위 `[-80°, 84°]`에서만 정의되며, 극지방은 별도의 UPS 시스템을 사용합니다.

```sql
geoToUTM(longitude, latitude[, zone])
```

**인수**

* `longitude` — 도 단위 경도입니다. 범위: `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `latitude` — 도 단위 위도입니다. 범위: `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `zone` — 선택 사항입니다. 자동 선택 대신 이 UTM 구역으로 투영을 강제합니다. 범위: `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).

**반환 값**

named tuple `(easting, northing, zone, band)`를 반환합니다. `easting` 및 `northing`은 미터 단위([`Float64`](../../data-types/float.md))이며, UTM `zone` 번호는 [`UInt8`](../../data-types/int-uint.md)이고, MGRS 위도 `band` 문자는 [`FixedString(1)`](../../data-types/fixedstring.md)입니다. `band`가 `'N'` 이후이면 북반구를 의미합니다.

위도가 `[-80°, 84°]` 범위를 벗어나거나 경도가 `[-180°, 180°]` 범위를 벗어나면 예외가 발생합니다.

**예시**

```sql
SELECT geoToUTM(2.294497, 48.858222) AS utm; -- Eiffel Tower
```

```text
(448251.5978370684,5411935.125629659,31,'U')
```

<div id="utmtogeo">
  ## UTMToGeo
</div>

[UTM](https://en.wikipedia.org/wiki/Universal_Transverse_Mercator_coordinate_system) 좌표를 WGS84 지리 좌표 `(경도, 위도)`로 변환합니다. 이는 [`geoToUTM`](#geotoutm)의 역변환입니다.

```sql
UTMToGeo(easting, northing, zone, is_north)
```

**인수**

* `easting` — 미터 단위의 동향 좌표입니다(500000 m false easting 포함). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
* `northing` — 미터 단위의 북향 좌표입니다(남반구에서는 10000000 m false northing 포함). [`(U)Int*`](../../data-types/int-uint.md)/[`Float*`](../../data-types/float.md).
* `zone` — UTM 구역 번호입니다. 범위: `[1, 60]`. [`(U)Int*`](../../data-types/int-uint.md).
* `is_north` — 반구를 나타냅니다. 북반구는 `1`, 남반구는 `0`입니다. [`(U)Int*`](../../data-types/int-uint.md).

**반환 값**

도 단위의 named tuple `(longitude, latitude)`입니다. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**예시**

```sql
SELECT UTMToGeo(448251.6, 5411935.13, 31, 1) AS coord;
```

```text
(2.2944970289079203,48.85822204127082)
```

<div id="geotomgrs">
  ## geoToMGRS
</div>

WGS84 지리 좌표 `(longitude, latitude)`를 [Military Grid Reference System (MGRS)](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) 문자열로 인코딩합니다.

문자열 형식은 `<zone><band><100km square><easting><northing>`이며, 예를 들어 `31UDQ4825111935`와 같습니다. `precision` 인수는 easting과 northing에 각각 사용할 자릿수를 제어합니다. `5`(기본값)는 1 m, `4`는 10 m, `3`은 100 m, `2`는 1 km, `1`은 10 km, `0`은 100 km 격자 구역만 나타냅니다. MGRS는 위도 범위 `[-80°, 84°]`에서만 정의됩니다.

```sql
geoToMGRS(longitude, latitude[, precision])
```

**인수**

* `longitude` — 도 단위의 경도입니다. 범위: `[-180°, 180°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `latitude` — 도 단위의 위도입니다. 범위: `[-80°, 84°]`. [`Float32`](../../data-types/float.md)/[`Float64`](../../data-types/float.md).
* `precision` — 선택 사항입니다. easting과 northing 각각의 자릿수입니다. 기본값: `5`. 범위: `[0, 5]`. [`(U)Int*`](../../data-types/int-uint.md).

**반환 값**

MGRS 참조 문자열입니다. [`String`](../../data-types/string.md).

**예시**

```sql
SELECT geoToMGRS(2.294497, 48.858222) AS mgrs, geoToMGRS(2.294497, 48.858222, 3) AS mgrs_100m;
```

```text
┌─mgrs────────────┬─mgrs_100m───┐
│ 31UDQ4825111935 │ 31UDQ482119 │
└─────────────────┴─────────────┘
```

<div id="mgrstogeo">
  ## MGRSToGeo
</div>

[MGRS](https://en.wikipedia.org/wiki/Military_Grid_Reference_System) 문자열을 WGS84 지리 좌표 `(longitude, latitude)`로 디코딩합니다. 이는 [`geoToMGRS`](#geotomgrs)의 역변환입니다.

반환되는 Point는 참조된 그리드 사각형의 중심이므로, 결과의 정밀도는 문자열에 인코딩된 정밀도와 일치합니다. 입력의 공백은 무시되며, 문자는 대소문자를 구분하지 않습니다.

```sql
MGRSToGeo(mgrs)
```

**인수**

* `mgrs` — 디코딩할 MGRS 참조 문자열입니다. [`String`](../../data-types/string.md)/[`FixedString`](../../data-types/fixedstring.md).

**반환 값**

도 단위의 `(longitude, latitude)` named tuple입니다. [`Tuple(Float64, Float64)`](../../data-types/tuple.md).

**예시**

```sql
SELECT MGRSToGeo('31UDQ4825111935') AS coord;
```

```text
(2.294495618908297,48.85822536113692)
```

<div id="pointinellipses">
  ## pointInEllipses
</div>

Point가 하나 이상의 타원에 속하는지 확인합니다.
좌표는 데카르트 좌표계의 기하학적 좌표입니다.

```sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**입력 매개변수**

* `x, y` — 평면 위 Point의 좌표입니다.
* `xᵢ, yᵢ` — `i`번째 타원의 중심 좌표입니다.
* `aᵢ, bᵢ` — x, y 좌표 단위로 나타낸 `i`번째 타원의 축입니다.

입력 매개변수의 개수는 `2+4⋅n`개여야 하며, 여기서 `n`은 타원의 개수입니다.

**반환 값**

Point가 하나 이상의 타원 내부에 있으면 `1`, 그렇지 않으면 `0`입니다.

**예시**

```sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

```text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

<div id="pointinpolygon">
  ## pointInPolygon
</div>

Point가 평면상의 Polygon에 속하는지 확인합니다.

```sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**입력 값**

* `(x, y)` — 평면 위 Point의 좌표입니다. 데이터 타입 — [Tuple](../../data-types/tuple.md) — 두 개의 숫자로 이루어진 튜플입니다.
* `[(a, b), (c, d) ...]` — 다각형의 꼭짓점입니다. 데이터 타입 — [배열](../../data-types/array.md). 각 꼭짓점은 좌표 쌍 `(a, b)`로 표현됩니다. 꼭짓점은 시계 방향 또는 반시계 방향 순서로 지정해야 합니다. 꼭짓점의 최소 개수는 3개입니다. 다각형은 상수여야 합니다.
* 이 함수는 hole(비어 있는 내부 영역)이 있는 다각형도 지원합니다. 데이터 타입 — [Polygon](../../data-types/geo.md/#polygon). 전체 `Polygon`을 두 번째 인수로 전달하거나, 바깥쪽 ring을 먼저 전달한 다음 각 hole을 별도의 추가 인수로 전달할 수 있습니다.
* 이 함수는 multipolygon도 지원합니다. 데이터 타입 — [MultiPolygon](../../data-types/geo.md/#multipolygon). 전체 `MultiPolygon`을 두 번째 인수로 전달하거나, 각 구성 polygon을 개별 인수로 나열할 수 있습니다.

**반환 값**

Point가 다각형 내부에 있으면 `1`, 그렇지 않으면 `0`을 반환합니다.
Point가 다각형 경계 위에 있으면 함수는 `0` 또는 `1`을 반환할 수 있습니다.

**예시**

```sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

```text
┌─res─┐
│   1 │
└─────┘
```

> **참고**
> • `validate_polygons = 0`을 설정하면 Geometry 유효성 검사를 우회할 수 있습니다.
> • `pointInPolygon`은 모든 다각형이 올바르게 구성되어 있다고 가정합니다. 입력이 자기 교차하거나, 링(Ring)의 순서가 잘못되었거나, 변이 서로 겹치면 결과를 신뢰할 수 없게 됩니다. 특히 Point가 정확히 변이나 꼭짓점 위에 있거나, &quot;내부&quot;와 &quot;외부&quot;의 개념이 undefined인 자기 교차 영역 내부에 있는 경우에는 더욱 그렇습니다.
> • 다각형 인수가 상수이고 점이 인덱싱된 키 컬럼으로 표현되는 경우(예: `x, y`가 `PRIMARY KEY`의 일부이거나 `minmax` 인덱스가 적용된 테이블에서 `pointInPolygon((x, y), constant_polygon)`을 사용하는 경우), ClickHouse는 기본 키(primary key)와 `minmax` 데이터 스키핑 인덱스를 모두 사용해 관련 없는 그래뉼을 프루닝할 수 있습니다.