---
description: 'Mapbox Vector Tiles 인코딩에 관한 문서'
sidebar_label: 'Mapbox Vector Tiles'
sidebar_position: 65
slug: /sql-reference/functions/geo/mvt
title: 'Mapbox Vector Tiles 인코딩용 함수'
doc_type: '참고'
---

<div id="overview">
  ## 개요
</div>

[Mapbox Vector Tiles](https://github.com/mapbox/vector-tile-spec) (MVT)는 MapLibre 및 Mapbox GL 같은 웹 맵
클라이언트가 네이티브로 렌더링하는 protobuf 인코딩 타일입니다. ClickHouse는 서로 연동되는 다음 두 개의
함수를 사용해 이러한 타일을 전적으로 SQL만으로 생성할 수 있습니다.

* `MVTEncodeGeom` — 지오메트리를 slippy-map 타일의 타일별 로컬 픽셀 공간으로 투영하고
  타일 경계에 맞게 클리핑하는 스칼라 함수입니다.
* `MVTEncode` — 그룹에 속한 투영된 지오메트리를 수집해
  단일 레이어 타일의 바이너리 바이트로 인코딩하는 집계 함수입니다.

도우미 함수 `MVTBoundingBox` 및 `MVTBoundingBoxMercator`는 타일의 경계 상자를 반환하므로, 행을
인덱스를 사용해 `WHERE` 절에서 해당 범위로 제한할 수 있습니다.

점, 선, 다각형 지오메트리를 지원하며, `Geometry` 타입과 구체적인 geo 타입(`Point`,
`LineString`, `MultiLineString`, `Ring`, `Polygon`, `MultiPolygon`)도 포함됩니다.

결과 바이트는 완전한 타일이며, `FORMAT RawBLOB`을 사용해 HTTP 인터페이스를 통해 직접 반환할 수 있습니다.

이 함수들은 PostGIS 워크플로를 따르며, PostGIS 이름의 별칭으로도 사용할 수 있습니다. `MVTEncodeGeom`의 별칭은 `ST_AsMVTGeom`이고,
`MVTEncode`의 별칭은 `ST_AsMVT`입니다.

<div id="mvtencodegeom">
  ## MVTEncodeGeom
</div>

지리 좌표(경도/위도)로 주어진 도형을 `zoom`, `tile_x`, `tile_y`로 식별되는
slippy-map 타일의 타일-로컬 픽셀 공간에 투영하고, 정수 픽셀 격자에 맞춘 뒤, 타일에 맞게 클리핑하여
타일-공간 도형을 반환합니다.

이 투영은 전체 `UInt32` 좌표 범위에서 Web Mercator를 사용합니다. 반환되는 좌표의 원점은 타일의
왼쪽 위 모서리에 있으며 y축은 아래쪽을 향합니다. 이는 Mapbox Vector
Tile 포맷의 좌표 규약이므로, 결과를 `MVTEncode`에 직접 전달할 수 있습니다. 좌표는 정수 픽셀로 반올림되므로,
`MVTEncodeGeom`으로 그룹화하면 동일한 격자에 놓인 도형은 하나의 클러스터로 합쳐집니다.

`clip`이 활성화되면(기본값) 도형은 `buffer` 픽셀만큼 확장된 타일 범위(각 축에서
`[-buffer, extent + buffer]`)로 클리핑되며, 완전히 바깥에 있는 도형은 `NULL`이 됩니다. 이는
PostGIS `ST_AsMVTGeom`에 대응합니다.

Polygon 좌표는 검증 전에 `2^30` 윈도우로 제한되는데, 이는 `zoom` 18 및 `extent` 4096에서
전 세계 전체의 픽셀 폭과 정확히 같습니다. 따라서 일반적인 타일에서는 도형이 검증되지만 클리핑되지는 않으며, 이 제한은
극단적인 `zoom` 또는 `extent` 값에 배치된 도형에만 영향을 줍니다.

출력 도형 유형은 입력에 따라 달라집니다. `Point`는 `Point`를 반환합니다. `LineString` 또는 `MultiLineString`은
`MultiLineString`을 반환합니다. `Ring`, `Polygon`, `MultiPolygon`은 `MultiPolygon`을 반환합니다
(클리핑 과정에서 하나의 도형이 여러 파트로 분할될 수 있습니다).

**구문**

```sql
MVTEncodeGeom(geometry, zoom, tile_x, tile_y[, extent[, buffer[, clip]]])
```

**인수**

* `geometry` — 경도/위도 도 단위의 지오메트리입니다. 경도는 `[-180, 180]`으로 제한되며, 위도는 Web Mercator 범위인 `[-85.05112878, 85.05112878]`로 제한됩니다. [`Point`](../../data-types/geo.md) / [`LineString`](../../data-types/geo.md) / [`MultiLineString`](../../data-types/geo.md) / [`Ring`](../../data-types/geo.md) / [`Polygon`](../../data-types/geo.md) / [`MultiPolygon`](../../data-types/geo.md) / [`Geometry`](../../data-types/geo.md).
* `zoom` — `[0, 32]` 범위의 slippy-map 확대 수준입니다. [`UInt8`](../../data-types/int-uint.md).
* `tile_x` — `[0, 2^zoom - 1]` 범위의 타일 컬럼 인덱스입니다. [`UInt32`](../../data-types/int-uint.md).
* `tile_y` — `[0, 2^zoom - 1]` 범위의 타일 행 인덱스입니다. [`UInt32`](../../data-types/int-uint.md).
* `extent` — 타일 한 변의 픽셀 수를 나타내는 선택적 extent이며, 범위는 `[1, 2147483647]`입니다. 기본값은 Mapbox Vector Tile의 기본값인 `4096`입니다. [`UInt32`](../../data-types/int-uint.md).
* `buffer` — 픽셀 단위의 선택적 클립 버퍼이며, 범위는 `[0, 2147483647]`입니다. 기본값은 `1`입니다. [`UInt32`](../../data-types/int-uint.md).
* `clip` — 선택적 flag입니다. 0이 아닌 경우(기본값) 지오메트리가 타일과 버퍼를 포함한 범위로 클리핑됩니다. [`UInt8`](../../data-types/int-uint.md).

**반환 값**

타일 공간의 지오메트리를 반환합니다. 완전히 클리핑되면 `NULL`을 반환합니다. [`Geometry`](../../data-types/geo.md).

**예시**

```sql
SELECT MVTEncodeGeom((13.37, 52.52)::Point, 10, 550, 335) AS pixel
```

```text
┌─pixel──────┐
│ (124,3384) │
└────────────┘
```

<div id="mvtencode">
  ## MVTEncode
</div>

피처 그룹을 바이너리 Mapbox Vector Tile 레이어로 인코딩합니다. 이는 스칼라
함수 `MVTEncodeGeom`에 대응하는 집계 버전입니다. 각 입력 행은 하나의 피처가 되며, point, line, polygon 지오메트리를 지원합니다.

`geometry` 인수는 tile-space 좌표의 `Geometry`이며, 일반적으로 `MVTEncodeGeom`에서 생성됩니다. 지오메트리가 `NULL`인 행은
(예를 들어 `MVTEncodeGeom`에 의해 clipped되어 제외된 경우) 건너뜁니다. 선택 사항인 `properties` 인수는
named 튜플이며, 해당 요소 이름은 피처 속성 키가 되고 요소 타입은 vector tile 값
타입을 결정합니다.

결과는 단일 레이어 타일의 raw bytes입니다. 빈 그룹은 빈 타일을 생성합니다. 이는
PostGIS `ST_AsMVT`에 해당합니다.

**구문**

```sql
MVTEncode(layer_name[, extent[, feature_id_name[, stringify_unsupported]]])(geometry[, properties])
```

**매개변수**

* `layer_name` — vector tile 레이어의 이름입니다. [`String`](../../data-types/string.md).
* `extent` — 타일 한 변의 픽셀 수를 나타내는 extent이며, 범위는 `[1, 2147483647]`입니다. 기본값은 `4096`입니다. [`UInt32`](../../data-types/int-uint.md).
* `feature_id_name` — `properties` 튜플에서 부호 없는 정수 요소의 이름을 지정하는 선택적 매개변수입니다. 이 요소는 tag가 아니라 MVT Feature의 `id`(`UInt64`)로 출력됩니다. 부호 있는 정수는 허용되지 않습니다. `NULL` id는 해당 feature에서 생략됩니다. 매개변수는 위치 기반이므로, 이를 사용하려면 `extent`를 지정해야 합니다. [`String`](../../data-types/string.md).
* `stringify_unsupported` — 선택적 flag(`0`/`1`, 기본값 `0`)입니다. `1`로 설정하면 직접 지원되지 않는 속성 유형(예: 큰 정수, `UUID`, `Decimal`)은 오류를 발생시키는 대신 텍스트 `string_value`로 인코딩됩니다. [`UInt8`](../../data-types/int-uint.md).

**인수**

* `geometry` — 타일 공간(tile-space)의 지오메트리입니다. 예를 들어 `MVTEncodeGeom`의 출력값이 여기에 해당합니다. [`Geometry`](../../data-types/geo.md).
* `properties` — feature 속성의 선택적 named 튜플입니다. 요소 이름은 속성 키가 됩니다. [`Tuple`](../../data-types/tuple.md).

**반환 값**

단일 레이어 Mapbox Vector Tile의 binary 내용을 반환합니다. [`String`](../../data-types/string.md).

<div id="property-types">
  ### 속성 유형
</div>

각 속성 요소는 해당 ClickHouse 타입에 맞는 Mapbox Vector Tile `Value` variant로 인코딩됩니다:

| ClickHouse type                                                | Vector tile value type |
| -------------------------------------------------------------- | ---------------------- |
| `String` / `FixedString`                                       | `string_value`         |
| `Float32` / `BFloat16`                                         | `float_value`          |
| `Float64`                                                      | `double_value`         |
| `Bool`                                                         | `bool_value`           |
| `Int8` / `Int16` / `Int32` / `Int64` / `Date32`                | `sint_value`           |
| `UInt8` / `UInt16` / `UInt32` / `UInt64` / `Date` / `DateTime` | `uint_value`           |

타입은 `널 허용` 및/또는 `LowCardinality`로 감쌀 수 있습니다. 벡터 타일 포맷에는 null 값이 없으므로 `NULL` 값이면 해당 피처에서 그 속성이 생략됩니다. 그 외의 속성 타입은 `stringify_unsupported`가 설정되지 않은 한 예외를 발생시키며, 설정된 경우에는 텍스트 `string_value`로 인코딩됩니다.

동일한 속성 값은 레이어의 공유 값 풀에 intern되므로, 여러 피처에 나타나는 값도 한 번만 저장됩니다.

<div id="naming-the-properties-tuple">
  ### properties 튜플 이름 지정
</div>

properties 튜플의 요소 이름은 명시적으로 지정해야 합니다. `tuple(...)` 내부의 컬럼 별칭은 튜플
요소 이름으로 **전파되지 않으므로**, 캐스트를 사용해 요소 이름을 지정하십시오:

```sql
tuple(count(), any(id))::Tuple(cluster_count UInt64, id String)
```

<div id="clustering">
  ### 클러스터링
</div>

클러스터링은 함수가 아니라 SQL로 표현됩니다. `MVTEncodeGeom`은 픽셀 단위로 반올림하므로, 픽셀 지오메트리를 기준으로 그룹화하면 같은 위치에 겹치는 지오메트리가 머지됩니다. 하위 쿼리(subquery)에서 각 그룹을 집계한 다음, 클러스터마다 한 행씩 `MVTEncode`에 전달하십시오:

```sql
SELECT MVTEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)) AS tile
FROM
(
    SELECT MVTEncodeGeom((lon, lat)::Point, 10, 550, 335) AS geom, count() AS cluster_count
    FROM points
    GROUP BY geom
)
SETTINGS allow_suspicious_types_in_group_by = 1;
```

`Geometry` 값으로 그룹화하려면 `allow_suspicious_types_in_group_by = 1` 설정이 필요합니다. `Variant` 기반
`Geometry` 타입은 기본적으로 그룹화가 제한되기 때문입니다. 클러스터링된 피처가 아니라 입력 행마다 하나의 피처를 출력하려면
내부 `GROUP BY`(및 `count()`)를 생략하십시오.

<div id="mvtboundingbox">
  ## MVTBoundingBox
</div>

`zoom`, `tile_x`, `tile_y`로 식별되는 slippy-map 타일의 지리적 경계 상자를 도 단위의 튜플
`(min_lon, min_lat, max_lon, max_lat)`로 반환합니다.

행마다 Web Mercator 투영을 다시 계산하는 대신, `longitude`/`latitude` 컬럼에 직접 필터를 적용해 행을 해당 타일 범위로 제한할 때 사용하십시오. 이렇게 하면 해당 컬럼의 프라이머리 키(primary key) 또는
인덱스를 사용할 수 있습니다. 선택적 인수 `margin`은 타일 크기의 해당 비율만큼 경계 상자를 각 방향으로 확장합니다. `MVTEncodeGeom`의 클립 버퍼를
포함하려면 `buffer / extent`로 설정하십시오.

**구문**

```sql
MVTBoundingBox(zoom, tile_x, tile_y[, margin])
```

**인수**

* `zoom` — `[0, 32]` 범위의 Slippy-map 줌 수준입니다. [`UInt8`](../../data-types/int-uint.md).
* `tile_x` — `[0, 2^zoom - 1]` 범위의 타일 컬럼 인덱스입니다. [`UInt32`](../../data-types/int-uint.md).
* `tile_y` — `[0, 2^zoom - 1]` 범위의 타일 행 인덱스입니다. [`UInt32`](../../data-types/int-uint.md).
* `margin` — 상자의 각 변을 확장하는 데 사용할 타일 크기의 선택적 비율입니다. 기본값은 `0`입니다. [`Float64`](../../data-types/float.md).

**반환 값**

타일 경계 상자를 도 단위의 튜플 `(min_lon, min_lat, max_lon, max_lat)`로 반환합니다. [`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md).

**예시**

```sql
SELECT MVTBoundingBox(0, 0, 0) AS bbox
```

```text
┌─bbox────────────────────────────────────────────┐
│ (-180,-85.05112877980659,180,85.05112877980659)  │
└──────────────────────────────────────────────────┘
```

<div id="mvtboundingboxmercator">
  ## MVTBoundingBoxMercator
</div>

`MVTBoundingBox`의 Web Mercator 버전입니다. `MVTEncodeGeom`에서 내부적으로 사용하는 전체 `UInt32` Web Mercator 좌표 공간에서 타일의
경계 상자를 `(min_x, min_y, max_x, max_y)` Tuple로 반환합니다. y축은 아래쪽으로 증가합니다(북쪽이 위쪽). `longitude`/`latitude` 대신
Mercator 좌표 컬럼을 구체화하고 해당 컬럼을 인덱싱하는 테이블에 사용하도록 설계되었습니다.

**구문**

```sql
MVTBoundingBoxMercator(zoom, tile_x, tile_y[, margin])
```

**인수**

[`MVTBoundingBox`](#mvtboundingbox)와 동일합니다.

**반환 값**

Web Mercator 좌표계에서 타일의 경계 상자를 튜플 `(min_x, min_y, max_x, max_y)` 형태로 반환합니다. [`Tuple(Float64, Float64, Float64, Float64)`](../../data-types/tuple.md).

**예시**

```sql
SELECT MVTBoundingBoxMercator(1, 0, 0) AS bbox
```

```text
┌─bbox────────────────────────┐
│ (0,0,2147483648,2147483648)  │
└──────────────────────────────┘
```

<div id="restricting-rows-to-a-tile">
  ## 행을 타일로 제한하기
</div>

타일에는 해당 타일에 속한 지오메트리만 포함되어야 합니다. 이를 가장 잘 표현하는 방법은 서로 맞물려 동작하는 두 단계입니다. 즉, `WHERE` 절의 비용이 낮고
인덱스를 활용하는 경계 상자 프레디케이트(성능)와 `MVTEncodeGeom`의 클리핑(정확성)입니다.
클리핑은 타일 밖의 지오메트리를 삭제하므로, 경계 상자 프레디케이트가 다소 느슨하더라도 타일 밖의 지오메트리가
결과에 섞여 들어가지 않습니다.

```sql
WITH
    1 AS buffer,
    4096 AS extent,
    MVTBoundingBox({z:UInt8}, {x:UInt32}, {y:UInt32}, buffer / extent) AS bounding_box   -- margin matches the clip buffer
SELECT MVTEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64))
FROM
(
    SELECT MVTEncodeGeom((lon, lat)::Point, {z:UInt8}, {x:UInt32}, {y:UInt32}) AS geom, count() AS cluster_count
    FROM points
    WHERE lon BETWEEN bounding_box.1 AND bounding_box.3 AND lat BETWEEN bounding_box.2 AND bounding_box.4   -- index-using prefilter
    GROUP BY geom
)
SETTINGS allow_suspicious_types_in_group_by = 1
```

바운딩 박스 프레디케이트는 대략적인 사전 필터일 뿐이며, 정확한 타일 경계는
`MVTEncodeGeom`의 클리핑에 의해 적용됩니다. 클리핑을 비활성화하고
`WHERE` 프레디케이트에만 의존하려면 `MVTEncodeGeom`에 `clip => false`(7번째 인수)를 전달하십시오.

<div id="serving-tiles-over-http">
  ## HTTP를 통해 타일 제공
</div>

ClickHouse는 기본적으로 타일 endpoint를 노출하지 않습니다. HTTP 인터페이스는 `/`에서만 쿼리만 허용합니다. 깔끔한
`/tile/{z}/{x}/{y}` URL은 서버 구성의 [미리 정의된 쿼리 핸들러](/ko/interfaces/http)를 통해 오퍼레이터가 추가합니다. 이
핸들러의 `url`은 `regex:` 형식을 사용해 경로 세그먼트를 추출하고, 이를 쿼리
매개변수에 바인딩한 뒤, `FORMAT RawBLOB`으로 바이트를 반환합니다.

가장 단순한 경우 테이블에는 `Geometry` 컬럼이 있고, 핸들러는 행마다 피처 하나를 제공합니다 — `MVTEncodeGeom`은
각 지오메트리를 요청된 타일로 투영하고 잘라내므로 타일 밖에 있는 행은 자동으로 제외됩니다:

```xml
<http_handlers>
    <rule>
        <methods>GET</methods>
        <url><![CDATA[regex:/tile/(?P<z>\d+)/(?P<x>\d+)/(?P<y>\d+)]]></url>
        <handler>
            <type>predefined_query_handler</type>
            <query>
                SELECT MVTEncode('shapes')(
                    MVTEncodeGeom(geom, {z:UInt8}, {x:UInt32}, {y:UInt32}),
                    tuple(id, name)::Tuple(id UInt32, name String))
                FROM shapes
                FORMAT RawBLOB
            </query>
            <content_type>application/vnd.mapbox-vector-tile</content_type>
        </handler>
    </rule>
    <defaults/>
</http_handlers>
```

여기서 `shapes`는 `geom Geometry` 컬럼(포인트, 선, 다각형이 임의로 섞여 있을 수 있음)을 가진 테이블입니다. `GET /tile/10/550/335`는
인코딩된 타일을 반환합니다.

포인트 데이터의 경우 `MVTEncodeGeom((lon, lat)::Point, …)`를 사용해 포인트를 Inline으로 직접 생성하면 일반 `longitude`/`latitude` 컬럼에도 동일하게 적용할 수 있습니다. 동일 위치의 피처를 클러스터링하거나, 큰 테이블에 대해 인덱스를 사용하는 바운딩 박스 사전 필터를 추가하려면
내부 쿼리를 [Clustering](#clustering) 및
[Restricting rows to a tile](#restricting-rows-to-a-tile)에 표시된 것처럼 확장하십시오.

<div id="limitations">
  ## 제한 사항
</div>

* Web Mercator 투영법은 위도를 `±85.05112878°`로 제한하며, 반대 자오선을 가로지르는 입력은 지원하지 않습니다.