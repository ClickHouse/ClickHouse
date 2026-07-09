---
alias: []
description: 'GeoJSON FeatureCollection 문서용 입력 및 출력 포맷입니다. 입력 시에는 각 feature가 id, geometry, properties 컬럼을 포함한 1개의 행으로 처리되며, 출력 시에는 각 행이 1개의 feature로 출력됩니다.'
input_format: true
output_format: true
keywords: ['GeoJSON']
sidebar_label: 'GeoJSON'
sidebar_position: 1
slug: /interfaces/formats/GeoJSON
title: 'GeoJSON'
doc_type: 'reference'
---

| 입력 | 출력 | 별칭 |
| -- | -- | -- |
| ✔  | ✔  |    |

<div id="description">
  ## 설명
</div>

[GeoJSON](https://geojson.org/) 데이터는 단일 [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3) 문서 형식으로 주고받으며, ClickHouse는 이를 `id`, `geometry`, `properties`의 3개 컬럼에 매핑합니다. 각 `Feature`마다 한 세트씩 대응됩니다. 문서를 [읽으면](#reading-data) `Feature`당 1개의 행이 생성되고, [쓰면](#writing-data) 행당 1개의 `Feature`가 생성됩니다.

<div id="reading-data">
  ## 데이터 읽기
</div>

`FeatureCollection`을 읽으면 각 피처마다 다음과 같은 고정 스키마의 행 1개가 생성됩니다:

| Column       | Type               | Description                                                                                           |
| ------------ | ------------------ | ----------------------------------------------------------------------------------------------------- |
| `id`         | `Nullable(String)` | 피처의 `id` 멤버(JSON 문자열 또는 숫자)를 텍스트로 저장합니다. `id`가 없거나 `null`이면 `NULL`이 되며, 명시적으로 빈 문자열인 id는 `''`로 유지됩니다. |
| `geometry`   | `Geometry`         | 피처의 지오메트리를 `Geometry` variant type으로 저장합니다.                                                        |
| `properties` | `Nullable(JSON)`   | 피처의 `properties` 객체를 반정형 `JSON` 컬럼으로 저장합니다. 명시적으로 `"properties": null`인 경우 `NULL`로 유지됩니다.             |

각 지오메트리는 ClickHouse의 `Geometry` 타입(`Variant`)에 저장됩니다. 지원되는 GeoJSON 지오메트리 타입은 `Point`, `LineString`, `MultiLineString`, `Polygon`, `MultiPolygon`입니다. 나머지 두 GeoJSON 지오메트리 타입인 `GeometryCollection`과 `MultiPoint`는 `Geometry` 타입으로 표현할 수 없습니다. 이를 `geometry` 컬럼으로 읽으면 기본적으로 예외가 발생하지만, 이 동작은 대신 `NULL`을 삽입하도록 변경할 수 있습니다. 자세한 내용은 아래의 [Handling unsupported geometry types](#unsupported-geometry)를 참조하십시오. 기본적으로 `geometry` 컬럼이 `NULL`이 되는 경우는 피처의 지오메트리가 명시적인 JSON `null`일 때뿐입니다. `input_format_geojson_unsupported_geometry_handling = 'null'`로 설정하면 지원되지 않는 지오메트리 타입도 `NULL`이 됩니다.

문서 구조에 대해서도 유효성 검사를 수행합니다. 최상위 `type`은 반드시 `FeatureCollection`이어야 하며, `features`의 모든 요소는 `type`이 `Feature`여야 합니다. 기본적으로 좌표는 GeoJSON shape 불변 조건을 만족해야 합니다. 즉, `LineString`(및 `MultiLineString`의 각 선)은 최소 두 개의 Point를 가져야 하고, `Polygon`의 Ring(및 `MultiPolygon`의 각 Ring)은 닫혀 있어야 하며 최소 네 개의 Point를 가져야 합니다([Geometry validation](#geometry-validation) 참조). 형식이 잘못된 문서는 조용히 로드되지 않고 거부됩니다.

키 순서는 유연합니다. 최상위 `type`은 `features` 배열 앞이나 뒤에 올 수 있고, 지오메트리 객체 내부에서도 `coordinates`는 `type` 앞이나 뒤에 올 수 있습니다.

스키마 추론은 위의 고정 스키마를 반환하므로, 테이블 정의 없이도 `DESCRIBE`와 `SELECT ... FROM format(...)`를 사용할 수 있습니다.

다음은 여러 지오메트리 타입이 섞여 있는 GeoJSON 파일 `london.geojson`입니다:

```json
{
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "1",
            "geometry": {"type": "Point", "coordinates": [-0.0761, 51.5081]},
            "properties": {"name": "Tower of London", "feature_type": "landmark", "year_built": 1078}
        },
        {
            "type": "Feature",
            "id": "2",
            "geometry": {
                "type": "LineString",
                "coordinates": [[-0.2500, 51.4700], [-0.1800, 51.4900], [-0.1200, 51.5060], [-0.0700, 51.5050], [0.0000, 51.5100]]
            },
            "properties": {"name": "River Thames", "feature_type": "river", "length_km": 346}
        },
        {
            "type": "Feature",
            "id": "3",
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[-0.1880, 51.5074], [-0.1533, 51.5074], [-0.1533, 51.5153], [-0.1880, 51.5153], [-0.1880, 51.5074]]]
            },
            "properties": {"name": "Hyde Park", "feature_type": "park", "area_km2": 1.42}
        }
    ]
}
```

파일에 쿼리를 실행해 Geometry 타입을 확인할 수 있습니다:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
┌─id─┬─name────────────┬─geo_type───┐
│ 1  │ Tower of London │ Point      │
│ 2  │ River Thames    │ LineString │
│ 3  │ Hyde Park       │ Polygon    │
└────┴─────────────────┴────────────┘
```

파일 확장자 `.geojson`는 자동으로 인식되므로 포맷 인수는 생략할 수 있습니다:

```sql title="Query"
SELECT id, properties.name AS name, variantType(geometry) AS geo_type
FROM file('london.geojson');
```

각 Geometry 객체의 내부 유형을 확인하려면 `variantType`을 사용할 수 있습니다:

```sql title="Query"
SELECT properties.name AS name, geometry, variantType(geometry)
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
geometry:              (-0.0761,51.5081)
variantType(geometry): Point

Row 2:
──────
name:                  River Thames
geometry:              [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
variantType(geometry): LineString

Row 3:
──────
name:                  Hyde Park
geometry:              [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
variantType(geometry): Polygon
```

그리고 다음과 같이 원본 데이터를 추출할 수 있습니다:

```sql title="Query"
SELECT properties.name AS name, variantType(geometry), geometry.Point, geometry.LineString, geometry.Polygon
FROM file('london.geojson', GeoJSON);
```

```response title="Response"
Row 1:
──────
name:                  Tower of London
variantType(geometry): Point
geometry.Point:        (-0.0761,51.5081)
geometry.LineString:   []
geometry.Polygon:      []

Row 2:
──────
name:                  River Thames
variantType(geometry): LineString
geometry.Point:        (0,0)
geometry.LineString:   [(-0.25,51.47),(-0.18,51.49),(-0.12,51.506),(-0.07,51.505),(0,51.51)]
geometry.Polygon:      []

Row 3:
──────
name:                  Hyde Park
variantType(geometry): Polygon
geometry.Point:        (0,0)
geometry.LineString:   []
geometry.Polygon:      [[(-0.188,51.5074),(-0.1533,51.5074),(-0.1533,51.5153),(-0.188,51.5153),(-0.188,51.5074)]]
```

`Geometry` 하위 컬럼에 접근하면 행에 해당 타입의 값이 들어 있는 경우 그 값을 반환하고, 그렇지 않으면 해당 타입의 기본값을 반환합니다. `Point`는 `(0,0)`, 배열 기반 타입은 `[]`을 반환하므로, 어떤 타입이 설정되어 있는지 확인하려면 `variantType(geometry)`를 사용하십시오.

GeoJSON 데이터도 테이블에 수집할 수 있습니다:

```sql title="Query"
CREATE TABLE london
(
    id           String,
    geometry     Geometry,
    properties   Nullable(JSON),
    name         String MATERIALIZED properties.name,
    feature_type String MATERIALIZED properties.feature_type
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO london
SELECT id, geometry, properties
FROM file('london.geojson', GeoJSON);
```

그런 다음 기능 유형을 기준으로 쿼리합니다:

```sql title="Query"
SELECT name, feature_type, variantType(geometry) AS geo_type
FROM london
ORDER BY id;
```

```response title="Response"
┌─name────────────┬─feature_type─┬─geo_type───┐
│ Tower of London │ landmark     │ Point      │
│ River Thames    │ river        │ LineString │
│ Hyde Park       │ park         │ Polygon    │
└─────────────────┴──────────────┴────────────┘
```

GeoJSON 데이터의 스키마도 테이블 정의 없이 추론할 수 있습니다:

```sql title="Query"
DESCRIBE format(GeoJSON, '{"type":"FeatureCollection","features":[]}');
```

```response title="Response"
┌─name───────┬─type─────────────┐
│ id         │ Nullable(String) │
│ geometry   │ Geometry         │
│ properties │ Nullable(JSON)   │
└────────────┴──────────────────┘
```

<div id="unsupported-geometry">
  ### 지원되지 않는 Geometry 타입 처리
</div>

`GeometryCollection` 및 `MultiPoint`와 같은 일부 유효한 GeoJSON 지오메트리 types는 ClickHouse의 `Geometry` 타입으로 표현할 수 없습니다. 이러한 지오메트리를 `geometry` 컬럼에 저장해야 하는 경우, `input_format_geojson_unsupported_geometry_handling` 설정을 사용해 동작 방식을 제어할 수 있습니다. 가능한 값은 다음과 같습니다.

* `'throw'` — 예외를 발생시킵니다(기본값)
* `'null'` — `geometry` 컬럼에 `NULL` 값을 삽입하고 파싱을 계속합니다

이 처리는 `geometry` 컬럼을 읽을 때만 적용됩니다. `geometry`가 요청된 출력 컬럼이 아닌 경우(예: `SELECT id FROM ...`)에는 지원되지 않는 지오메트리도 형식이 올바른지는 계속 검증되지만, 이 처리가 적용되지는 않습니다. 즉, 지오메트리 값이 구체화되지 않으므로 예외가 발생하지도 않고 `NULL`이 삽입되지도 않습니다.

<div id="reading-limitations">
  ### 제한 사항
</div>

읽을 때는 고정된 스키마에 맞는 내용만 반영되므로, 일부 GeoJSON 정보는 보존되지 않습니다.

* 생성되는 것은 `id`, `geometry`, `properties`뿐이며, 그 밖의 문서 구조는 컬럼으로 노출되지 않습니다.
* 위치의 세 번째(고도) 좌표와 그 이후의 좌표는 제거되므로, 위치는 `[longitude, latitude]`가 됩니다.
* `bbox`와 foreign member(예: 최상위 `name` 또는 `crs`, 혹은 `Feature` 내부의 추가 멤버)는 무시됩니다.
* 숫자형 `id`는 텍스트로 저장되므로 문자열과 숫자의 구분이 사라지며, `id`가 없거나 `null`이면 `NULL`이 됩니다.
* `GeometryCollection`과 `MultiPoint`는 표현할 수 없습니다 — [지원되지 않는 지오메트리 type 처리](#unsupported-geometry)를 참조하십시오.

<div id="writing-data">
  ## 데이터 쓰기
</div>

결과 집합을 기록하면 단일 GeoJSON [`FeatureCollection`](https://datatracker.ietf.org/doc/html/rfc7946#section-3.3)이 생성되며, 각 행마다 `Feature` 1개가 생성됩니다.

결과의 컬럼은 다음과 같이 각 `Feature`에 매핑됩니다.

| Feature member | Built from                       | Notes                                                                                                                                                                               |
| -------------- | -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`         | —                                | 항상 `"Feature"`입니다.                                                                                                                                                                  |
| `geometry`     | the single geometry-typed column | 지오메트리 유형의 컬럼은 정확히 1개여야 하며, 그렇지 않으면 쿼리가 거부됩니다. `NULL` 지오메트리는 `null`로 기록됩니다.                                                                                                    |
| `id`           | a column named `id`              | 값이 `NULL`이면 생략됩니다. `String` 컬럼은 JSON 문자열로, 숫자 컬럼은 JSON 숫자로 기록됩니다.                                                                                                                   |
| `properties`   | all remaining columns            | 이름이 `properties`인 단일 컬럼의 유형이 객체와 유사한 경우(`JSON`, `Map` 또는 이름이 지정된 `Tuple`), `properties` 키 아래에 중첩하지 않고 `properties` 객체로 직접 기록됩니다. 그렇지 않으면 나머지 각 컬럼은 자신의 이름을 키로 하는 속성이 됩니다(없으면 빈 객체). |

지오메트리 유형 컬럼은 `Geometry` Variant이거나 특정 geo type일 수 있으며, 각각은 다음 GeoJSON 지오메트리 type에 매핑됩니다.

| ClickHouse type   | GeoJSON `"type"`          |
| ----------------- | ------------------------- |
| `Point`           | `Point`                   |
| `LineString`      | `LineString`              |
| `MultiLineString` | `MultiLineString`         |
| `Polygon`         | `Polygon`                 |
| `MultiPolygon`    | `MultiPolygon`            |
| `Ring`            | `Polygon` (단일 ring)       |
| `Geometry`        | 활성 Variant의 유형(또는 `null`) |

`Ring`은 GeoJSON 지오메트리 type이 아닙니다. [linear ring](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1.6)은 `Polygon`의 구성 요소이므로, `Ring` 값은 단일 ring `Polygon`으로 기록됩니다.

<div id="writing-examples">
  ### 예시
</div>

앞서 [생성한](#reading-data) `london` 테이블을 계속 사용해, 일반 속성 컬럼을 내보내면 `id`와 `geometry`를 제외한 모든 컬럼이 프로퍼티로 변환됩니다:

```sql title="Query"
SELECT id, geometry, name, feature_type
FROM london
ORDER BY id
FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"name":"Tower of London","feature_type":"landmark"}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"name":"River Thames","feature_type":"river"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"name":"Hyde Park","feature_type":"park"}}]}
```

`properties`라는 이름의 객체 유형 컬럼 하나는 직접 출력되므로, GeoJSON 파일을 읽은 뒤 그대로 다시 기록하면 원본 문서가 재현됩니다(`id`, `geometry`, `properties` 컬럼이 해당 파일에서 추론된 컬럼입니다):

```sql title="Query"
SELECT * FROM file('london.geojson', GeoJSON) FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[-0.0761,51.5081]},"properties":{"feature_type":"landmark","name":"Tower of London","year_built":1078}},{"type":"Feature","id":"2","geometry":{"type":"LineString","coordinates":[[-0.25,51.47],[-0.18,51.49],[-0.12,51.506],[-0.07,51.505],[0,51.51]]},"properties":{"feature_type":"river","length_km":346,"name":"River Thames"}},{"type":"Feature","id":"3","geometry":{"type":"Polygon","coordinates":[[[-0.188,51.5074],[-0.1533,51.5074],[-0.1533,51.5153],[-0.188,51.5153],[-0.188,51.5074]]]},"properties":{"area_km2":1.42,"feature_type":"park","name":"Hyde Park"}}]}
```

숫자형 `id` 컬럼은 JSON 숫자 형식으로 기록됩니다(`NULL`인 널 허용 `id`는 완전히 생략됩니다):

```sql title="Query"
SELECT 42 AS id, (-0.1276, 51.5072)::Point AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","id":42,"geometry":{"type":"Point","coordinates":[-0.1276,51.5072]},"properties":{}}]}
```

`Ring`은 단일 ring `Polygon`으로 표현됩니다:

```sql title="Query"
SELECT [(0., 0.), (10., 0.), (10., 10.), (0., 0.)]::Ring AS geometry FORMAT GeoJSON;
```

```response title="Response"
{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Polygon","coordinates":[[[0,0],[10,0],[10,10],[0,0]]]},"properties":{}}]}
```

<div id="writing-to-a-file">
  ### 파일에 쓰기
</div>

클라이언트에서 GeoJSON 파일로 저장하려면 `INTO OUTFILE`을 사용합니다:

```sql title="Query"
SELECT id, geometry, properties
FROM london
ORDER BY id
INTO OUTFILE 'london_export.geojson'
FORMAT GeoJSON;
```

서버는 `file` 테이블 함수를 사용해 파일에 직접 쓸 수 있습니다(`.geojson` 확장자가 포맷을 자동으로 자동 선택합니다):

```sql title="Query"
INSERT INTO FUNCTION file('london_export.geojson', GeoJSON)
SELECT id, geometry, properties FROM london;
```

<div id="reading-limitations">
  ### 제한 사항
</div>

:::note
ClickHouse의 geo 타입에는 좌표 참조 시스템이 포함되지 않으므로, 출력은 [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-4)에서 요구하는 대로 좌표가 이미 `[longitude, latitude]` 순서의 WGS84 경도/위도라고 가정합니다. 재투영이나 축 스왑은 수행되지 않으므로, 투영 좌표 또는 `(latitude, longitude)`로 저장된 데이터는 구조적으로는 유효하지만 규격을 준수하지 않는 GeoJSON을 생성합니다.
:::

출력에는 ClickHouse에 저장된 내용만 반영됩니다.

* 읽는 과정에서 제거된 정보(위치의 고도, `bbox`, foreign members, 그리고 `id`가 문자열인지 숫자인지에 대한 구분)는 복원할 수 없습니다. [읽기 제한 사항](#reading-limitations)을 참조하십시오.
* 좌표는 `Float64` 값에서 왕복 변환이 가능한 가장 짧은 표현으로 기록됩니다.
* `JSON` 컬럼에서 직접 가져온 `properties` 객체는 `JSON` 타입의 정규 키 순서로 출력되므로, 입력과 순서가 다를 수 있습니다.

지오메트리는 저장된 그대로 기록되며 좌표 순서와 winding도 유지됩니다. 기본적으로 쓰기 시 GeoJSON 도형의 유효성을 검사합니다([Geometry 유효성 검사](#geometry-validation) 참조). 따라서 점이 하나뿐인 `LineString`이나 닫히지 않은 `Polygon` ring처럼 유효한 GeoJSON 도형이 아닌 지오메트리는, 기록한 문서를 다시 읽을 수 있도록 거부됩니다. 대신 이러한 지오메트리를 있는 그대로 출력하려면 `format_geojson_validate_geometry = 0`으로 설정하십시오. 이 경우 구조적으로는 유효하지만 규격을 준수하지 않는 GeoJSON이 생성됩니다. right-hand-rule(winding) 불변 조건은 어느 경우에도 강제되지 않으며, `null`과 비어 있는 `properties` 객체의 구분도 유지됩니다.

<div id="geometry-validation">
  ## Geometry 유효성 검사
</div>

설정 `format_geojson_validate_geometry`는 포맷이 [RFC 7946](https://datatracker.ietf.org/doc/html/rfc7946#section-3.1)의 도형 구조 규칙을 읽기와 쓰기 양방향에서 적용할지 여부를 제어합니다. 이 설정은 기본적으로 활성화되어 있습니다.

활성화된 경우 GeoJSON 구조 규칙을 위반하는 도형은 거부됩니다. 예를 들어 점이 2개 미만인 `LineString`(또는 `MultiLineString`의 선), 점이 4개 미만이거나 첫 점과 마지막 점이 달라 닫히지 않은 `Polygon` 또는 `MultiPolygon`의 ring, 또는 비어 있는 `MultiLineString`, `Polygon`, `MultiPolygon`이 이에 해당합니다. 동일한 규칙이 이러한 문서를 읽을 때와 이러한 ClickHouse 값을 쓸 때 모두 적용되므로, 한 번 기록한 문서는 항상 다시 읽을 수 있습니다.

비활성화된 경우 이러한 구조 규칙은 어느 방향에서도 적용되지 않습니다. 즉, 퇴화된 도형도 있는 그대로 읽고 있는 그대로 기록합니다. 따라서 유효한 GeoJSON 도형이 아닌 ClickHouse 도형 값도 이 포맷을 통해 round-trip할 수 있지만, 그 대신 유효한 GeoJSON이 아닌 문서가 생성될 수 있습니다.

이 유효성 검사는 구조만 검사합니다. 즉, 점 개수와 ring 폐합 여부만 확인합니다. 도형의 기하학적 타당성은 검사하지 않으므로, 구조적으로는 유효하지만 기하학적으로는 퇴화된 도형도 양방향 모두에서 허용됩니다. 예를 들어 면적이 0인 polygon, 자기 자신과 교차하는 ring, 또는 holes(내부 ring)가 외부 ring 바깥에 있는 polygon이 이에 해당합니다. 또한 polygon ring의 right-hand-rule(감김 방향) orientation도 적용되지 않습니다.

한 가지 검사는 이 설정과 무관합니다. 유한하지 않은 좌표(`NaN`, `Inf`)는 JSON 숫자로 표현할 수 없으므로 항상 거부됩니다.