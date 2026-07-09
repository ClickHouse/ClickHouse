---
description: 'flipCoordinates 문서'
sidebar_label: '좌표 뒤집기'
sidebar_position: 63
slug: /sql-reference/functions/geo/flipCoordinates
title: '좌표 뒤집기'
doc_type: 'reference'
---

<div id="flipcoordinates">
  ## flipCoordinates
</div>

`flipCoordinates` 함수는 Point, ring, polygon 또는 multipolygon의 좌표를 서로 바꿉니다. 예를 들어 위도와 경도의 순서가 다른 좌표계 사이를 변환할 때 유용합니다.

```sql
flipCoordinates(coordinates)
```

<div id="input-parameters">
  ### 입력 매개변수
</div>

* `coordinates` — Point를 나타내는 튜플 `(x, y)`이거나, Ring, 다각형 또는 Multipolygon을 나타내는 이러한 튜플의 배열입니다. 지원되는 입력 타입은 다음과 같습니다.
  * [**Point**](../../data-types/geo.md#point): `x`와 `y`가 [Float64](../../data-types/float.md) 값인 튜플 `(x, y)`입니다.
  * [**Ring**](../../data-types/geo.md#ring): Point의 배열 `[(x1, y1), (x2, y2), ...]`입니다.
  * [**Polygon**](../../data-types/geo.md#polygon): Ring의 배열 `[ring1, ring2, ...]`이며, 각 Ring은 Point의 배열입니다.
  * [**Multipolygon**](../../data-types/geo.md#multipolygon): 다각형의 배열 `[polygon1, polygon2, ...]`입니다.

<div id="returned-value">
  ### 반환 값
</div>

이 함수는 좌표 순서가 뒤바뀐 입력값을 반환합니다. 예시는 다음과 같습니다.

* Point `(x, y)`는 `(y, x)`가 됩니다.
* Ring `[(x1, y1), (x2, y2)]`는 `[(y1, x1), (y2, x2)]`가 됩니다.
* 다각형 및 multipolygon과 같은 중첩된 구조는 재귀적으로 처리됩니다.

<div id="examples">
  ### 예시
</div>

<div id="example-1">
  #### 예시 1: 단일 Point 뒤집기
</div>

```sql
SELECT flipCoordinates((10, 20)) AS flipped_point
```

```text
┌─flipped_point─┐
│ (20,10)       │
└───────────────┘
```

<div id="example-2">
  #### 예시 2: Point 배열(Ring) 뒤집기
</div>

```sql
SELECT flipCoordinates([(10, 20), (30, 40)]) AS flipped_ring
```

```text
┌─flipped_ring──────────────┐
│ [(20,10),(40,30)]         │
└───────────────────────────┘
```

<div id="example-3">
  #### 예시 3: 다각형 반전
</div>

```sql
SELECT flipCoordinates([[(10, 20), (30, 40)], [(50, 60), (70, 80)]]) AS flipped_polygon
```

```text
┌─flipped_polygon──────────────────────────────┐
│ [[(20,10),(40,30)],[(60,50),(80,70)]]        │
└──────────────────────────────────────────────┘
```

<div id="example-4">
  #### 예시 4: Multipolygon 뒤집기
</div>

```sql
SELECT flipCoordinates([[[10, 20], [30, 40]], [[50, 60], [70, 80]]]) AS flipped_multipolygon
```

```text
┌─flipped_multipolygon──────────────────────────────┐
│ [[[20,10],[40,30]],[[60,50],[80,70]]]             │
└───────────────────────────────────────────────────┘
```