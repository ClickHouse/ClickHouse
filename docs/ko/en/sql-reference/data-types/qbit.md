---
description: '검색 시점에 근사 벡터 검색을 위한 세밀한 양자화를 지원하는 ClickHouse의 QBit 데이터 타입 문서'
keywords: ['qbit', 'data type']
sidebar_label: 'QBit'
sidebar_position: 64
slug: /sql-reference/data-types/qbit
title: 'QBit 데이터 타입'
doc_type: 'reference'
---

`QBit` 데이터 타입은 더 빠른 근사 벡터 검색을 위해 벡터 저장 방식을 재구성합니다. 각 벡터의 요소를 함께 저장하는 대신, 모든 벡터에 걸쳐 동일한 이진 비트 위치끼리 묶어 저장합니다.
이 방식은 벡터를 전체 정밀도로 저장하면서도 검색 시점에 세밀한 양자화 수준을 선택할 수 있게 합니다. 더 적은 비트를 읽으면 I/O가 줄고 계산이 빨라지며, 더 많은 비트를 읽으면 더 높은 정확도를 얻을 수 있습니다. 즉, 양자화를 통해 데이터 전송과 계산량을 줄여 속도 이점을 얻으면서도, 필요할 때는 원본 데이터를 그대로 사용할 수 있습니다.

`QBit` 타입의 컬럼을 선언하려면 다음 구문을 사용하십시오:

```sql
column_name QBit(element_type, dimension[, stride])
```

* `element_type` – 각 벡터 요소의 타입입니다. 허용되는 타입은 `Int8`, `BFloat16`, `Float32`, `Float64`입니다.
* `dimension` – 각 벡터의 차원입니다.
* `stride` – 선택 사항입니다. 하나의 스트림 그룹에 함께 저장되는 차원의 수입니다. 생략하면 기본값은 `dimension`(단일 그룹)입니다. 지정한 경우 `dimension`은 `stride`의 배수여야 하며, `stride`가 `dimension`보다 작을 때는 `stride`가 8의 배수여야 합니다. [Strides](#strides)를 참조하십시오.

<div id="creating-qbit">
  ## QBit 생성
</div>

테이블 컬럼 정의에서 `QBit` 유형을 사용하는 방법:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [1, 2, 3, 4, 5, 6, 7, 8]), (2, [9, 10, 11, 12, 13, 14, 15, 16]);
SELECT vec FROM test ORDER BY id;
```

```text
┌─vec──────────────────────┐
│ [1,2,3,4,5,6,7,8]        │
│ [9,10,11,12,13,14,15,16] │
└──────────────────────────┘
```

<div id="converting-arrays-to-qbit">
  ## 배열을 QBit로 변환하기
</div>

배열 길이가 `QBit` 차원과 일치하면 `QBit`로 변환됩니다. 배열의 타입이 `QBit`의 타입과 일치할 필요는 없습니다. 숫자 타입은 모두 자동으로 해당 타입으로 변환됩니다. 따라서 기존 embeddings 컬럼을 바로 `QBit` 컬럼으로 옮길 수 있습니다:

```sql
CREATE TABLE embeddings (id UInt32, embedding Array(Float32)) ENGINE = Memory;
INSERT INTO embeddings VALUES (1, [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8]), (2, [0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1]);

CREATE TABLE vectors (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO vectors SELECT id, embedding FROM embeddings;

SELECT * FROM vectors ORDER BY id;
```

```text
┌─id─┬─vec───────────────────────────────┐
│  1 │ [0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8] │
│  2 │ [0.8,0.7,0.6,0.5,0.4,0.3,0.2,0.1] │
└────┴───────────────────────────────────┘
```

이 변환은 `CAST`를 사용해 명시적으로 수행할 수도 있습니다. 예를 들면 `CAST(embedding AS QBit(Float32, 8))`입니다.

<div id="converting-qbit-to-arrays">
  ## QBit를 배열로 변환하기
</div>

역변환은 비트 전치된 표현에서 원래 벡터를 복원하므로, `QBit`를 `배열`로 캐스팅하면 저장된 값이 반환됩니다. 이는 [`배열을 `QBit`로 변환하기`](#converting-arrays-to-qbit)의 역과정입니다:

```sql
SELECT [1, 2, 3, 4]::QBit(Float32, 4)::Array(Float32) AS vec;
```

```text
┌─vec───────┐
│ [1,2,3,4] │
└───────────┘
```

재구성된 배열은 `QBit`의 타입을 사용하며, 이후 각 요소가 요청된 배열 타입으로 변환됩니다. 따라서 `QBit(Float32, N)`에서 `Array(Float64)`로 변환하는 것처럼 타입까지 바꾸는 캐스트도 정상적으로 동작합니다.

`Array` -&gt; `QBit` -&gt; `Array` 왕복 변환은 `Int8`, `Float32`, `Float64`의 경우 손실이 없습니다. `BFloat16`의 경우에는 `BFloat16`으로 직접 변환한 결과와 동일하며, 손실되는 정밀도는 `BFloat16` 자체의 정밀도뿐입니다.

`dimension`이 8의 배수가 아니면 내부 표현에 포함된 끝부분의 패딩 요소가 제거되므로, 결과에는 항상 정확히 `dimension`개의 요소가 포함됩니다.

<div id="qbit-subcolumns">
  ## QBit 서브컬럼
</div>

`QBit`은 저장된 벡터의 개별 비트 평면에 접근할 수 있는 서브컬럼 액세스 패턴을 구현합니다. 각 비트 위치에는 `.N` 구문으로 접근할 수 있으며, 여기서 `N`은 비트 위치를 나타냅니다:

```sql
CREATE TABLE test (id UInt32, vec QBit(Float32, 8)) ENGINE = Memory;
INSERT INTO test VALUES (1, [0, 0, 0, 0, 0, 0, 0, 0]);
INSERT INTO test VALUES (1, [-0, -0, -0, -0, -0, -0, -0, -0]);
SELECT bin(vec.1) FROM test;
```

```text
┌─bin(tupleElement(vec, 1))─┐
│ 00000000                  │
│ 11111111                  │
└───────────────────────────┘
```

접근 가능한 서브컬럼 수는 요소 타입에 따라 달라지며(스트라이드가 적용된 경우에는 스트라이드 그룹 수에도 좌우됩니다. 자세한 내용은 [Strides](#strides)를 참조하십시오):

* `Int8`: 스트라이드 그룹당 8개의 서브컬럼 (1-8)
* `BFloat16`: 스트라이드 그룹당 16개의 서브컬럼 (1-16)
* `Float32`: 스트라이드 그룹당 32개의 서브컬럼 (1-32)
* `Float64`: 스트라이드 그룹당 64개의 서브컬럼 (1-64)

<div id="strides">
  ## 스트라이드
</div>

기본적으로 `QBit`는 각 비트 평면을 모든 `dimension` 차원에 걸친 단일 스트림으로 저장하므로, 검색 시 항상 전체 벡터에 대한 비트 평면 전체를 읽습니다. 선택적 `stride` 매개변수는 `dimension` 차원을 연속된 `dimension / stride`개의 그룹으로 나누고, 각 그룹의 비트 평면을 별도의 스트림에 저장합니다. 그러면 처음 `D`개 차원만 대상으로 검색할 때(`D`는 `stride`의 배수), 해당 차원을 포함하는 그룹의 스트림만 읽으면 됩니다. 이는 앞부분 차원만으로도 사용 가능한 저차원 임베딩을 구성하는 [Matryoshka embeddings](https://arxiv.org/abs/2205.13147)에서 특히 유용합니다.

```sql
CREATE TABLE test (id UInt32, vec QBit(BFloat16, 4096, 1024)) ENGINE = MergeTree ORDER BY id;
```

여기서는 4096개의 차원을 1024개씩 4개의 그룹으로 나눕니다. 서브컬럼은 그룹 우선 순서(group-major order)를 따릅니다. 즉, `BFloat16`(16개의 비트 평면)에서는 `vec.1` … `vec.16`이 첫 번째 stride 그룹(차원 1–1024)의 16개 비트 평면이고, `vec.17` … `vec.32`는 두 번째 그룹(차원 1025–2048)에 해당하며, 이후도 같은 방식입니다. 일반적으로 `vec.N`은 stride 그룹 `(N-1) / element_size`의 비트 평면 `(N-1) % element_size`를 읽습니다.

축소된 차원으로 검색을 실행하려면, 전치된 거리 함수의 네 번째 인수로 읽을 차원 수를 전달하십시오(아래 참고). 참조 벡터에는 정확히 그 개수만큼의 원소가 있어야 하며, 이 값은 `stride`의 배수여야 합니다.

<div id="vector-search-functions">
  ## 벡터 검색 함수
</div>

다음은 `QBit` 데이터 타입을 사용하는 벡터 유사도 검색용 거리 함수입니다:

* [`L2DistanceTransposed`](../functions/distance-functions.md#L2DistanceTransposed)
* [`cosineDistanceTransposed`](../functions/distance-functions.md#cosineDistanceTransposed)
* [`dotProductTransposed`](../functions/distance-functions.md#dotProductTransposed)

stride가 있는 `QBit`의 경우, 이 함수는 선택적 네 번째 인수 `used_dims`(읽을 선행 차원의 수)를 받을 수 있으며, 이 경우 해당 차원을 포함하는 stride 그룹만 읽습니다:

```sql
-- read 8 bit planes over the first 2048 of 4096 dimensions
SELECT id, L2DistanceTransposed(vec, reference_vec, 8, 2048) AS dist FROM test ORDER BY dist;
```