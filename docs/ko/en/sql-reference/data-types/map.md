---
description: 'ClickHouse의 Map 데이터 타입 문서'
sidebar_label: 'Map(K, V)'
sidebar_position: 36
slug: /sql-reference/data-types/map
title: 'Map(K, V)'
doc_type: 'reference'
---

데이터 타입 `Map(K, V)`는 key-value 쌍을 저장합니다.

다른 데이터베이스와 달리 ClickHouse의 맵은 키가 고유하지 않습니다. 즉, 하나의 맵에 동일한 키를 가진 요소가 2개 들어갈 수 있습니다.
(이는 맵이 내부적으로 `Array(Tuple(K, V))`로 구현되기 때문입니다.)

구문 `m[k]`를 사용하면 맵 `m`에서 키 `k`에 해당하는 값을 가져올 수 있습니다.
또한 `m[k]`는 맵 전체를 스캔하므로, 이 연산의 런타임은 맵 크기에 선형적으로 비례합니다.

**매개변수**

* `K` — 맵 키의 타입입니다. [Nullable](../../sql-reference/data-types/nullable.md) 및 [Nullable](../../sql-reference/data-types/nullable.md)가 중첩된 [LowCardinality](../../sql-reference/data-types/lowcardinality.md)를 제외한 임의의 타입입니다.
* `V` — 맵 값의 타입입니다. 임의의 타입입니다.

**예시**

맵 타입 컬럼이 있는 테이블을 생성합니다:

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':1, 'key2':10}), ({'key1':2,'key2':20}), ({'key1':3,'key2':30});
```

`key2` 값을 선택하려면:

```sql title="Query"
SELECT m['key2'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key2')─┐
│                      10 │
│                      20 │
│                      30 │
└─────────────────────────┘
```

요청된 키 `k`가 맵에 없으면 `m[k]`는 값 타입의 기본값을 반환합니다. 예를 들어 정수 타입은 `0`, 문자열 타입은 `''`를 반환합니다.
맵에 키가 존재하는지 확인하려면 [mapContains](/ko/sql-reference/functions/tuple-map-functions#mapContainsKey) 함수를 사용할 수 있습니다.

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE=Memory;
INSERT INTO tab VALUES ({'key1':100}), ({});
SELECT m['key1'] FROM tab;
```

```text title="Response"
┌─arrayElement(m, 'key1')─┐
│                     100 │
│                       0 │
└─────────────────────────┘
```

<div id="converting-tuple-to-map">
  ## Tuple를 Map으로 변환하기
</div>

`Tuple()` 유형의 값은 [CAST](/ko/sql-reference/functions/type-conversion-functions#CAST) 함수를 사용해 `Map()` 유형의 값으로 변환할 수 있습니다.

**예시**

```sql title="Query"
SELECT CAST(([1, 2, 3], ['Ready', 'Steady', 'Go']), 'Map(UInt8, String)') AS map;
```

```text title="Response"
┌─map───────────────────────────┐
│ {1:'Ready',2:'Steady',3:'Go'} │
└───────────────────────────────┘
```

<div id="reading-subcolumns-of-map">
  ## 맵의 서브컬럼 읽기
</div>

맵 전체를 읽지 않으려면 일부 경우에는 서브컬럼 `keys` 및 `values`를 사용할 수 있습니다.

**예시**

```sql title="Query"
CREATE TABLE tab (m Map(String, UInt64)) ENGINE = Memory;
INSERT INTO tab VALUES (map('key1', 1, 'key2', 2, 'key3', 3));

SELECT m.keys FROM tab; --   same as mapKeys(m)
SELECT m.values FROM tab; -- same as mapValues(m)
```

```text title="Response"
┌─m.keys─────────────────┐
│ ['key1','key2','key3'] │
└────────────────────────┘

┌─m.values─┐
│ [1,2,3]  │
└──────────┘
```

<div id="bucketed-map-serialization">
  ## MergeTree의 버킷 맵 직렬화
</div>

기본적으로 MergeTree의 `Map` 컬럼은 하나의 `Array(Tuple(K, V))` 스트림으로 저장됩니다.
`m['key']`로 특정 키 하나를 읽으려면, 그 키만 필요하더라도 모든 행의 모든 key-value 쌍이 포함된 전체 컬럼을 스캔해야 합니다.
서로 다른 키가 많은 맵에서는 이것이 병목이 될 수 있습니다.

버킷 직렬화(`with_buckets`)는 키를 해시하여 key-value 쌍을 여러 개의 독립적인 서브스트림(버킷)으로 나눕니다.
쿼리에서 `m['key']`에 접근하면 해당 키가 들어 있는 버킷만 디스크에서 읽고, 나머지 버킷은 모두 스키핑합니다.

<div id="enabling-bucketed-serialization">
  ### 버킷 직렬화 활성화
</div>

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

삽입이 느려지는 것을 방지하려면 0레벨 파트(`INSERT` 중 생성됨)에는 `basic` 직렬화를 유지하고, 병합된 파트에만 `with_buckets`를 사용할 수 있습니다:

```sql
CREATE TABLE tab (id UInt64, m Map(String, UInt64))
ENGINE = MergeTree ORDER BY id
SETTINGS
    map_serialization_version = 'with_buckets',
    map_serialization_version_for_zero_level_parts = 'basic',
    max_buckets_in_map = 32,
    map_buckets_strategy = 'sqrt';
```

<div id="how-it-works">
  ### 작동 방식
</div>

데이터 파트가 `with_buckets` 직렬화로 저장될 때:

1. 블록 통계에서 행당 평균 키 수를 계산합니다.
2. 구성된 전략에 따라 버킷 수를 결정합니다([설정](#bucketed-map-settings) 참조).
3. 각 키-값 쌍은 키를 해시하여 버킷에 할당됩니다: `bucket = hash(key) % num_buckets`.
4. 각 버킷은 자체 키, 값, 오프셋을 가진 독립적인 서브스트림으로 저장됩니다.
5. `buckets_info` 메타데이터 스트림에 버킷 수와 통계가 기록됩니다.

쿼리가 특정 키(`m['key']`)를 읽을 때 옵티마이저는 해당 표현식을 키 서브컬럼(`m.key_<serialized_key>`)으로 재작성합니다.
직렬화 계층은 요청된 키가 어느 버킷에 속하는지 계산하고, 해당 버킷 하나만 디스크에서 읽습니다.

전체 맵을 읽을 때(예: `SELECT m`)는 모든 버킷을 읽어 원래 맵으로 재구성합니다. 이 방식은 여러 서브스트림을 읽고 머지하는 오버헤드가 있으므로 `basic` 직렬화보다 느립니다.

:::note
`with_buckets` 직렬화를 사용하면 맵 값 내부의 키 순서가 원래 삽입 순서와 달라질 수 있습니다. 키는 해시에 따라 버킷에 분산되며, 삽입 순서가 아니라 버킷 순서대로 다시 조합됩니다. `basic` 직렬화에서는 삽입된 맵의 키 순서가 유지됩니다.
:::

버킷 수는 파트마다 다를 수 있습니다. 버킷 수가 서로 다른 파트들을 머지하면 새 파트의 버킷 수는 머지된 통계를 바탕으로 다시 계산됩니다. `basic` 및 `with_buckets` 직렬화를 사용하는 파트는 같은 테이블에 함께 존재할 수 있으며, 투명하게 머지됩니다.

<div id="bucketed-map-settings">
  ### 설정
</div>

| 설정                                               | 기본값     | 설명                                                                                                                                                                                                                          |
| ------------------------------------------------ | ------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `map_serialization_version`                      | `basic` | `Map` 컬럼의 직렬화 포맷입니다. `basic`은 단일 배열 스트림으로 저장합니다. `with_buckets`는 단일 키 읽기 성능을 높이기 위해 키를 버킷으로 나눕니다.                                                                                                                           |
| `map_serialization_version_for_zero_level_parts` | `basic` | 0레벨 파트(`INSERT`로 생성됨)의 직렬화 포맷입니다. 삽입 시 쓰기 오버헤드를 피하기 위해 `basic`을 유지하면서, 병합된 파트에는 `with_buckets`를 사용할 수 있습니다.                                                                                                                 |
| `max_buckets_in_map`                             | `32`    | 버킷 수의 상한입니다. 실제 개수는 `map_buckets_strategy`에 따라 달라집니다. 허용되는 최대값은 256입니다.                                                                                                                                                     |
| `map_buckets_strategy`                           | `sqrt`  | 평균 맵 크기에서 버킷 수를 계산하는 전략입니다: `constant` — 항상 `max_buckets_in_map`을 사용합니다. `sqrt` — `round(coefficient * sqrt(avg_size))`를 사용합니다. `linear` — `round(coefficient * avg_size)`를 사용합니다. 결과는 `[1, max_buckets_in_map]` 범위로 제한됩니다. |
| `map_buckets_coefficient`                        | `1.0`   | `sqrt` 및 `linear` 전략에 적용되는 계수입니다. 전략이 `constant`이면 무시됩니다.                                                                                                                                                                   |
| `map_buckets_min_avg_size`                       | `32`    | 버킷 분할을 활성화하기 위한 행당 평균 키 수의 최소값입니다. 평균이 이 임계값보다 낮으면 다른 설정과 관계없이 단일 버킷이 사용됩니다. 임계값을 비활성화하려면 `0`으로 설정하십시오.                                                                                                                     |

<div id="performance-trade-offs">
  ### 성능 트레이드오프
</div>

다음 표는 다양한 맵 크기(행당 10개~10,000개의 키)에서 `basic` 직렬화와 비교한 `with_buckets`의 성능 영향을 요약합니다. 버킷 수는 최대 32개로 제한하는 `sqrt` 전략으로 결정되었습니다. 정확한 수치는 키/값 타입, 데이터 분포, 하드웨어에 따라 달라집니다.

| Operation                                      | 10 keys       | 100 keys      | 1,000 keys    | 10,000 keys   | Notes                                                                                                    |
| ---------------------------------------------- | ------------- | ------------- | ------------- | ------------- | -------------------------------------------------------------------------------------------------------- |
| **Single key lookup** (`m['key']`)             | 1.6–3.2배 더 빠름 | 4.5–7.7배 더 빠름 | 16–39배 더 빠름   | 21–49배 더 빠름   | 전체 컬럼이 아니라 하나의 버킷만 읽습니다.                                                                                 |
| **5 key lookups**                              | ~1배           | 1.5–3.1배 더 빠름 | 2.9–8.3배 더 빠름 | 4.5–6.7배 더 빠름 | 각 키는 해당 버킷을 읽으며, 일부 버킷은 겹칠 수 있습니다.                                                                       |
| **PREWHERE** (`SELECT m WHERE m['key'] = ...`) | 1.5–3.0배 더 빠름 | 2.9–7.3배 더 빠름 | 5.3–31배 더 빠름  | 20–45배 더 빠름   | PREWHERE 필터는 하나의 버킷만 읽고, 전체 맵은 일치하는 행에 대해서만 읽습니다. 속도 향상은 선택도에 따라 달라집니다 — 일치하는 그래뉼이 적을수록 전체 맵 I/O가 줄어듭니다. |
| **Full map scan** (`SELECT m`)                 | ~2배 더 느림      | ~2배 더 느림      | ~2배 더 느림      | ~2배 더 느림      | 모든 버킷을 읽어 다시 재구성해야 합니다.                                                                                  |
| **INSERT**                                     | 1.5–2.5배 더 느림 | 1.5–2.5배 더 느림 | 1.5–2.5배 더 느림 | 1.5–2.5배 더 느림 | 키를 해시하고 여러 하위 스트림에 기록하는 오버헤드가 있습니다.                                                                      |

<div id="recommendations">
  ### 권장 사항
</div>

* **작은 맵(평균 키 32개 미만):** `basic` 직렬화를 유지하십시오. 작은 맵에는 버킷 분할에 따른 오버헤드가 크지 않은 이점을 위해 추가될 만큼 크지 않습니다. 기본값 `map_buckets_min_avg_size = 32`가 이를 자동으로 적용합니다.
* **중간 크기 맵(키 32–100개):** 쿼리에서 개별 키에 자주 접근한다면 `sqrt` 전략과 함께 `with_buckets`를 사용하십시오. 단일 키 조회 속도가 4–8배 빨라집니다.
* **큰 맵(키 100개 이상):** `with_buckets`를 사용하십시오. 단일 키 조회 속도가 16–49배 빨라집니다. 삽입 속도를 기준값에 가깝게 유지하려면 `map_serialization_version_for_zero_level_parts = 'basic'` 설정을 고려하십시오.
* **전체 맵 스캔이 워크로드의 대부분을 차지하는 경우:** `basic`을 유지하십시오. 버킷 직렬화는 전체 스캔에서 약 2배의 오버헤드를 추가합니다.
* **혼합 워크로드(일부는 키 조회, 일부는 전체 스캔):** zero-level 파트를 `basic`으로 설정한 상태에서 `with_buckets`를 사용하십시오. `PREWHERE` 최적화는 먼저 필터와 관련된 버킷만 읽고, 그다음 일치하는 행에 대해서만 전체 맵을 읽으므로 전체적으로 상당한 속도 향상을 제공합니다.

<div id="map-alternatives">
  ### 대체 접근 방식
</div>

버킷 기반 `Map` 직렬화가 사용 사례에 적합하지 않다면, 키 수준 접근 성능을 개선할 수 있는 두 가지 대체 접근 방식이 있습니다:

<div id="using-the-json-data-type">
  #### `JSON` 데이터 타입 사용
</div>

[JSON](/ko/sql-reference/data-types/newjson) 데이터 타입은 자주 사용되는 각 경로를 별도의 동적 서브컬럼으로 저장합니다. `max_dynamic_paths` 제한을 초과하는 경로는 [공유 데이터 구조](/ko/sql-reference/data-types/newjson#shared-data-structure)에 저장되며, 이 구조는 단일 경로 읽기를 최적화하기 위해 `advanced` 직렬화를 사용할 수 있습니다. `advanced` 직렬화에 대한 자세한 내용은 [블로그 게시물](https://clickhouse.com/blog/json-data-type-gets-even-better)을 참조하십시오.

| 측면        | 버킷을 사용하는 `Map`                                                | `JSON`                                                                                                 |
| --------- | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| 단일 키 읽기   | 하나의 버킷을 읽습니다(다른 키가 포함될 수 있음). 버킷에 있는 모든 key-value 쌍이 역직렬화됩니다. | 자주 사용되는 경로는 동적 서브컬럼에서 직접 읽습니다. 자주 사용되지 않는 경로는 공유 데이터에 저장되며, `advanced` 직렬화를 사용하면 해당 경로의 데이터만 정확히 읽습니다. |
| 값 타입      | 모든 값이 동일한 타입 `V`를 공유합니다                                       | 각 경로는 자체 타입을 가질 수 있습니다. 타입 힌트가 없는 경로는 `Dynamic`을 사용합니다.                                                |
| 스킵 인덱스 지원 | `mapKeys`/`mapValues`에 생성된 일부 인덱스 유형에서 작동합니다                  | 스킵 인덱스는 모든 경로/값에 한 번에 생성할 수 없으며, 특정 경로 서브컬럼에만 생성할 수 있습니다.                                              |
| 전체 컬럼 읽기  | 버킷을 다시 조합해야 하므로 `basic`보다 약 2배 느립니다                           | `Dynamic` 타입 인코딩과 경로 재구성으로 인한 오버헤드가 있습니다.                                                              |
| 스토리지 오버헤드 | 추가 메타데이터가 거의 없습니다                                             | `Dynamic` 타입 인코딩, 경로 이름 저장, 그리고 `advanced` 직렬화의 추가 메타데이터로 인해 더 큽니다.                                    |
| 스키마 유연성   | 테이블 생성 시 키 타입과 값 타입이 고정됩니다                                    | 완전히 동적입니다 — 키와 값 타입은 행마다 달라질 수 있습니다. 알려진 경로에는 직접 서브컬럼으로 접근할 수 있도록 타입이 지정된 경로 힌트를 선언할 수 있습니다.           |

서로 다른 키에 서로 다른 값 타입이 필요하거나, 키 집합이 행마다 크게 달라지거나, 자주 액세스하는 키를 미리 알고 있어 직접 서브컬럼 접근용 타입 지정 경로로 선언할 수 있는 경우 `JSON`을 사용하십시오.

<div id="manual-sharding-into-multiple-map-columns">
  #### 여러 맵 컬럼으로 수동 세그먼트 분할
</div>

애플리케이션에서 키 해시값을 기준으로 하나의 `Map`을 여러 컬럼으로 수동 분할할 수 있습니다:

```sql
CREATE TABLE tab (
    id UInt64,
    m0 Map(String, UInt64),
    m1 Map(String, UInt64),
    m2 Map(String, UInt64),
    m3 Map(String, UInt64)
) ENGINE = MergeTree ORDER BY id;
```

삽입 시 각 key-value 쌍을 `m{hash(key) % 4}` 컬럼으로 라우팅합니다. 쿼리 시에는 해당 컬럼에서 읽습니다: `m{hash('target_key') % 4}['target_key']`.

| 항목        | 버킷을 사용하는 `Map`             | 수동 세그먼트 분할                                 |
| --------- | --------------------------- | ------------------------------------------ |
| 사용 편의성    | 투명함 — 스토리지 엔진에서 처리됨         | 삽입 및 SELECT를 위해 애플리케이션 수준의 라우팅 로직이 필요함     |
| 수직 병합     | 지원되지 않음 — 모든 버킷이 하나의 컬럼에 속함 | 지원됨 — 각 `Map` 컬럼은 독립적인 컬럼이므로 수직으로 병합할 수 있음 |
| 스키마 변경    | 버킷 수가 각 파트별로 자동 조정됨         | 세그먼트 수를 변경하려면 데이터를 다시 쓰거나 새 컬럼을 추가해야 함     |
| 쿼리 구문     | `m['key']`를 바로 사용할 수 있음     | 올바른 컬럼을 계산해야 함: `m0['key']`, `m1['key']` 등 |
| 버킷 세분화 수준 | 파트별로 적용되며 데이터 통계에 따라 조정됨    | 테이블 생성 시 고정됨                               |

수동 세그먼트 분할은 컬럼이 많은 테이블을 머지할 때 메모리 사용량을 줄이기 위해 수직 병합이 중요한 경우, 또는 세그먼트 수를 고정해 명시적으로 제어해야 하는 경우에 유용합니다. 대부분의 사용 사례에서는 자동 버킷 직렬화가 더 간단하며 충분합니다.

**관련 항목**

* [map()](/ko/sql-reference/functions/tuple-map-functions#map) 함수
* [CAST()](/ko/sql-reference/functions/type-conversion-functions#CAST) 함수
* [`Map` 데이터 형식용 -Map combinator](../aggregate-functions/combinators.md#-map)

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse로 관측성 솔루션 구축하기 - Part 2 - 트레이스](https://clickhouse.com/blog/storing-traces-and-spans-open-telemetry-in-clickhouse)