---
description: '정확 벡터 검색 및 근사 벡터 검색 문서'
keywords: ['vector similarity search', 'ann', 'knn', 'hnsw', 'indices', 'index', 'nearest neighbor', 'vector search']
sidebar_label: '정확 벡터 검색 및 근사 벡터 검색'
slug: /engines/table-engines/mergetree-family/annindexes
title: '정확 벡터 검색 및 근사 벡터 검색'
doc_type: 'guide'
---

다차원(벡터) 공간에서 주어진 점에 가장 가까운 N개의 점을 찾는 문제를 [최근접 이웃 검색](https://en.wikipedia.org/wiki/Nearest_neighbor_search)이라고 하며, 줄여서 벡터 검색이라고 합니다.
벡터 검색을 수행하는 일반적인 접근 방식은 두 가지입니다.

* 정확 벡터 검색은 주어진 점과 벡터 공간의 모든 점 사이의 거리를 계산합니다. 이 방식은 가능한 최고 수준의 정확도를 보장하므로, 반환된 점이 실제 최근접 이웃임이 보장됩니다. 벡터 공간 전체를 완전히 탐색하므로, 정확 벡터 검색은 실제 환경에서 사용하기에는 너무 느릴 수 있습니다.
* 근사 벡터 검색은 정확 벡터 검색보다 훨씬 빠르게 결과를 계산하는 여러 기법(예: 그래프나 랜덤 포리스트와 같은 특수한 데이터 구조)을 말합니다. 결과 정확도는 일반적으로 실무에서 사용하기에 &quot;충분히 좋은&quot; 수준입니다. 많은 근사 기법은 결과 정확도와 검색 시간 사이의 절충 관계를 조정할 수 있는 매개변수를 제공합니다.

벡터 검색(정확 또는 근사)은 SQL에서 다음과 같이 작성할 수 있습니다.

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

벡터 공간의 점은 배열 타입의 컬럼 `vectors`에 저장됩니다. 예를 들면 [Array(Float64)](../../../sql-reference/data-types/array.md), [Array(Float32)](../../../sql-reference/data-types/array.md), 또는 [Array(BFloat16)](../../../sql-reference/data-types/array.md)입니다.
기준 벡터는 상수 배열이며 공통 테이블 식으로 제공됩니다.
`<DistanceFunction>`은 기준 점과 저장된 모든 점 사이의 거리를 계산합니다.
이를 위해 사용 가능한 [거리 함수](/ko/sql-reference/functions/distance-functions)를 어떤 것이든 사용할 수 있습니다.
`<N>`은 반환할 이웃의 개수를 지정합니다.

<div id="exact-nearest-neighbor-search">
  ## 정확 벡터 검색
</div>

정확 벡터 검색은 위의 SELECT 쿼리를 그대로 사용해 수행할 수 있습니다.
이러한 쿼리의 런타임은 일반적으로 저장된 벡터 수와 그 차원, 즉 배열 요소 수에 비례합니다.
또한 ClickHouse는 모든 벡터를 브루트 포스(전체 스캔)하므로, 런타임은 쿼리에서 사용하는 스레드 수에도 영향을 받습니다([max&#95;threads](../../../operations/settings/settings.md#max_threads) 설정 참조).

<div id="exact-nearest-neighbor-search-example">
  ### 예시
</div>

```sql
CREATE TABLE tab(id Int32, vec Array(Float32)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

반환값

```result
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

<div id="approximate-nearest-neighbor-search">
  ## 근사 벡터 검색
</div>

<div id="vector-similarity-index">
  ### 벡터 유사성 인덱스
</div>

ClickHouse는 근사 벡터 검색을 수행할 수 있도록 특수한 &quot;벡터 유사성&quot; 인덱스를 제공합니다.

:::note
벡터 유사성 인덱스는 ClickHouse 버전 25.8 이상에서 사용할 수 있습니다.
문제가 발생하면 [ClickHouse 리포지토리](https://github.com/clickhouse/clickhouse/issues)에 이슈를 등록해 주십시오.
:::

<div id="creating-a-vector-similarity-index">
  #### 벡터 유사도 인덱스 생성
</div>

새 테이블에 벡터 유사도 인덱스(vector similarity index)를 다음과 같이 생성할 수 있습니다:

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>]
)
ENGINE = MergeTree
ORDER BY [...]
```

또는 기존 테이블에 벡터 유사도 인덱스를 추가하는 방법은 다음과 같습니다.

```sql
ALTER TABLE table ADD INDEX <index_name> vectors TYPE vector_similarity(<type>, <distance_function>, <dimensions>) [GRANULARITY <N>];
```

벡터 유사성 인덱스는 스키핑 인덱스의 특수한 유형입니다([여기](mergetree.md#table_engine-mergetree-data_skipping-indexes) 및 [여기](../../../optimize/skipping-indexes) 참조).
따라서 위의 `ALTER TABLE` 구문은 이후에 테이블에 삽입되는 새 데이터에 대해서만 인덱스를 빌드합니다.
기존 데이터에 대해서도 인덱스를 빌드하려면 다음과 같이 구체화해야 합니다:

```sql
ALTER TABLE table MATERIALIZE INDEX <index_name> SETTINGS mutations_sync = 2;
```

`<distance_function>` 함수는 다음 중 하나여야 합니다.

* `L2Distance`, [유클리드 거리](https://en.wikipedia.org/wiki/Euclidean_distance)로, 유클리드 공간에서 두 점 사이를 잇는 선의 길이를 나타내며,
* `cosineDistance`, [코사인 거리](https://en.wikipedia.org/wiki/Cosine_similarity#Cosine_distance)로, 0이 아닌 두 벡터 사이의 각도를 나타내거나,
* `dotProduct`, [내적](https://en.wikipedia.org/wiki/Dot_product) (inner product)으로, 두 벡터의 요소별 곱을 모두 더한 값을 나타냅니다. 정규화된 데이터에서는 `cosineDistance`와 동일합니다.

정규화된(normalized) 데이터에는 `L2Distance`가 일반적으로 최선의 선택이며, 그렇지 않은 경우 스케일 차이를 보정하기 위해 `cosineDistance`를 권장합니다.

:::note
거리 함수(Distance functions) `L2Distance` 및 `cosineDistance`는 값이 작을수록 유사도가 높고, `dotProduct`는 값이 클수록 유사도가 높습니다.
따라서 `L2Distance` 및 `cosineDistance` 기반의 벡터 인덱스는 `SELECT [...] ORDER BY [...] ASC` 쿼리에서만 사용할 수 있으며(`ASC`는 `ORDER BY`의 기본값입니다), `dotProduct` 기반의 벡터 인덱스는 `SELECT [...] ORDER BY [...] DESC` 쿼리에서만 사용할 수 있습니다.
:::

`<dimensions>`는 기반 컬럼의 배열 카디널리티(요소 수)를 지정합니다.
인덱스 생성 중 ClickHouse가 카디널리티가 다른 배열을 발견하면 해당 인덱스는 폐기되고 오류가 반환됩니다.

선택적 GRANULARITY 매개변수 `<N>`은 인덱스 그래뉼의 크기를 나타냅니다([여기](../../../optimize/skipping-indexes) 참조).
기본 인덱스 세분화 수준으로 1을 사용하는 일반 스킵 인덱스와 달리, 벡터 유사성 인덱스는 기본 인덱스 세분화 수준으로 1억을 사용합니다.
이 값은 대용량 파트에서도 내부적으로 생성되는 인덱스 수를 최소화하기 위한 것입니다.
인덱스 세분화 수준 변경은 해당 작업의 영향을 충분히 이해하는 고급 사용자에게만 권장합니다([아래](#differences-to-regular-skipping-indexes) 참조).

벡터 유사성 인덱스는 다양한 근사 검색 방법을 지원하는 범용적인 구조입니다.
실제로 사용할 방법은 매개변수 `<type>`으로 지정합니다.
현재 사용 가능한 방법은 HNSW([학술 논문](https://arxiv.org/abs/1603.09320))뿐이며, 계층적 근접 그래프 기반의 근사 벡터 검색을 위한 널리 알려진 최신 기법입니다.
HNSW를 유형으로 사용하는 경우, 다음과 같은 HNSW 전용 매개변수를 선택적으로 추가 지정할 수 있습니다:

```sql
CREATE TABLE table
(
  [...],
  vectors Array(Float*),
  INDEX index_name vectors TYPE vector_similarity('hnsw', <distance_function>, <dimensions>[, <quantization>, <hnsw_max_connections_per_layer>, <hnsw_candidate_list_size_for_construction>]) [GRANULARITY N]
)
ENGINE = MergeTree
ORDER BY [...]
```

다음 HNSW 전용 매개변수를 사용할 수 있습니다:

* `<quantization>`은 근접 그래프에서 벡터의 양자화 수준을 제어합니다. 가능한 값은 `f64`, `f32`, `f16`, `bf16`, `i8`, `b1`입니다. 기본값은 `bf16`입니다. 이 매개변수는 underlying column에 저장된 벡터의 표현에는 영향을 주지 않습니다.
* `<hnsw_max_connections_per_layer>`는 그래프의 각 node에 대한 이웃 수를 제어하며, HNSW 하이퍼매개변수 `M`이라고도 합니다. 기본값은 `32`입니다. 값 `0`은 기본값을 사용함을 의미합니다.
* `<hnsw_candidate_list_size_for_construction>`는 HNSW 그래프를 구성할 때 사용하는 동적 후보 목록의 크기를 제어하며, HNSW 하이퍼매개변수 `ef_construction`이라고도 합니다. 기본값은 `128`입니다. 값 `0`은 기본값을 사용함을 의미합니다.

모든 HNSW 전용 매개변수의 기본값은 대부분의 사용 사례에서 대체로 충분히 잘 동작합니다.
따라서 HNSW 전용 매개변수를 사용자 지정하는 것은 권장하지 않습니다.

다음과 같은 추가 제한 사항이 있습니다.

* 벡터 유사성 인덱스는 [Array(Float32)](../../../sql-reference/data-types/array.md), [Array(Float64)](../../../sql-reference/data-types/array.md), 또는 [Array(BFloat16)](../../../sql-reference/data-types/array.md) 타입의 컬럼에만 생성할 수 있습니다. `Array(Nullable(Float32))` 및 `Array(LowCardinality(Float32))`와 같은 널 허용 및 LowCardinality float 배열은 허용되지 않습니다.
* 벡터 유사성 인덱스는 단일 컬럼에만 생성해야 합니다.
* 벡터 유사성 인덱스는 계산된 표현식(예: `INDEX index_name arraySort(vectors) TYPE vector_similarity([...])`)에 생성할 수 있지만, 이러한 인덱스는 이후 근사 최근접 이웃 검색에 사용할 수 없습니다.
* 벡터 유사성 인덱스를 사용하려면 기반이 되는 컬럼의 모든 배열이 `<dimension>`개의 요소를 가져야 하며, 이 조건은 인덱스 생성 중에 검사됩니다. 이 요구 사항 위반을 가능한 한 조기에 감지하려면 벡터 컬럼에 [제약 조건(constraint)](/ko/sql-reference/statements/create/table.md#constraints)을 추가할 수 있습니다. 예: `CONSTRAINT same_length CHECK length(vectors) = 256`.
* 마찬가지로, 기반이 되는 컬럼의 배열 값은 비어 있으면 안 되며(`[]`), 기본값(이 경우에도 `[]`)이어도 안 됩니다.

**스토리지 및 메모리 사용량 추정**

일반적인 AI 모델(예: 대규모 언어 모델, [LLMs](https://en.wikipedia.org/wiki/Large_language_model))과 함께 사용하기 위해 생성된 벡터는 수백 개에서 수천 개의 부동소수점 값으로 구성됩니다.
따라서 단일 벡터 값 하나만으로도 메모리를 수 킬로바이트 사용할 수 있습니다.
테이블에서 기반이 되는 벡터 컬럼에 필요한 스토리지와 벡터 유사성 인덱스에 필요한 주 메모리를 추정하려는 경우, 아래의 두 공식을 사용할 수 있습니다.

테이블의 벡터 컬럼 스토리지 사용량(비압축):

```text
Storage consumption = Number of vectors * Dimension * Size of column data type
```

[dbpedia 데이터셋](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) 예시:

```text
Storage consumption = 1 million * 1536 * 4 (for Float32) = 6.1 GB
```

검색을 수행하려면 벡터 유사성 인덱스 전체를 디스크에서 주 메모리로 로드해야 합니다.
마찬가지로 벡터 인덱스도 메모리에서 전체를 구성한 다음 디스크에 저장됩니다.

벡터 인덱스를 로드하는 데 필요한 메모리 사용량:

```text
Memory for vectors in the index (mv) = Number of vectors * Dimension * Size of quantized data type
Memory for in-memory graph (mg) = Number of vectors * hnsw_max_connections_per_layer * Bytes_per_node_id (= 4) * Layer_node_repetition_factor (= 2)

Memory consumption: mv + mg
```

[dbpedia 데이터셋](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M) 예시는 다음과 같습니다:

```text
Memory for vectors in the index (mv) = 1 million * 1536 * 2 (for BFloat16) = 3072 MB
Memory for in-memory graph (mg) = 1 million * 64 * 2 * 4 = 512 MB

Memory consumption = 3072 + 512 = 3584 MB
```

위 공식은 벡터 유사성 인덱스가 사전 할당된 버퍼와 캐시 같은 런타임 데이터 구조를 할당하는 데 추가로 필요한 메모리는 반영하지 않습니다.

<div id="using-a-vector-similarity-index">
  #### 벡터 유사성 인덱스 사용하기
</div>

:::note
벡터 유사성 인덱스를 사용하려면 [compatibility](../../../operations/settings/settings.md) 설정이 `''`(기본값) 또는 `'25.1'` 이상이어야 합니다.
:::

벡터 유사성 인덱스는 다음 형식의 SELECT 쿼리를 지원합니다.

```sql
WITH [...] AS reference_vector
SELECT [...]
FROM table
WHERE [...] -- a WHERE clause is optional
ORDER BY <DistanceFunction>(vectors, reference_vector)
LIMIT <N>
```

ClickHouse의 쿼리 최적화기(query optimizer)는 위의 쿼리 템플릿에 맞춰 사용 가능한 벡터 유사성 인덱스를 활용하려고 합니다.
SELECT 쿼리의 거리 함수가 인덱스 정의의 거리 함수와 동일할 때만 벡터 유사성 인덱스를 사용할 수 있습니다.

고급 사용자는 검색 중 후보 목록 크기를 조정하기 위해 설정 [hnsw&#95;candidate&#95;list&#95;size&#95;for&#95;search](../../../operations/settings/settings.md#hnsw_candidate_list_size_for_search)(HNSW 하이퍼매개변수 &quot;ef&#95;search&quot;라고도 함)에 사용자 지정 값을 지정할 수 있습니다(예: `SELECT [...] SETTINGS hnsw_candidate_list_size_for_search = <value>`).
이 설정의 기본값인 256은 대부분의 사용 사례에서 잘 작동합니다.
설정값이 클수록 정확도는 높아지지만 성능은 느려집니다.

쿼리가 벡터 유사성 인덱스를 사용할 수 있는 경우, ClickHouse는 SELECT 쿼리에 지정된 LIMIT `<N>`이 합리적인 범위 내에 있는지 확인합니다.
구체적으로는, `<N>`이 기본값이 100인 설정 [max&#95;limit&#95;for&#95;vector&#95;search&#95;queries](../../../operations/settings/settings.md#max_limit_for_vector_search_queries) 값보다 크면 오류가 반환됩니다.
LIMIT 값이 너무 크면 검색이 느려질 수 있으며, 대개 사용상의 오류를 의미합니다.

SELECT 쿼리가 벡터 유사성 인덱스를 사용하는지 확인하려면 쿼리 앞에 `EXPLAIN indexes = 1`를 붙일 수 있습니다.

예시로, 쿼리

```sql
EXPLAIN indexes = 1
WITH [0.462, 0.084, ..., -0.110] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 10;
```

반환할 수 있습니다

```result
    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                      │
 2. │   Limit (preliminary LIMIT (without OFFSET))                                                    │
 3. │     Sorting (Sorting for ORDER BY)                                                              │
 4. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers))) │
 5. │         ReadFromMergeTree (default.tab)                                                         │
 6. │         Indexes:                                                                                │
 7. │           PrimaryKey                                                                            │
 8. │             Condition: true                                                                     │
 9. │             Parts: 1/1                                                                          │
10. │             Granules: 575/575                                                                   │
11. │           Skip                                                                                  │
12. │             Name: idx                                                                           │
13. │             Description: vector_similarity GRANULARITY 100000000                                │
14. │             Parts: 1/1                                                                          │
15. │             Granules: 10/575                                                                    │
    └─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

이 예시에서는 [dbpedia 데이터셋](https://huggingface.co/datasets/KShivendu/dbpedia-entities-openai-1M)의 100만 개 벡터(각 벡터의 차원은 1536)를 575개의 그래뉼에 저장합니다. 즉, 그래뉼당 1.7k 행입니다.
이 쿼리는 10개의 이웃을 요청하며, 벡터 유사성 인덱스는 이 10개의 이웃을 서로 다른 10개의 그래뉼에서 찾습니다.
이 10개의 그래뉼은 쿼리 실행 중 읽힙니다.

출력에 `Skip`과 벡터 인덱스의 이름 및 유형(이 예시에서는 `idx` 및 `vector_similarity`)이 포함되면 벡터 유사성 인덱스가 사용된 것입니다.
이 경우 벡터 유사성 인덱스는 4개의 그래뉼 중 2개를 제외했으며, 즉 데이터의 50%를 제외했습니다.
제외할 수 있는 그래뉼이 많을수록 인덱스 활용 효과가 커집니다.

:::tip
인덱스 사용을 강제하려면 [force&#95;data&#95;skipping&#95;indexes](../../../operations/settings/settings#force_data_skipping_indices) 설정을 사용해 SELECT 쿼리를 실행할 수 있습니다(설정 값으로 인덱스 이름을 제공).
:::

**포스트필터링과 프리필터링**

사용자는 SELECT 쿼리에 추가 필터 조건이 포함된 `WHERE` 절을 선택적으로 지정할 수 있습니다.
ClickHouse는 포스트필터링 또는 프리필터링 전략을 사용해 이러한 필터 조건을 평가합니다.
간단히 말해, 두 전략은 필터를 어떤 순서로 평가할지 결정합니다.

* 포스트필터링은 먼저 벡터 유사성 인덱스를 평가한 다음, ClickHouse가 `WHERE` 절에 지정된 추가 필터를 평가하는 방식입니다.
* 프리필터링은 필터 평가 순서가 그 반대인 방식입니다.

이 전략들은 서로 다른 트레이드오프가 있습니다:

* 포스트필터링의 일반적인 문제는 `LIMIT <N>` 절에서 요청한 행 수보다 더 적은 수의 행을 반환할 수 있다는 점입니다. 이런 상황은 벡터 유사성 인덱스가 반환한 결과 행 중 하나 이상이 추가 필터를 만족하지 못할 때 발생합니다.
* 프리필터링은 일반적으로 아직 해결되지 않은 문제입니다. 일부 특화된 벡터 데이터베이스는 프리필터링 알고리즘을 제공하지만, 대부분의 관계형 데이터베이스(ClickHouse 포함)는 정확한 최근접 이웃 탐색, 즉 인덱스 없는 브루트 포스(전체 스캔)으로 폴백됩니다.

어떤 전략을 사용할지는 필터 조건에 따라 달라집니다.

*추가 필터가 파티션 키의 일부인 경우*

추가 필터 조건이 파티션 키의 일부이면 ClickHouse는 파티션 프루닝을 적용합니다.
예시로, 테이블이 `year` 컬럼을 기준으로 범위 파티셔닝되어 있고 다음 쿼리를 실행한다고 가정합니다:

```sql
WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
WHERE year = 2025
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

ClickHouse는 2025년 파티션만 남기고 나머지 모든 파티션을 가지치기합니다.

*추가 필터는 인덱스를 사용해 평가할 수 없습니다*

추가 필터 조건을 인덱스(프라이머리 키 인덱스(primary key index), 스키핑 인덱스(skipping index))를 사용해 평가할 수 없는 경우, ClickHouse는 포스트필터링을 적용합니다.

*추가 필터는 프라이머리 키 인덱스를 사용해 평가할 수 있습니다*

추가 필터 조건을 [프라이머리 키](mergetree.md#primary-key)로 평가할 수 있는 경우(즉, 프라이머리 키의 접두사를 이루는 경우)에는 다음과 같습니다.

* 필터 조건이 파트 내에서 적어도 1개의 행을 제거하면, ClickHouse는 해당 파트에서 &quot;남아 있는&quot; 범위에 대해 프리필터링으로 폴백합니다.
* 필터 조건이 파트 내에서 어떤 행도 제거하지 않으면, ClickHouse는 해당 파트에 대해 포스트필터링을 수행합니다.

실제 사용 사례에서는 후자의 경우가 발생할 가능성은 비교적 낮습니다.

*추가 필터는 스키핑 인덱스를 사용해 평가할 수 있습니다*

추가 필터 조건을 [스키핑 인덱스](mergetree.md#table_engine-mergetree-data_skipping-indexes)(minmax index, set index 등)를 사용해 평가할 수 있는 경우, ClickHouse는 포스트필터링을 수행합니다.
이 경우 다른 스키핑 인덱스보다 더 많은 행을 제거할 것으로 예상되는 벡터 유사성 인덱스가 먼저 평가됩니다.

포스트필터링과 프리필터링을 더 세밀하게 제어하려면 두 가지 설정을 사용할 수 있습니다.

설정 [vector&#95;search&#95;filter&#95;strategy](../../../operations/settings/settings#vector_search_filter_strategy)(기본값: 위 휴리스틱을 구현하는 `auto`)은 `prefilter`로 설정할 수 있습니다.
이는 추가 필터 조건의 선택도가 매우 높은 경우 프리필터링을 강제할 때 유용합니다.
예를 들어, 다음 쿼리는 프리필터링의 이점을 얻을 수 있습니다:

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
```

가격이 2달러 미만인 책이 극히 적다고 가정하면, 포스트필터링은 0개의 행을 반환할 수 있습니다. 벡터 인덱스가 반환한 상위 10개의 일치 항목이 모두 2달러를 초과하는 가격일 수 있기 때문입니다.
프리필터링을 강제하면(쿼리에 `SETTINGS vector_search_filter_strategy = 'prefilter'`를 추가), ClickHouse는 먼저 가격이 2달러 미만인 모든 책을 찾은 다음, 찾은 책을 대상으로 브루트 포스(전체 스캔) 벡터 검색을 실행합니다.

위 문제를 해결하는 또 다른 방법으로, [vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier)를 `1.0`보다 큰 값(예: `2.0`)으로 설정할 수 있습니다(기본값: `1.0`, 최댓값: `1000.0`).
벡터 인덱스에서 가져오는 최근접 이웃 수에 이 설정값을 곱한 뒤, 해당 행에 추가 필터를 적용하여 LIMIT 개수만큼의 행을 반환합니다.
예시로, multiplier를 `3.0`으로 설정해 다시 쿼리할 수 있습니다:

```sql
SELECT bookid, author, title
FROM books
WHERE price < 2.00
ORDER BY cosineDistance(book_vector, getEmbedding('Books on ancient Asian empires'))
LIMIT 10
SETTING vector_search_index_fetch_multiplier = 3.0;
```

ClickHouse는 각 파트의 벡터 인덱스에서 3.0 x 10 = 30개의 최근접 이웃을 가져온 후, 추가 필터를 평가합니다.
가장 가까운 이웃 10개만 반환됩니다.
`vector_search_index_fetch_multiplier`를 설정하면 이 문제를 완화할 수 있지만, 극단적인 경우(WHERE 조건의 선택성이 매우 높은 경우)에는 요청한 N개의 행보다 적은 수의 행만 반환될 수도 있습니다.

**재점수화**

ClickHouse의 스킵 인덱스는 일반적으로 그래뉼 수준에서 필터링합니다. 즉, 스킵 인덱스를 조회하면(내부적으로) 잠재적으로 일치할 수 있는 그래뉼 목록이 반환되며, 그 결과 후속 스캔에서 읽어야 하는 데이터 양이 줄어듭니다.
이 방식은 일반적인 스킵 인덱스에는 효과적이지만, 벡터 유사성 인덱스에서는 &quot;세분화 수준 불일치&quot;를 초래합니다.
좀 더 자세히 설명하면, 벡터 유사성 인덱스는 주어진 기준 벡터에 대해 가장 유사한 N개 벡터의 행 번호를 찾지만, 이후 이 행 번호를 그래뉼 번호로 환산해야 합니다.
그러면 ClickHouse는 디스크에서 해당 그래뉼을 로드한 뒤, 그 그래뉼에 포함된 모든 벡터에 대해 거리 계산을 다시 수행합니다.
이 단계를 재점수화라고 하며, 이론적으로는 정확도를 높일 수 있지만 벡터 유사성 인덱스가 *근사* 결과만 반환한다는 점을 감안하더라도 성능 측면에서는 분명히 최적이 아닙니다.

따라서 ClickHouse는 재점수화를 비활성화하고, 가장 유사한 벡터와 해당 거리값을 인덱스에서 직접 반환하는 최적화를 제공합니다.
이 최적화는 기본적으로 활성화되어 있습니다. 자세한 내용은 설정 [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring)을 참조하십시오.
개략적으로 설명하면, ClickHouse는 가장 유사한 벡터와 해당 거리값을 가상 컬럼 `_distances`를 통해 제공합니다.
이를 확인하려면 `EXPLAIN header = 1`과 함께 벡터 검색 쿼리를 실행하십시오:

```sql
EXPLAIN header = 1
WITH [0., 2.] AS reference_vec
SELECT id
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3
SETTINGS vector_search_with_rescoring = 0
```

```result
Query id: a2a9d0c8-a525-45c1-96ca-c5a11fa66f47

    ┌─explain─────────────────────────────────────────────────────────────────────────────────────────────────┐
 1. │ Expression (Project names)                                                                              │
 2. │ Header: id Int32                                                                                        │
 3. │   Limit (preliminary LIMIT (without OFFSET))                                                            │
 4. │   Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64     │
 5. │           __table1.id Int32                                                                             │
 6. │     Sorting (Sorting for ORDER BY)                                                                      │
 7. │     Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64   │
 8. │             __table1.id Int32                                                                           │
 9. │       Expression ((Before ORDER BY + (Projection + Change column names to column identifiers)))         │
10. │       Header: L2Distance(__table1.vec, _CAST([0., 2.]_Array(Float64), 'Array(Float64)'_String)) Float64 │
11. │               __table1.id Int32                                                                         │
12. │         ReadFromMergeTree (default.tab)                                                                 │
13. │         Header: id Int32                                                                                │
14. │                 _distance Float32                                                                       │
    └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

:::note
재점수화 없이(`vector_search_with_rescoring = 0`) 실행한 쿼리라도 병렬 레플리카가 활성화된 경우 재점수화로 폴백될 수 있습니다.
:::

<div id="performance-tuning">
  #### 성능 튜닝
</div>

**압축 튜닝**

거의 모든 사용 사례에서 원본 컬럼의 벡터는 밀집되어 있어 압축 효율이 좋지 않습니다.
그 결과, [압축](/ko/sql-reference/statements/create/table.md#column_compression_codec)을 사용하면 벡터 컬럼에 대한 삽입 및 읽기 성능이 저하됩니다.
따라서 압축을 비활성화하는 것이 좋습니다.
비활성화하려면 다음과 같이 벡터 컬럼에 `CODEC(NONE)`을 지정하십시오:

```sql
CREATE TABLE tab(id Int32, vec Array(Float32) CODEC(NONE), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;
```

**인덱스 생성 튜닝**

벡터 유사성 인덱스의 수명 주기는 파트의 수명 주기와 연결됩니다.
즉, 벡터 유사성 인덱스가 정의된 새 파트가 생성될 때마다 해당 인덱스도 함께 생성됩니다.
이는 일반적으로 데이터가 [삽입](https://clickhouse.com/docs/guides/inserting-data)될 때 또는 [머지](https://clickhouse.com/docs/merges) 중에 발생합니다.
안타깝게도 HNSW는 인덱스 생성에 시간이 오래 걸리는 것으로 알려져 있어, 삽입과 머지 속도를 크게 떨어뜨릴 수 있습니다.
벡터 유사성 인덱스는 데이터가 불변이거나 거의 변경되지 않을 때만 사용하는 것이 가장 바람직합니다.

인덱스 생성을 가속하려면 다음 기법을 사용할 수 있습니다:

첫째, 인덱스 생성을 병렬화할 수 있습니다.
인덱스 생성에 사용할 최대 스레드 수는 서버 설정 [max&#95;build&#95;vector&#95;similarity&#95;index&#95;thread&#95;pool&#95;size](/ko/operations/server-configuration-parameters/settings#max_build_vector_similarity_index_thread_pool_size)로 구성할 수 있습니다.
최적의 성능을 위해 이 설정값은 CPU 코어 수에 맞춰 설정해야 합니다.

둘째, INSERT SQL 문의 속도를 높이기 위해 세션 설정 [materialize&#95;skip&#95;indexes&#95;on&#95;insert](../../../operations/settings/settings.md#materialize_skip_indexes_on_insert)를 사용하여 새로 삽입된 파트에서 스키핑 인덱스 생성을 비활성화할 수 있습니다.
이러한 파트에 대한 SELECT 쿼리는 정확 검색으로 폴백됩니다.
삽입된 파트는 전체 테이블 크기에 비해 대체로 작으므로, 이로 인한 성능 영향은 무시할 수 있는 수준일 것으로 예상됩니다.

셋째, 머지 속도를 높이기 위해 세션 설정 [materialize&#95;skip&#95;indexes&#95;on&#95;merge](../../../operations/settings/merge-tree-settings.md#materialize_skip_indexes_on_merge)를 사용하여 병합된 파트에서 스키핑 인덱스 생성을 비활성화할 수 있습니다.
이와 함께 SQL 문 [ALTER TABLE [...] MATERIALIZE INDEX [...]](../../../sql-reference/statements/alter/skipping-index.md#materialize-index)을 사용하면 벡터 유사성 인덱스의 수명 주기를 명시적으로 제어할 수 있습니다.
예를 들어, 모든 데이터가 수집될 때까지 또는 주말처럼 시스템 부하가 낮은 시점까지 인덱스 생성을 미룰 수 있습니다.

**인덱스 사용 튜닝**

SELECT 쿼리가 벡터 유사성 인덱스를 사용하려면 이를 주 메모리에 로드해야 합니다.
동일한 벡터 유사성 인덱스를 주 메모리에 반복해서 로드하지 않도록, ClickHouse는 이러한 인덱스를 위한 전용 인메모리 캐시를 제공합니다.
이 캐시가 클수록 불필요한 로드 발생은 줄어듭니다.
캐시의 최대 크기는 서버 설정 [vector&#95;similarity&#95;index&#95;cache&#95;size](../../../operations/server-configuration-parameters/settings.md#vector_similarity_index_cache_size)로 구성할 수 있습니다.
기본적으로 이 캐시는 최대 5 GB까지 커질 수 있습니다.

다음 로그 메시지(`system.text_log`)는 벡터 유사성 인덱스가 로드되고 있음을 나타냅니다.
이러한 메시지가 서로 다른 벡터 검색 쿼리에서 반복적으로 나타난다면, 이는 캐시 크기가 너무 작다는 뜻입니다.

```text
2026-02-03 07:39:10.351635 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Start loading vector similarity index

<...>

2026-02-03 07:40:25.217603 [1386] f0ac5c85-1b1c-4f35-8848-87a1d1aa00ba : VectorSimilarityIndex Loaded vector similarity index: max_level = 2, connectivity = 64, size = 1808111, capacity = 1808111, memory_usage = 8.00 GiB, bytes_per_vector = 4096, scalar_words = 1024, nodes = 1808111, edges = 51356964, max_edges = 233395072
```

:::note
벡터 유사성 인덱스 캐시는 벡터 인덱스 그래뉼을 저장합니다.
개별 벡터 인덱스 그래뉼의 크기가 캐시 크기보다 크면 캐시되지 않습니다.
따라서 「스토리지 및 메모리 활용 추정」의 공식 또는 [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices)를 기준으로 벡터 인덱스 크기를 계산하고, 그에 맞춰 캐시 크기를 설정하십시오.
:::

*느린 벡터 검색 쿼리를 조사할 때는 먼저 벡터 인덱스 캐시를 확인하고, 필요하면 크기를 늘려야 한다는 점을 다시 한번 강조합니다.*

현재 벡터 유사성 인덱스 캐시의 크기는 [system.metrics](../../../operations/system-tables/metrics.md)에서 확인할 수 있습니다.

```sql
SELECT metric, value
FROM system.metrics
WHERE metric = 'VectorSimilarityIndexCacheBytes'
```

특정 Query id를 가진 쿼리의 캐시 적중 및 미적중은 [system.query&#95;log](../../../operations/system-tables/query_log.md)에서 확인할 수 있습니다:

```sql
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['VectorSimilarityIndexCacheHits'], ProfileEvents['VectorSimilarityIndexCacheMisses']
FROM system.query_log
WHERE type = 'QueryFinish' AND query_id = '<...>'
ORDER BY event_time_microseconds;
```

프로덕션 환경에서는 모든 벡터 인덱스가 항상 메모리에 상주할 수 있도록 캐시 크기를 충분히 크게 설정하는 것이 좋습니다.

**양자화 조정**

[양자화](https://huggingface.co/blog/embedding-quantization)는 벡터의 메모리 사용량과 벡터 인덱스를 구축하고 탐색하는 데 드는 계산 비용을 줄이는 기법입니다.
ClickHouse 벡터 인덱스는 다음과 같은 양자화 옵션을 지원합니다.

| 양자화            | 이름                | 차원당 저장 공간 |
| -------------- | ----------------- | --------- |
| f32            | 단정밀도              | 4바이트      |
| f16            | 반정밀도              | 2바이트      |
| bf16 (default) | 반정밀도(brain float) | 2바이트      |
| i8             | 1/4 정밀도           | 1바이트      |
| b1             | 이진                | 1비트       |

양자화는 원래의 전체 정밀도 부동소수점 값(`f32`)으로 검색할 때와 비교하면 벡터 검색의 정밀도를 낮춥니다.
하지만 대부분의 데이터셋에서는 반정밀도 brain float 양자화(`bf16`)로 인한 정밀도 손실이 무시할 만한 수준이므로, 벡터 유사성 인덱스는 기본적으로 이 양자화 기법을 사용합니다.
1/4 정밀도(`i8`)와 이진(`b1`) 양자화는 벡터 검색에서 눈에 띄는 정밀도 손실을 초래합니다.
이 두 양자화는 벡터 유사성 인덱스의 크기가 사용 가능한 DRAM 크기보다 현저히 큰 경우에만 권장합니다.
이 경우 정확도를 높이기 위해 rescoring([vector&#95;search&#95;index&#95;fetch&#95;multiplier](../../../operations/settings/settings#vector_search_index_fetch_multiplier), [vector&#95;search&#95;with&#95;rescoring](../../../operations/settings/settings#vector_search_with_rescoring))을 활성화하는 것도 권장합니다.
이진 양자화는 1) 정규화된 임베딩(즉, 벡터 길이 = 1이며 OpenAI 모델은 일반적으로 정규화되어 있음)이고, 2) 거리 함수로 cosine distance를 사용하는 경우에만 권장합니다.
이진 양자화는 내부적으로 Hamming distance를 사용해 근접성 그래프를 구성하고 검색합니다.
rescoring 단계에서는 테이블에 저장된 원래의 전체 정밀도 벡터를 사용해 cosine distance로 최근접 이웃을 식별합니다.

**데이터 전송 조정**

벡터 검색 쿼리의 기준 벡터는 사용자가 제공하며, 일반적으로 Large Language Model(LLM)을 호출해 가져옵니다.
ClickHouse에서 벡터 검색을 실행하는 일반적인 Python 코드는 다음과 같습니다

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'search_v': search_v}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, %(search_v)s)
    LIMIT 10",
    parameters = params)
```

임베딩 벡터(위 스니펫의 `search_v`)는 차원이 매우 클 수 있습니다.
예를 들어 OpenAI는 1536차원, 심지어 3072차원의 임베딩 벡터를 생성하는 모델을 제공합니다.
위 코드에서는 ClickHouse Python 드라이버가 임베딩 벡터를 사람이 읽을 수 있는 문자열로 치환한 뒤, SELECT 쿼리 전체를 문자열로 전송합니다.
임베딩 벡터가 1536개의 단정밀도 부동소수점 값으로 구성되어 있다고 가정하면, 전송되는 문자열의 길이는 20 kB에 달합니다.
그 결과 토큰화, 파싱, 그리고 수천 번의 문자열-부동소수점 변환을 처리하느라 CPU 사용량이 크게 증가합니다.
또한 ClickHouse 서버 로그 파일에도 상당한 공간이 필요하며, `system.query_log` 역시 비대해집니다.

대부분의 LLM 모델은 임베딩 벡터를 네이티브 float 목록 또는 NumPy 배열로 반환합니다.
따라서 Python 애플리케이션에서는 다음과 같은 방식으로 참조 벡터 매개변수를 바이너리 형식으로 바인딩하는 것을 권장합니다:

```python
search_v = openai_client.embeddings.create(input = "[Good Books]", model='text-embedding-3-large', dimensions=1536).data[0].embedding

params = {'$search_v_binary$': np.array(search_v, dtype=np.float32).tobytes()}
result = chclient.query(
   "SELECT id FROM items
    ORDER BY cosineDistance(vector, reinterpret($search_v_binary$, 'Array(Float32)'))
    LIMIT 10"
    parameters = params)
```

이 예시에서는 기준 벡터를 바이너리 형식 그대로 전송한 뒤, 서버에서 실수 배열로 재해석합니다.
이렇게 하면 서버 측 CPU 시간을 절약할 수 있고, 서버 로그와 `system.query_log`가 불필요하게 커지는 것도 방지할 수 있습니다.

<div id="administration">
  #### 관리 및 모니터링
</div>

벡터 유사성 인덱스의 디스크 사용량은 [system.data&#95;skipping&#95;indices](../../../operations/system-tables/data_skipping_indices)에서 확인할 수 있습니다:

```sql
SELECT database, table, name, formatReadableSize(data_compressed_bytes)
FROM system.data_skipping_indices
WHERE type = 'vector_similarity';
```

출력 예시:

```result
┌─database─┬─table─┬─name─┬─formatReadab⋯ssed_bytes)─┐
│ default  │ tab   │ idx  │ 348.00 MB                │
└──────────┴───────┴──────┴──────────────────────────┘
```

<div id="differences-to-regular-skipping-indexes">
  #### 일반 스키핑 인덱스와의 차이점
</div>

모든 일반 [스키핑 인덱스](/ko/optimize/skipping-indexes)와 마찬가지로 벡터 유사성 인덱스도 그래뉼을 기준으로 구성되며, 각 인덱싱된 블록은 `GRANULARITY = [N]`개의 그래뉼로 이루어집니다(일반 스키핑 인덱스의 기본값은 `[N]` = 1).
예를 들어 테이블의 프라이머리 인덱스 세분화 수준이 8192이고(설정 `index_granularity = 8192`) `GRANULARITY = 2`이면, 각 인덱싱된 블록에는 16384개의 행이 포함됩니다.
하지만 근사 최근접 이웃 검색을 위한 데이터 구조와 알고리즘은 본질적으로 행 지향입니다.
이들은 행 집합을 압축된 형태로 저장하고, 벡터 검색 쿼리에 대해서도 행을 반환합니다.
이 때문에 벡터 유사성 인덱스는 일반 스키핑 인덱스와 비교했을 때 동작 방식에 다소 직관적이지 않은 차이가 있습니다.

사용자가 컬럼에 벡터 유사성 인덱스를 정의하면 ClickHouse는 내부적으로 각 인덱스 블록에 대해 벡터 유사성 &quot;서브 인덱스&quot;를 생성합니다.
이 서브 인덱스는 자신이 속한 인덱스 블록의 행만 알고 있다는 점에서 &quot;로컬&quot;입니다.
앞선 예시에서 컬럼에 65536개의 행이 있다고 가정하면, 4개의 인덱스 블록(8개의 그래뉼에 걸쳐 있음)과 각 인덱스 블록별 벡터 유사성 서브 인덱스 1개가 생성됩니다.
이론적으로 서브 인덱스는 자신의 인덱스 블록 내에서 가장 가까운 N개의 포인트에 해당하는 행을 직접 반환할 수 있습니다.
하지만 ClickHouse는 그래뉼 단위로 디스크에서 메모리로 데이터를 적재하므로, 서브 인덱스는 일치하는 행을 그래뉼 단위로 확장해 반환합니다.
이는 인덱스 블록 단위로 데이터를 스키핑하는 일반 스키핑 인덱스와 다릅니다.

`GRANULARITY` 매개변수는 생성되는 벡터 유사성 서브 인덱스의 수를 결정합니다.
`GRANULARITY` 값이 클수록 벡터 유사성 서브 인덱스의 수는 줄어들고 크기는 커지며, 컬럼(또는 컬럼의 데이터 파트)에 서브 인덱스가 하나만 있는 수준까지 이를 수 있습니다.
이 경우 해당 서브 인덱스는 모든 컬럼 행에 대한 &quot;전역&quot; 관점을 가지며, 관련 행이 있는 컬럼(파트)의 모든 그래뉼을 직접 반환할 수 있습니다(이러한 그래뉼은 최대 `LIMIT [N]`개입니다).
두 번째 단계에서 ClickHouse는 이 그래뉼들을 적재하고, 해당 그래뉼의 모든 행에 대해 brute-force 거리 계산을 수행하여 실제로 가장 적합한 행을 식별합니다.
`GRANULARITY` 값이 작으면 각 서브 인덱스가 최대 `LIMIT N`개의 그래뉼을 반환합니다.
그 결과 더 많은 그래뉼을 적재하고 후처리 필터링해야 합니다.
검색 정확도는 두 경우 모두 동일하게 우수하며, 차이가 나는 것은 처리 성능뿐이라는 점에 유의하십시오.
일반적으로 벡터 유사성 인덱스에는 큰 `GRANULARITY`를 사용하는 것이 권장되며, 벡터 유사성 구조의 과도한 메모리 활용과 같은 문제가 발생할 때만 더 작은 `GRANULARITY` 값으로 폴백하는 것이 좋습니다.
벡터 유사성 인덱스에 `GRANULARITY`를 지정하지 않으면 기본값은 1억입니다.

<div id="approximate-nearest-neighbor-search-example">
  #### 예시
</div>

쿼리:

```sql title="Query"
CREATE TABLE tab(id Int32, vec Array(Float32), INDEX idx vec TYPE vector_similarity('hnsw', 'L2Distance', 2)) ENGINE = MergeTree ORDER BY id;

INSERT INTO tab VALUES (0, [1.0, 0.0]), (1, [1.1, 0.0]), (2, [1.2, 0.0]), (3, [1.3, 0.0]), (4, [1.4, 0.0]), (5, [1.5, 0.0]), (6, [0.0, 2.0]), (7, [0.0, 2.1]), (8, [0.0, 2.2]), (9, [0.0, 2.3]), (10, [0.0, 2.4]), (11, [0.0, 2.5]);

WITH [0., 2.] AS reference_vec
SELECT id, vec
FROM tab
ORDER BY L2Distance(vec, reference_vec) ASC
LIMIT 3;
```

```result title="Response"
   ┌─id─┬─vec─────┐
1. │  6 │ [0,2]   │
2. │  7 │ [0,2.1] │
3. │  8 │ [0,2.2] │
   └────┴─────────┘
```

근사 벡터 검색을 사용하는 다른 예시 데이터셋:

* [LAION-400M](../../../getting-started/example-datasets/laion-400m-dataset)
* [LAION-5B](../../../getting-started/example-datasets/laion-5b-dataset)
* [dbpedia](../../../getting-started/example-datasets/dbpedia-dataset)
* [hackernews](../../../getting-started/example-datasets/hackernews-vector-search-dataset)

<div id="approximate-nearest-neighbor-search-qbit">
  ### Quantized Bit (QBit)
</div>

정확 벡터 검색의 속도를 높이는 일반적인 방법 중 하나는 더 낮은 정밀도의 [부동소수점 데이터 타입](../../../sql-reference/data-types/float.md)을 사용하는 것입니다.
예를 들어, 벡터를 `Array(Float32)` 대신 `Array(BFloat16)`로 저장하면 데이터 크기가 절반으로 줄어들고, 쿼리 런타임도 그에 비례해 감소할 것으로 예상됩니다.
이 방법을 양자화(quantization)라고 합니다. 이 방식은 계산 속도를 높이지만, 모든 벡터를 빠짐없이 스캔하더라도 결과 정확도가 낮아질 수 있습니다.

기존 양자화 방식에서는 검색할 때뿐 아니라 데이터를 저장할 때도 정밀도가 손실됩니다. 위 예시에서는 `Float32` 대신 `BFloat16`를 저장하므로, 나중에 더 정확한 검색을 수행하고 싶더라도 불가능합니다. 한 가지 대안은 양자화된 데이터와 전체 정밀도 데이터를 각각 저장하는 것입니다. 이 방법은 가능하지만 중복된 저장 공간이 필요합니다. 예를 들어 원본 데이터가 `Float64`이고, 서로 다른 정밀도(16비트, 32비트 또는 전체 64비트)로 검색을 실행하려는 경우를 생각해 보십시오. 이 경우 데이터 사본 3개를 각각 저장해야 합니다.

ClickHouse는 이러한 한계를 해결하는 Quantized Bit (`QBit`) 데이터 타입을 제공하며, 다음과 같은 특징이 있습니다:

1. 원본 전체 정밀도 데이터를 저장합니다.
2. 쿼리 시점에 양자화 정밀도를 지정할 수 있습니다.

이는 데이터를 비트 그룹화된 포맷으로 저장하여(즉, 모든 벡터의 i번째 비트를 함께 저장하여) 요청한 정밀도 수준만 읽을 수 있도록 함으로써 구현됩니다. 필요할 때는 원본 데이터를 모두 사용할 수 있으면서도, 양자화를 통해 I/O와 계산량을 줄여 속도 이점을 얻을 수 있습니다. 최대 정밀도를 선택하면 검색은 정확해집니다.

`QBit` 타입의 컬럼을 선언하려면 다음 구문을 사용하십시오:

```sql
column_name QBit(element_type, dimension[, stride])
```

각 항목의 의미는 다음과 같습니다:

* `element_type` – 각 벡터 요소의 타입입니다. 지원되는 타입은 `Int8`, `BFloat16`, `Float32`, `Float64`입니다.
* `dimension` – 각 벡터를 구성하는 요소 수입니다.
* `stride` – 선택 사항입니다. `dimension`의 제수로, 차원을 별도 스트림에 저장되는 `dimension / stride`개의 연속된 그룹으로 나눕니다. 따라서 선행 차원만 검색할 때는 더 적은 수의 스트림만 읽게 됩니다(Matryoshka embeddings에 유용함). 기본값은 `dimension`이며, 이 경우 이 타입은 stride가 없는 `QBit`과 바이트 단위로 동일합니다. 자세한 내용은 [`QBit` 데이터 타입 페이지](/ko/sql-reference/data-types/qbit)를 참조하십시오.

<div id="qbit-create">
  #### `QBit` 테이블 생성 및 데이터 추가
</div>

```sql
CREATE TABLE fruit_animal (
    word String,
    vec QBit(Float64, 5)
) ENGINE = MergeTree
ORDER BY word;

INSERT INTO fruit_animal VALUES
    ('apple', [-0.99105519, 1.28887844, -0.43526649, -0.98520696, 0.66154391]),
    ('banana', [-0.69372815, 0.25587061, -0.88226235, -2.54593015, 0.05300475]),
    ('orange', [0.93338752, 2.06571317, -0.54612565, -1.51625717, 0.69775337]),
    ('dog', [0.72138876, 1.55757105, 2.10953259, -0.33961248, -0.62217325]),
    ('cat', [-0.56611276, 0.52267331, 1.27839863, -0.59809804, -1.26721048]),
    ('horse', [-0.61435682, 0.48542571, 1.21091247, -0.62530446, -1.33082533]);
```

<div id="qbit-search">
  #### `QBit`을 사용한 벡터 검색
</div>

L2 거리를 사용해 단어 &#39;lemon&#39;을 나타내는 벡터의 최근접 이웃을 찾아보겠습니다. 거리 함수의 세 번째 매개변수는 비트 단위의 정밀도를 지정합니다. 값이 클수록 정확도는 높아지지만 더 많은 계산이 필요합니다.

`QBit`에서 사용할 수 있는 모든 거리 함수는 [여기](../../../sql-reference/data-types/qbit.md#vector-search-functions)에서 확인할 수 있습니다.

**전체 정밀도 검색(64비트):**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 64) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬────────────distance─┐
1. │ apple  │ 0.14639757188169716 │
2. │ banana │   1.998961369007679 │
3. │ orange │   2.039041552613732 │
4. │ cat    │   2.752802631487914 │
5. │ horse  │  2.7555776805484813 │
6. │ dog    │   3.382295083120104 │
   └────────┴─────────────────────┘
```

**정밀도를 낮춘 검색:**

```sql
SELECT
    word,
    L2DistanceTransposed(vec, [-0.88693672, 1.31532824, -0.51182908, -0.99652702, 0.59907770], 12) AS distance
FROM fruit_animal
ORDER BY distance;
```

```text
   ┌─word───┬───────────distance─┐
1. │ apple  │  0.757668703053566 │
2. │ orange │ 1.5499475034938677 │
3. │ banana │ 1.6168396735102937 │
4. │ cat    │  2.429752230904804 │
5. │ horse  │  2.524650475528617 │
6. │ dog    │   3.17766975527459 │
   └────────┴────────────────────┘
```

12-bit 양자화를 사용하면 거리를 잘 근사하면서도 쿼리를 더 빠르게 실행할 수 있습니다. 상대적 순서는 대체로 유지되며, &#39;apple&#39;은 여전히 가장 가까운 일치 항목입니다.

<div id="qbit-performance">
  #### 성능 고려 사항
</div>

`QBit`의 성능상 이점은 I/O 작업이 줄어드는 데 있습니다. 정밀도를 낮추면 저장소에서 읽어야 하는 데이터가 감소하기 때문입니다. 또한 `QBit`에 `Float32` 데이터가 포함된 경우, 정밀도 매개변수가 16 이하이면 연산량이 줄어드는 데 따른 추가 이점도 얻을 수 있습니다. 정밀도 매개변수는 정확도와 속도 사이의 절충점을 직접 제어합니다.

* **더 높은 정밀도**(원본 데이터 폭에 더 가까움): 더 정확한 결과, 더 느린 쿼리
* **더 낮은 정밀도**: 근사 결과를 제공하는 더 빠른 쿼리, 더 적은 메모리 사용량

<div id="references">
  ### 참고
</div>

블로그 글:

* [ClickHouse를 활용한 벡터 검색 - 1부](https://clickhouse.com/blog/vector-search-clickhouse-p1)
* [ClickHouse를 활용한 벡터 검색 - 2부](https://clickhouse.com/blog/vector-search-clickhouse-p2)
* [쿼리 시점에 정밀도를 선택할 수 있는 벡터 검색 엔진을 구축했습니다](https://clickhouse.com/blog/qbit-vector-search)