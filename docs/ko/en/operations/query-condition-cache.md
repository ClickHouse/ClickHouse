---
description: 'ClickHouse에서 쿼리 조건 캐시 기능을 사용하고 구성하는 방법에 대한 안내'
sidebar_label: '쿼리 조건 캐시'
sidebar_position: 64
slug: /operations/query-condition-cache
title: '쿼리 조건 캐시'
doc_type: 'guide'
---

:::note
쿼리 조건 캐시는 [enable&#95;analyzer](https://clickhouse.com/docs/operations/settings/settings#enable_analyzer)가 true로 설정된 경우에만 작동하며, 기본값도 true입니다.
:::

실제 환경의 많은 워크로드에서는 동일하거나 거의 동일한 데이터(예: 기존 데이터에 새 데이터가 추가된 경우)에 대해 쿼리를 반복해서 실행합니다.
ClickHouse는 이러한 쿼리 패턴에 대응하기 위해 다양한 최적화 기법을 제공합니다.
한 가지 방법은 인덱스 구조(예: 프라이머리 키 인덱스, 스키핑 인덱스, 프로젝션)나 사전 계산(구체화된 뷰(Materialized View))을 사용해 물리적 데이터 레이아웃을 조정하는 것입니다.
또 다른 방법은 ClickHouse의 [쿼리 캐시](query-cache.md)를 사용해 반복적인 쿼리 평가를 피하는 것입니다.
첫 번째 방식의 단점은 데이터베이스 관리자의 수동 개입과 모니터링이 필요하다는 점입니다.
두 번째 방식은 오래된 결과를 반환할 수 있으며(쿼리 캐시는 트랜잭션 일관성이 없기 때문), 이것이 허용 가능한지는 사용 사례에 따라 달라질 수 있습니다.

쿼리 조건 캐시는 이 두 문제를 모두 해결할 수 있는 깔끔한 방법을 제공합니다.
이 기능은 동일한 데이터에 대해 필터 조건(예: `WHERE col = 'xyz'`)을 평가하면 항상 같은 결과가 나온다는 원리에 기반합니다.
좀 더 구체적으로 말하면, 쿼리 조건 캐시는 평가된 각 필터 조건과 각 그래뉼(= 기본적으로 8192행 블록)에 대해, 해당 그래뉼 안에 필터 조건을 만족하는 행이 없는지를 기억합니다.
이 정보는 단일 비트로 기록됩니다. 0 비트는 조건에 일치하는 행이 없음을 나타내고, 1 비트는 적어도 하나의 일치하는 행이 있음을 의미합니다.
전자의 경우 ClickHouse는 필터 평가 중 해당 그래뉼을 건너뛸 수 있고, 후자의 경우 그래뉼을 로드해 평가해야 합니다.

쿼리 조건 캐시는 다음 3가지 전제 조건이 충족될 때 효과적입니다:

* 첫째, 워크로드에서 동일한 필터 조건을 반복적으로 평가해야 합니다. 이는 하나의 쿼리를 여러 번 반복 실행할 때 자연스럽게 발생하지만, 두 쿼리가 동일한 필터를 공유하는 경우에도 발생할 수 있습니다. 예를 들어 `SELECT product FROM products WHERE quality > 3` 와 `SELECT vendor, count() FROM products WHERE quality > 3` 가 이에 해당합니다.
* 둘째, 데이터의 대부분이 불변이어야 합니다. 즉, 쿼리 사이에 변경되지 않아야 합니다. 이는 일반적으로 ClickHouse에서 성립하는데, 파트는 불변이며 INSERT에 의해서만 생성되기 때문입니다.
* 셋째, 필터는 선택성이 있어야 합니다. 즉, 필터 조건을 만족하는 행이 상대적으로 적어야 합니다. 필터 조건에 일치하는 행이 적을수록 더 많은 그래뉼이 0 비트(일치하는 행 없음)로 기록되고, 후속 필터 평가에서 더 많은 데이터를 &quot;가지치기&quot;할 수 있습니다.

<div id="memory-consumption">
  ## 메모리 사용량
</div>

쿼리 조건 캐시는 필터 조건과 그래뉼당 1비트만 저장하므로 메모리를 거의 사용하지 않습니다.
쿼리 조건 캐시의 최대 크기는 서버 설정 [`query_condition_cache_size`](server-configuration-parameters/settings.md#query_condition_cache_size)로 설정할 수 있습니다(기본값: 100 MB).
캐시 크기 100 MB는 100 * 1024 * 1024 * 8 = 838,860,800개의 엔트리에 해당합니다.
각 엔트리는 하나의 mark(기본적으로 8192개 행)를 나타내므로, 캐시는 단일 컬럼에 대해 최대 6,871,947,673,600(6.8조)개의 행까지 커버할 수 있습니다.
실제 환경에서는 필터가 둘 이상의 컬럼에서 평가되므로, 이 수치는 필터링되는 컬럼 수로 나누어야 합니다.

<div id="configuration-settings-and-usage">
  ## 구성 설정 및 사용법
</div>

[use&#95;query&#95;condition&#95;cache](settings/settings#use_query_condition_cache) 설정은 특정 쿼리 또는 현재 세션의 모든 쿼리에서 쿼리 조건 캐시를 사용할지 여부를 제어합니다.

예를 들어, 쿼리를 처음 실행할 때

```sql
SELECT col1, col2
FROM table
WHERE col1 = 'x'
SETTINGS use_query_condition_cache = true;
```

프레디케이트를 충족하지 않는 테이블 범위를 저장합니다.
이후 동일한 쿼리를 `use_query_condition_cache = true` 매개변수와 함께 다시 실행하면 쿼리 조건 캐시를 활용하여 스캔하는 데이터를 줄일 수 있습니다.

<div id="administration">
  ## 관리
</div>

쿼리 조건 캐시는 ClickHouse가 재시작되면 유지되지 않습니다.

쿼리 조건 캐시를 비우려면 [`SYSTEM CLEAR QUERY CONDITION CACHE`](../sql-reference/statements/system.md#drop-query-condition-cache)를 실행하십시오.

캐시의 내용은 시스템 테이블(system table) [system.query&#95;condition&#95;cache](system-tables/query_condition_cache.md)에 표시됩니다.
현재 쿼리 조건 캐시의 크기를 MB 단위로 계산하려면 `SELECT formatReadableSize(sum(entry_size)) FROM system.query_condition_cache`를 실행하십시오.
개별 필터 조건을 조사하려면 `system.query_condition_cache`의 `condition` 필드를 확인할 수 있습니다. 이 필드는 디버그 빌드에서만 사용할 수 있습니다.

데이터베이스 시작 이후의 쿼리 조건 캐시 적중 횟수와 누락 횟수는 시스템 테이블(system table) [system.events](system-tables/events.md)의 &quot;QueryConditionCacheHits&quot; 및 &quot;QueryConditionCacheMisses&quot; 이벤트에 표시됩니다.
두 counters는 `use_query_condition_cache = true` 설정으로 실행되는 `SELECT` 쿼리에 대해서만 갱신되며, 다른 쿼리는 &quot;QueryCacheMisses&quot;에 영향을 주지 않습니다.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [쿼리 조건 캐시 소개](https://clickhouse.com/blog/introducing-the-clickhouse-query-condition-cache)
* [Predicate Caching: Query-Driven Secondary Indexing for Cloud Data Warehouses (Schmidt et. al., 2024)](https://doi.org/10.1145/3626246.3653395)