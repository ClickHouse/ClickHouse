---
description: 'ClickHouse 쿼리 분석기를 자세히 설명하는 페이지'
keywords: ['분석기']
sidebar_label: '분석기'
slug: /operations/analyzer
title: '분석기'
doc_type: 'reference'
---

ClickHouse 버전 `24.3`부터는 새로운 쿼리 분석기가 기본적으로 활성화됩니다.
작동 방식에 대한 자세한 내용은 [여기](/ko/guides/developer/understanding-query-execution-with-the-analyzer#analyzer)를 참조하십시오.

<div id="known-incompatibilities">
  ## 알려진 비호환 사항
</div>

많은 버그를 수정하고 새로운 최적화를 도입했지만, 이와 함께 ClickHouse의 동작 방식에도 일부 호환되지 않는 변경 사항이 적용되었습니다. 분석기에 맞게 쿼리를 어떻게 재작성해야 하는지 확인하려면 아래 변경 사항을 읽어보십시오.

<div id="invalid-queries-are-no-longer-optimized">
  ### 유효하지 않은 쿼리는 더 이상 최적화되지 않습니다
</div>

이전의 쿼리 계획 체계에서는 쿼리 검증 단계 전에 AST 수준의 최적화를 적용했습니다.
이 최적화로 인해 원본 쿼리가 유효하고 실행 가능한 형태로 재작성될 수 있었습니다.

분석기에서는 최적화 단계 전에 쿼리 검증이 수행됩니다.
즉, 이전에는 실행할 수 있었던 유효하지 않은 쿼리를 이제는 지원하지 않습니다.
이러한 경우에는 쿼리를 수동으로 수정해야 합니다.

<div id="example-1">
  #### 예시 1
</div>

다음 쿼리는 집계 후에는 `toString(number)`만 사용할 수 있음에도 PROJECTION 목록에서 컬럼 `number`를 사용합니다.
이전 분석기에서는 `GROUP BY toString(number)`가 `GROUP BY number,`로 최적화되어 해당 쿼리가 유효했습니다.

```sql
SELECT number
FROM numbers(1)
GROUP BY toString(number)
```

<div id="example-2">
  #### 예시 2
</div>

이 쿼리에서도 동일한 문제가 발생합니다. `number` 컬럼은 다른 키와 함께 집계된 뒤에 사용됩니다.
이전 쿼리 분석기는 `number > 5` 필터를 `HAVING` 절에서 `WHERE` 절로 옮겨 이 쿼리를 수정했습니다.

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
GROUP BY n
HAVING number > 5
```

쿼리를 수정하려면 표준 SQL 구문에 맞게 집계되지 않은 컬럼에 적용되는 모든 조건을 `WHERE` 절로 옮겨야 합니다:

```sql
SELECT
    number % 2 AS n,
    sum(number)
FROM numbers(10)
WHERE number > 5
GROUP BY n
```

<div id="create-view-with-invalid-query">
  ### 잘못된 쿼리가 있는 `CREATE VIEW`
</div>

분석기는 항상 타입 검사를 수행합니다.
이전에는 잘못된 `SELECT` 쿼리가 포함된 `VIEW`를 생성할 수 있었습니다.
이 경우 첫 번째 `SELECT` 또는 `INSERT` 시점에 오류가 발생했습니다(`MATERIALIZED VIEW`의 경우).

이제는 이런 방식으로 `VIEW`를 생성할 수 없습니다.

<div id="example-view">
  #### 예시
</div>

```sql
CREATE TABLE source (data String)
ENGINE=MergeTree
ORDER BY tuple();

CREATE VIEW some_view
AS SELECT JSONExtract(data, 'test', 'DateTime64(3)')
FROM source;
```

<div id="known-incompatibilities-of-the-join-clause">
  ### `JOIN` 절의 알려진 비호환 사항
</div>

<div id="join-using-column-from-projection">
  #### PROJECTION의 컬럼을 사용한 `JOIN`
</div>

기본적으로 `SELECT` 목록의 별칭은 `JOIN USING` 키로 사용할 수 없습니다.

새로운 설정 `analyzer_compatibility_join_using_top_level_identifier`를 활성화하면 `JOIN USING`의 동작이 바뀌어, 왼쪽 테이블의 컬럼을 직접 사용하는 대신 `SELECT` 쿼리의 PROJECTION 목록에 있는 표현식을 기준으로 식별자를 우선적으로 해석합니다.

예시:

```sql
SELECT a + 1 AS b, t2.s
FROM VALUES('a UInt64, b UInt64', (1, 1)) AS t1
JOIN VALUES('b UInt64, s String', (1, 'one'), (2, 'two')) t2
USING (b);
```

`analyzer_compatibility_join_using_top_level_identifier`를 `true`로 설정하면 join 조건은 이전 버전과 동일하게 `t1.a + 1 = t2.b`로 해석됩니다.
결과는 `2, 'two'`입니다.
설정이 `false`이면 join 조건은 기본적으로 `t1.b = t2.b`로 해석되며, 쿼리는 `2, 'one'`을 반환합니다.
`t1`에 `b`가 없으면 쿼리는 오류를 반환하며 실패합니다.

<div id="changes-in-behavior-with-join-using-and-aliasmaterialized-columns">
  #### `JOIN USING`과 `ALIAS`/`MATERIALIZED` 컬럼 사용 시 동작 변경
</div>

분석기에서 `ALIAS` 또는 `MATERIALIZED` 컬럼이 포함된 `JOIN USING` 쿼리에 `*`를 사용하면, 기본적으로 해당 컬럼도 결과 집합에 포함됩니다.

예시:

```sql
CREATE TABLE t1 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t1 VALUES (1), (2);

CREATE TABLE t2 (id UInt64, payload ALIAS sipHash64(id)) ENGINE = MergeTree ORDER BY id;
INSERT INTO t2 VALUES (2), (3);

SELECT * FROM t1
FULL JOIN t2 USING (payload);
```

분석기에서는 이 쿼리의 결과에 두 테이블 모두의 `id`와 함께 `payload` 컬럼이 포함됩니다.
반면 이전 분석기에서는 특정 설정(`asterisk_include_alias_columns` 또는 `asterisk_include_materialized_columns`)이 활성화된 경우에만 이러한 `ALIAS` 컬럼이 포함되었고,
컬럼이 다른 순서로 나타날 수도 있었습니다.

일관되고 예상 가능한 결과를 얻으려면, 특히 기존 쿼리를 분석기로 마이그레이션할 때 `*`를 사용하는 대신 `SELECT` 절에서 컬럼을 명시적으로 지정하는 것이 좋습니다.

<div id="handling-of-type-modifiers-for-columns-in-using-clause">
  #### `USING` 절에서 컬럼 유형 수정자 처리
</div>

새 버전의 분석기에서는 `USING` 절에 지정된 컬럼의 공통 상위 타입을 결정하는 규칙이 더 예측 가능한 결과를 내도록 표준화되었습니다.
특히 `LowCardinality`와 `Nullable` 같은 유형 수정자를 처리할 때 그렇습니다.

* `LowCardinality(T)` 및 `T`: `LowCardinality(T)` 유형의 컬럼을 `T` 유형의 컬럼과 조인하면, 결과 공통 상위 타입은 `T`가 되며 `LowCardinality` 수정자는 사실상 제거됩니다.
* `Nullable(T)` 및 `T`: `Nullable(T)` 유형의 컬럼을 `T` 유형의 컬럼과 조인하면, 결과 공통 상위 타입은 `Nullable(T)`가 되어 널 허용 속성이 유지됩니다.

예시:

```sql
SELECT id, toTypeName(id)
FROM VALUES('id LowCardinality(String)', ('a')) AS t1
FULL OUTER JOIN VALUES('id String', ('b')) AS t2
USING (id);
```

이 쿼리에서는 `id`의 공통 상위 유형(공통 상위 타입)이 `String`으로 결정되며, 이 과정에서 `t1`의 `LowCardinality` 수정자는 제거됩니다.

<div id="projection-column-names-changes">
  ### PROJECTION 컬럼 이름 변경 사항
</div>

PROJECTION 이름을 계산할 때는 별칭이 치환되지 않습니다.

```sql
SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 0
FORMAT PrettyCompact

   ┌─x─┬─plus(plus(1, 1), 1)─┐
1. │ 2 │                   3 │
   └───┴─────────────────────┘

SELECT
    1 + 1 AS x,
    x + 1
SETTINGS enable_analyzer = 1
FORMAT PrettyCompact

   ┌─x─┬─plus(x, 1)─┐
1. │ 2 │          3 │
   └───┴────────────┘
```

<div id="incompatible-function-arguments-types">
  ### 호환되지 않는 함수 인수 타입
</div>

분석기에서는 원본 쿼리 분석 중에 타입 추론이 이루어집니다.
이 변경으로 인해 타입 검사는 단락 평가보다 먼저 수행되므로, `if` 함수의 인수는 항상 공통 상위 타입을 가져야 합니다.

예를 들어, 다음 쿼리는 `There is no supertype for types Array(UInt8), String because some of them are Array and some of them are not` 오류와 함께 실패합니다:

```sql
SELECT toTypeName(if(0, [2, 3, 4], 'String'))
```

<div id="heterogeneous-clusters">
  ### 이기종 클러스터
</div>

분석기는 클러스터 내 서버 간 통신 프로토콜을 크게 바꿉니다. 따라서 서버마다 `enable_analyzer` 설정 값이 다르면 분산 쿼리를 실행할 수 없습니다.

<div id="mutations-are-interpreted-by-previous-analyzer">
  ### 뮤테이션은 이전 분석기로 해석됩니다
</div>

뮤테이션은 여전히 기존 분석기를 사용합니다.
즉, 일부 새로운 ClickHouse SQL 기능은 뮤테이션에서 아직 사용할 수 없습니다. 예를 들어 `QUALIFY` 절이 있습니다.
현재 상태는 [여기](https://github.com/ClickHouse/ClickHouse/issues/61563)에서 확인할 수 있습니다.

<div id="unsupported-features">
  ### 지원되지 않는 기능
</div>

현재 분석기가 지원하지 않는 기능 목록은 다음과 같습니다.

* Annoy 인덱스.
* Hypothesis 인덱스. [여기](https://github.com/ClickHouse/ClickHouse/pull/48381)에서 현재 작업이 진행 중입니다.
* Window view는 지원되지 않습니다. 앞으로도 지원할 계획이 없습니다.

<div id="cloud-migration">
  ## Cloud 마이그레이션
</div>

새로운 기능 및 성능 최적화를 지원하기 위해 현재 비활성화된 모든 인스턴스에서 새로운 쿼리 분석기를 활성화하고 있습니다. 이 변경으로 SQL 범위 규칙이 더 엄격하게 적용되므로, 고객은 이를 준수하지 않는 쿼리를 수동으로 수정해야 합니다.

<div id="migration-workflow">
  ### 마이그레이션 워크플로
</div>

1. `normalized_query_hash`를 사용하여 `system.query_log`를 필터링한 뒤 쿼리를 식별합니다:

```sql
SELECT query 
FROM clusterAllReplicas(default, system.query_log)
WHERE normalized_query_hash='{hash}' 
LIMIT 1 
SETTINGS skip_unavailable_shards=1
```

2. 다음 설정을 추가해 분석기를 활성화한 뒤 쿼리를 실행하세요.

```sql
SETTINGS
    enable_analyzer=1,
    analyzer_compatibility_join_using_top_level_identifier=1
```

3. 쿼리 결과를 재구성하고, 분석기를 비활성화했을 때 생성되는 출력과 일치하는지 검증합니다.

내부 테스트에서 가장 자주 발견된 비호환성은 아래 내용을 참조하십시오.

<div id="unknown-expression-identifier">
  ### 알 수 없는 표현식 식별자
</div>

오류: `Unknown expression identifier ... in scope ... (UNKNOWN_IDENTIFIER)`. 예외 코드: 47

원인: 필터에서 계산된 별칭을 참조하거나, 모호한 서브쿼리 PROJECTION을 사용하거나, &quot;동적&quot; CTE 스코프를 사용하는 등 비표준적이고 느슨한 legacy 동작에 의존하는 쿼리는 이제 잘못된 쿼리로 올바르게 판별되어 즉시 거부됩니다.

해결 방법: SQL 패턴을 다음과 같이 업데이트하세요.

* 필터 로직: 결과를 기준으로 필터링하는 경우 로직을 WHERE에서 HAVING으로 옮기고, 원본 데이터를 기준으로 필터링하는 경우 WHERE에 해당 표현식을 다시 작성하세요.
* 서브쿼리 범위: 외부 쿼리에 필요한 모든 컬럼을 명시적으로 선택하세요.
* JOIN 키: 키가 별칭인 경우 USING 대신 전체 표현식을 포함한 ON을 사용하세요.
* 외부 쿼리에서는 내부 테이블이 아니라 서브쿼리/CTE 자체의 별칭을 참조하세요.

<div id="non-aggregated-columns-in-group-by">
  ### GROUP BY의 비집계 컬럼
</div>

오류: `Column ... is not under aggregate function and not in GROUP BY keys (NOT_AN_AGGREGATE)`. 예외 코드: 215

원인: 이전 분석기는 GROUP BY 절에 없는 컬럼도 선택할 수 있게 허용했습니다(이 경우 대개 임의의 값을 선택했습니다). 분석기는 표준 SQL을 따르므로, 선택한 모든 컬럼은 집계 함수의 결과이거나 그룹화 키여야 합니다.

해결 방법: 해당 컬럼을 `any()`, `argMax()`로 감싸거나 GROUP BY에 추가하십시오.

```sql
/* ORIGINAL QUERY */
-- device_id is ambiguous
SELECT user_id, device_id FROM table GROUP BY user_id

/* FIXED QUERY */
SELECT user_id, any(device_id) FROM table GROUP BY user_id
-- OR
SELECT user_id, device_id FROM table GROUP BY user_id, device_id
```

<div id="duplicate-cte-names">
  ### 중복된 CTE 이름
</div>

오류: `CTE with name ... already exists (MULTIPLE_EXPRESSIONS_FOR_ALIAS)`. Exception code: 179

원인: 이전 분석기는 같은 이름의 공통 테이블 표현식(WITH ...)을 여러 개 정의해, 나중에 정의한 것이 앞서 정의한 것을 가리도록 허용했습니다. 분석기는 이러한 모호함을 허용하지 않습니다.

해결 방법: 중복된 CTE 이름을 각각 고유하게 변경하십시오.

```sql
/* ORIGINAL QUERY */
WITH 
  data AS (SELECT 1 AS id), 
  data AS (SELECT 2 AS id) -- Redefined
SELECT * FROM data;

/* FIXED QUERY */
WITH 
  raw_data AS (SELECT 1 AS id), 
  processed_data AS (SELECT 2 AS id)
SELECT * FROM processed_data;
```

<div id="ambiguous-column-identifiers">
  ### 모호한 컬럼 식별자
</div>

오류: `JOIN [JOIN TYPE] ambiguous identifier ... (AMBIGUOUS_IDENTIFIER)` Exception 코드: 207

원인: 쿼리에서 소스 테이블을 지정하지 않은 상태로 JOIN에 포함된 여러 테이블에 존재하는 컬럼 이름을 참조합니다. 이전 분석기는 내부 로직에 따라 해당 컬럼을 추정하는 경우가 많았지만, 현재 분석기는 명시적으로 이름을 지정해야 합니다.

해결 방법: 컬럼을 `table&#95;alias.column&#95;name` 형식으로 완전히 지정합니다.

```sql
/* ORIGINAL QUERY */
SELECT table1.ID AS ID FROM table1, table2 WHERE ID...

/* FIXED QUERY */
SELECT table1.ID AS ID_RENAMED FROM table1, table2 WHERE ID_RENAMED...
```

<div id="invalid-usage-of-final">
  ### FINAL의 잘못된 사용
</div>

오류: `Table expression modifiers FINAL are not supported for subquery...` 또는 `Storage ... doesn't support FINAL` (`UNSUPPORTED_METHOD`). 예외 코드: 1, 181

원인: FINAL은 테이블 저장소, 특히 [Shared]ReplacingMergeTree용 수정자입니다. 분석기는 다음 대상에 FINAL이 적용되면 이를 거부합니다:

* 서브쿼리 또는 파생 테이블(예: FROM (SELECT ...) FINAL).
* FINAL을 지원하지 않는 테이블 엔진(예: SharedMergeTree).

해결 방법: 서브쿼리 내부의 원본 테이블에만 FINAL을 적용하거나, 엔진이 이를 지원하지 않으면 FINAL을 제거하십시오.

```sql
/* ORIGINAL QUERY */
SELECT * FROM (SELECT * FROM my_table) AS subquery FINAL ...

/* FIXED QUERY */
SELECT * FROM (SELECT * FROM my_table FINAL) AS subquery ...
```

<div id="countdistinct-case-insensitivity">
  ### `countDistinct()` 함수의 대소문자 비구분
</div>

오류: `Function with name countdistinct does not exist (UNKNOWN_FUNCTION)`. 예외 코드: 46

원인: 함수 이름은 대소문자를 구분하며, 분석기에서 엄격하게 매핑됩니다. `countdistinct`(모두 소문자)는 더 이상 자동으로 인식되지 않습니다.

해결 방법: 표준 `countDistinct`(camelCase) 또는 ClickHouse 전용 `uniq`를 사용하십시오.