---
description: '집계 함수의 중간 상태를 저장하는 ClickHouse의 AggregateFunction 데이터 타입 문서'
keywords: ['AggregateFunction', '데이터 타입']
sidebar_label: 'AggregateFunction'
sidebar_position: 46
slug: /sql-reference/data-types/aggregatefunction
title: 'AggregateFunction 데이터 타입'
doc_type: '참고'
---

<div id="description">
  ## 설명
</div>

ClickHouse의 모든 [집계 함수](/ko/sql-reference/aggregate-functions)에는
구현별 중간 상태가 있으며, 이 상태는 직렬화하여 `AggregateFunction` 데이터 타입으로
테이블에 저장할 수 있습니다. 이는 일반적으로
[materialized view](../../sql-reference/statements/create/view.md)를 사용해 수행됩니다.

`AggregateFunction` 유형과 함께 일반적으로 사용되는 집계 함수 [조합자](/ko/sql-reference/aggregate-functions/combinators)는
다음 두 가지입니다:

* [`-State`](/ko/sql-reference/aggregate-functions/combinators#-state) 집계 함수 조합자로, 집계
  함수 이름 뒤에 붙이면 `AggregateFunction` 중간 상태를 생성합니다.
* [`-Merge`](/ko/sql-reference/aggregate-functions/combinators#-merge) 집계
  함수 조합자로, 중간 상태에서 집계의 최종 결과를
  얻는 데 사용됩니다.

<div id="syntax">
  ## 구문
</div>

```sql
AggregateFunction(aggregate_function_name, types_of_arguments...)
```

**매개변수**

* `aggregate_function_name` - 집계 함수의 이름입니다. 함수가
  매개변수형인 경우 해당 매개변수도 함께 지정해야 합니다.
* `types_of_arguments` - 집계 함수 인수의 타입입니다.

예시:

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

<div id="usage">
  ## 사용량
</div>

<div id="data-insertion">
  ### 데이터 삽입
</div>

컬럼 유형이 `AggregateFunction`인 테이블에 데이터를 삽입하려면,
집계 함수와
[`-State`](/ko/sql-reference/aggregate-functions/combinators#-state) 집계 함수 조합자를 사용하는 `INSERT SELECT`를
사용할 수 있습니다.

예를 들어, `AggregateFunction(uniq, UInt64)` 및
`AggregateFunction(quantiles(0.5, 0.9), UInt64)` 유형의 컬럼에 삽입하려면 다음과 같은
조합자가 적용된 집계 함수를 사용합니다.

```sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

함수 `uniq` 및 `quantiles`와 달리 `uniqState`와 `quantilesState`는
(`-State` 조합자가 추가된 형태) 최종 값이 아니라 상태를 반환합니다.
즉, `AggregateFunction` 유형의 값을 반환합니다.

`SELECT` 쿼리 결과에서 `AggregateFunction` 유형의 값은
모든 ClickHouse 출력 포맷에 대해 구현별 이진 표현을 가집니다.

입력 값으로부터 상태를 만들 수 있게 해 주는 특별한 Session 수준 설정 `aggregate_function_input_format`이 있습니다.
이 설정은 다음 포맷을 지원합니다:

* `state` - 직렬화된 상태를 담은 바이너리 문자열입니다(기본값).
  예를 들어 `SELECT` 쿼리로 데이터를 `TabSeparated` 포맷에 dump하면
  이 dump를 `INSERT` 쿼리를 사용해 다시 로드할 수 있습니다.
* `value` - 이 포맷은 집계 함수 인수의 단일 값 하나를 기대하며, 인수가 여러 개인 경우에는 해당 값들의 튜플을 기대합니다. 이 값은 역직렬화되어 해당 상태를 형성합니다
* `array` - 이 포맷은 위의 values 옵션에서 설명한 것처럼 값의 배열을 기대하며, 배열의 모든 요소가 집계되어 상태를 형성합니다

<div id="data-selection">
  ### 데이터 선택
</div>

`AggregatingMergeTree` 테이블에서 데이터를 조회할 때는 `GROUP BY` 절을 사용하고,
데이터를 삽입할 때 사용한 것과 동일한 집계 함수를 사용하되
[`-Merge`](/ko/sql-reference/aggregate-functions/combinators#-merge) 조합자를 사용합니다.

`-Merge` 조합자가 추가된 집계 함수는 state 집합을 받아
이를 결합한 뒤 전체 데이터의 집계 결과를 반환합니다.

예를 들어, 다음 두 쿼리는 동일한 결과를 반환합니다:

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

<div id="usage-example">
  ## 사용 예시
</div>

[AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) 엔진 설명을 참고하십시오.

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse에서 배열, 맵, 상태에 집계 조합자 사용하기](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* [MergeState](/ko/sql-reference/aggregate-functions/combinators#-mergestate)
  조합자
* [State](/ko/sql-reference/aggregate-functions/combinators#-state) 조합자