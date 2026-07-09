---
description: 'SimpleAggregateFunction 데이터 타입 문서'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'SimpleAggregateFunction 타입'
doc_type: 'reference'
---

<div id="description">
  ## 설명
</div>

`SimpleAggregateFunction` 데이터 타입은 집계 함수의 중간 상태를 저장하지만, [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md)
타입처럼 전체 상태를 저장하지는 않습니다.

이 최적화는 다음 속성을 만족하는 함수에 적용할 수
있습니다:

> 행 집합 `S1 UNION ALL S2`에 함수 `f`를 적용한 결과는
> 행 집합의 각 부분에 `f`를 개별적으로 적용한 다음, 그 결과에 다시
> `f`를 적용하여 얻을 수 있습니다: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`.

이 속성은 부분 집계 결과만으로도 결합된 결과를 계산하는 데 충분하다는 것을
보장하므로, 추가 데이터를 저장하거나 처리할 필요가 없습니다. 예를
들어 `min` 또는 `max` 함수는 중간 단계의 결과만으로 최종 결과를
계산할 수 있어 추가 단계가 필요하지 않습니다. 반면 `avg` 함수는 합계와 개수를
함께 유지해야 하며, 이 값들은 중간 상태를 결합하는 최종 `Merge` 단계에서 나누어져
평균이 계산됩니다.

집계 함수 값은 일반적으로 함수 이름에 [`-SimpleState`](/ko/sql-reference/aggregate-functions/combinators#-simplestate) 컴비네이터(combinator)를 붙여
집계 함수를 호출하면 생성됩니다.

<div id="syntax">
  ## 구문
</div>

```sql
SimpleAggregateFunction(aggregate_function_name, types_of_arguments...)
```

**매개변수**

* `aggregate_function_name` - 집계 함수의 이름입니다.
* `Type` - 집계 함수 인수의 타입입니다.

<div id="supported-functions">
  ## 지원되는 함수
</div>

다음 집계 함수가 지원됩니다.

* [`any`](/ko/sql-reference/aggregate-functions/reference/any.md)
* [`any_respect_nulls`](/ko/sql-reference/aggregate-functions/reference/any.md)
* [`anyLast`](/ko/sql-reference/aggregate-functions/reference/anyLast.md)
* [`anyLast_respect_nulls`](/ko/sql-reference/aggregate-functions/reference/anyLast.md)
* [`min`](/ko/sql-reference/aggregate-functions/reference/min.md)
* [`max`](/ko/sql-reference/aggregate-functions/reference/max.md)
* [`sum`](/ko/sql-reference/aggregate-functions/reference/sum.md)
* [`sumWithOverflow`](/ko/sql-reference/aggregate-functions/reference/sumWithOverflow.md)
* [`groupBitAnd`](/ko/sql-reference/aggregate-functions/reference/groupBitAnd.md)
* [`groupBitOr`](/ko/sql-reference/aggregate-functions/reference/groupBitOr.md)
* [`groupBitXor`](/ko/sql-reference/aggregate-functions/reference/groupBitXor.md)
* [`groupArrayArray`](/ko/sql-reference/aggregate-functions/reference/groupArrayArray.md)
* [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupUniqArray.md)
* [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
* [`sumMap` (`sumMappedArrays`)](/ko/sql-reference/aggregate-functions/reference/sumMappedArrays.md)
* [`minMap` (`minMappedArrays`)](/ko/sql-reference/aggregate-functions/reference/minMappedArrays.md)
* [`maxMap` (`maxMappedArrays`)](/ko/sql-reference/aggregate-functions/reference/maxMappedArrays.md)

:::note
`SimpleAggregateFunction(func, Type)`의 값은 모두 동일한 `Type`을 가지므로,
`AggregateFunction` 타입과는 달리
`-Merge`/`-State` combinator를 적용할 필요가 없습니다.

동일한 집계 함수에 대해서는 `SimpleAggregateFunction` 타입이 `AggregateFunction`보다 더 나은 성능을 제공합니다.
:::

<div id="example">
  ## 예시
</div>

```sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

<div id="related-content">
  ## 관련 콘텐츠
</div>

* 블로그: [ClickHouse에서 배열, 맵, 상태에서 집계 조합자 사용하기](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)    - 블로그: [ClickHouse에서 배열, 맵, 상태에서 집계 조합자 사용하기](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* [AggregateFunction](/ko/sql-reference/data-types/aggregatefunction) 타입.