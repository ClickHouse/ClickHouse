---
description: 'DISTINCT 절 문서'
sidebar_label: 'DISTINCT'
slug: /sql-reference/statements/select/distinct
title: 'DISTINCT 절'
doc_type: '참고'
---

`SELECT DISTINCT`를 지정하면 쿼리 결과에는 고유한 행만 남습니다. 즉, 결과에서 완전히 동일한 행들의 각 집합마다 1개의 행만 남습니다.

고유한 값을 가져야 하는 컬럼 목록은 `SELECT DISTINCT ON (column1, column2,...)`로 지정할 수 있습니다. 컬럼을 지정하지 않으면 모든 컬럼을 기준으로 판단합니다.

다음 테이블을 예로 살펴보겠습니다:

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

컬럼을 지정하지 않고 `DISTINCT`를 사용하는 경우:

```sql
SELECT DISTINCT * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 1 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

지정한 컬럼에 `DISTINCT` 사용:

```sql
SELECT DISTINCT ON (a,b) * FROM t1;
```

```text
┌─a─┬─b─┬─c─┐
│ 1 │ 1 │ 1 │
│ 2 │ 2 │ 2 │
│ 1 │ 2 │ 2 │
└───┴───┴───┘
```

<div id="distinct-and-order-by">
  ## DISTINCT 및 ORDER BY
</div>

ClickHouse는 하나의 쿼리에서 `DISTINCT` 절과 `ORDER BY` 절을 서로 다른 컬럼에 적용할 수 있습니다. `DISTINCT` 절은 `ORDER BY` 절보다 먼저 실행됩니다.

다음 테이블을 살펴보겠습니다:

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

데이터 조회:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b ASC;
```

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

서로 다른 정렬 방향으로 데이터 선택:

```sql
SELECT DISTINCT a FROM t1 ORDER BY b DESC;
```

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

행 `2, 4`는 정렬 전에 제외되었습니다.

쿼리를 작성할 때 이러한 구현상의 특성을 고려하십시오.

<div id="null-processing">
  ## NULL 처리
</div>

`DISTINCT`는 [NULL](/ko/sql-reference/syntax#null)을 `NULL`이 특정 값이며 `NULL==NULL`인 것처럼 처리합니다. 다시 말해, `DISTINCT` 결과에서는 `NULL`이 포함된 서로 다른 조합이 한 번만 나타납니다. 이는 대부분의 다른 문맥에서의 `NULL` 처리와 다릅니다.

<div id="alternatives">
  ## 대안
</div>

집계 함수를 사용하지 않고도 `SELECT` 절에 지정된 것과 동일한 값 집합을 기준으로 [GROUP BY](/ko/sql-reference/statements/select/group-by)를 적용해 같은 결과를 얻을 수 있습니다. 하지만 이 방식은 `GROUP BY` 접근 방식과 몇 가지 차이가 있습니다.

* `DISTINCT`는 `GROUP BY`와 함께 적용할 수 있습니다.
* [ORDER BY](../../../sql-reference/statements/select/order-by.md)를 생략하고 [LIMIT](../../../sql-reference/statements/select/limit.md)를 지정하면, 필요한 수의 서로 다른 행을 읽는 즉시 쿼리 실행이 중단됩니다.
* 데이터 블록은 전체 쿼리 실행이 끝날 때까지 기다리지 않고 처리되는 대로 출력됩니다.