---
description: '`GROUP BY` 절 문서'
sidebar_label: 'GROUP BY'
slug: /sql-reference/statements/select/group-by
title: '`GROUP BY` 절'
doc_type: 'reference'
---

`GROUP BY` 절은 `SELECT` 쿼리를 집계 모드로 전환합니다. 동작 방식은 다음과 같습니다.

* `GROUP BY` 절에는 표현식 목록이 포함됩니다(단일 표현식도 길이가 1인 목록으로 간주됨). 이 목록은 &quot;그룹화 키&quot; 역할을 하며, 각 개별 표현식은 &quot;키 표현식&quot;이라고 합니다.
* [SELECT](/ko/sql-reference/statements/select/index.md), [HAVING](/ko/sql-reference/statements/select/having.md), [ORDER BY](/ko/sql-reference/statements/select/order-by.md) 절의 모든 표현식은 **반드시** 키 표현식을 기반으로 계산되거나, 키 표현식이 아닌 표현식(일반 컬럼 포함)에 대한 [집계 함수](../../../sql-reference/aggregate-functions/index.md)를 기반으로 계산되어야 합니다. 즉, 테이블에서 선택한 각 컬럼은 키 표현식에 사용되거나 집계 함수 내부에서 사용되어야 하며, 두 곳 모두에 사용될 수는 없습니다.
* `SELECT` 쿼리를 집계한 결과에는 원본 테이블에서 &quot;그룹화 키&quot;의 고유값 수만큼 행이 포함됩니다. 일반적으로 이로 인해 행 수가 크게 줄어들며, 많게는 자릿수 단위로 감소하기도 합니다. 다만 항상 그런 것은 아닙니다. 모든 &quot;그룹화 키&quot; 값이 서로 달랐다면 행 수는 그대로 유지됩니다.

컬럼 이름 대신 컬럼 번호로 테이블 데이터를 그룹화하려면 [enable&#95;positional&#95;arguments](/ko/operations/settings/settings#enable_positional_arguments) 설정을 활성화하십시오.

:::note
테이블에 대해 집계를 수행하는 다른 방법도 있습니다. 쿼리에서 테이블 컬럼이 집계 함수 내부에만 사용되는 경우 `GROUP BY` 절를 생략할 수 있으며, 이때는 빈 키 집합에 대한 집계가 수행되는 것으로 간주됩니다. 이러한 쿼리는 항상 정확히 1개의 행을 반환합니다.
:::

<div id="null-processing">
  ## NULL 처리
</div>

그룹화할 때 ClickHouse는 [NULL](/ko/sql-reference/syntax#null)을 하나의 값으로 해석하며, `NULL==NULL`로 취급합니다. 이는 대부분의 다른 상황에서의 `NULL` 처리와는 다릅니다.

다음 예시를 통해 그 의미를 살펴보겠습니다.

다음과 같은 테이블(table)이 있다고 가정하겠습니다:

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

쿼리 `SELECT sum(x), y FROM t_null_big GROUP BY y`를 실행한 결과는 다음과 같습니다:

```text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

`y = NULL`에 대한 `GROUP BY`가 `NULL`을 하나의 값처럼 취급하여 `x`를 합산한 것을 확인할 수 있습니다.

여러 개의 키를 `GROUP BY`에 전달하면, 결과에는 `NULL`을 특정 값처럼 취급한 선택 항목의 모든 조합이 표시됩니다.

<div id="rollup-modifier">
  ## ROLLUP 수정자
</div>

`ROLLUP` 수정자는 `GROUP BY` 목록의 순서를 기준으로 키 표현식의 소계를 계산할 때 사용됩니다. 소계 행은 결과 테이블 뒤에 추가됩니다.

소계는 역순으로 계산됩니다. 먼저 목록에서 마지막 키 표현식의 소계를 계산한 다음, 그 앞의 표현식에 대한 소계를 계산하는 방식으로 첫 번째 키 표현식까지 진행됩니다.

소계 행에서는 이미 &quot;그룹화된&quot; 키 표현식의 값이 `0` 또는 빈 문자열로 설정됩니다.

:::note
[HAVING](/ko/sql-reference/statements/select/having.md) 절은 소계 결과에 영향을 줄 수 있습니다.
:::

**예시**

테이블 t를 예로 들어 보겠습니다:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY ROLLUP(year, month, day);
```

`GROUP BY` 섹션에는 3개의 키 표현식이 있으므로, 결과에는 오른쪽에서 왼쪽으로 부분합이 &quot;롤업된&quot; 4개의 테이블이 포함됩니다:

* `GROUP BY year, month, day`;
* `GROUP BY year, month` (`day` 컬럼은 0으로 채워집니다);
* `GROUP BY year` (이제 `month`, `day` 컬럼이 모두 0으로 채워집니다);
* 그리고 합계 (3개의 키 표현식 컬럼이 모두 0입니다).

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```

같은 쿼리는 `WITH` keyword를 사용해서도 작성할 수 있습니다.

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;
```

**관련 항목**

* SQL 표준 호환성을 위한 [group&#95;by&#95;use&#95;nulls](/ko/operations/settings/settings.md#group_by_use_nulls) 설정.

<div id="cube-modifier">
  ## CUBE 수정자
</div>

`CUBE` 수정자는 `GROUP BY` 목록에 있는 키 표현식의 모든 조합에 대한 소계를 계산하는 데 사용됩니다. 소계 행은 결과 테이블 뒤에 추가됩니다.

소계 행에서는 &quot;그룹화된&quot; 모든 키 표현식의 값이 `0` 또는 빈 문자열로 설정됩니다.

:::note
[HAVING](/ko/sql-reference/statements/select/having.md) 절은 소계 결과에 영향을 줄 수 있습니다.
:::

**예시**

테이블 t를 살펴보겠습니다:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY CUBE(year, month, day);
```

`GROUP BY` 절에 3개의 키 표현식이 있으므로, 결과에는 가능한 모든 키 표현식 조합에 대한 소계가 포함된 8개의 테이블이 생성됩니다.

* `GROUP BY year, month, day`
* `GROUP BY year, month`
* `GROUP BY year, day`
* `GROUP BY year`
* `GROUP BY month, day`
* `GROUP BY month`
* `GROUP BY day`
* 그리고 합계.

`GROUP BY`에서 제외된 컬럼은 0으로 채워집니다.

```text title="Response"
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │     0 │   5 │       2 │
│ 2019 │     0 │   5 │       1 │
│ 2020 │     0 │  15 │       2 │
│ 2019 │     0 │  15 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   5 │       2 │
│    0 │    10 │  15 │       1 │
│    0 │    10 │   5 │       1 │
│    0 │     1 │  15 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   0 │       4 │
│    0 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   5 │       3 │
│    0 │     0 │  15 │       3 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```

같은 쿼리는 `WITH` 키워드를 사용해 작성할 수도 있습니다.

```sql title="Query"
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH CUBE;
```

**관련 항목**

* SQL 표준 호환성을 위한 [group&#95;by&#95;use&#95;nulls](/ko/operations/settings/settings.md#group_by_use_nulls) 설정.

<div id="with-totals-modifier">
  ## WITH TOTALS 수정자
</div>

`WITH TOTALS` 수정자를 지정하면 행이 하나 추가로 계산됩니다. 이 행에는 기본값(0 또는 빈 문자열)이 들어 있는 키 컬럼과, 모든 행에 걸쳐 계산된 값을 담은 집계 함수 컬럼(&quot;합계&quot; 값)이 포함됩니다.

이 추가 행은 `JSON*`, `TabSeparated*`, `Pretty*` 포맷에서만 생성되며, 다른 행과는 별도로 출력됩니다:

* `XML` 및 `JSON*` 포맷에서는 이 행이 별도의 `totals` 필드로 출력됩니다.
* `TabSeparated*`, `CSV*`, `Vertical` 포맷에서는 이 행이 주 결과 뒤에 출력되며, 그 앞에 빈 행이 하나 추가됩니다(다른 데이터 뒤).
* `Pretty*` 포맷에서는 이 행이 주 결과 뒤에 별도의 테이블로 출력됩니다.
* `Template` 포맷에서는 이 행이 지정된 템플릿에 따라 출력됩니다.
* 그 밖의 포맷에서는 사용할 수 없습니다.

:::note
`totals`는 `SELECT` 쿼리 결과에만 출력되며, `INSERT INTO ... SELECT`에서는 출력되지 않습니다.
:::

[HAVING](/ko/sql-reference/statements/select/having.md)이 있는 경우 `WITH TOTALS`는 여러 방식으로 동작할 수 있습니다. 동작 방식은 `totals_mode` 설정에 따라 달라집니다.

<div id="configuring-totals-processing">
  ### 합계 처리 구성
</div>

기본값은 `totals_mode = 'before_having'`입니다. 이 경우 &#39;totals&#39;는 HAVING 및 `max_rows_to_group_by`를 통과하지 못한 행까지 포함해 모든 행을 기준으로 계산됩니다.

다른 옵션은 &#39;totals&#39;에 HAVING을 통과한 행만 포함하며, `max_rows_to_group_by` 및 `group_by_overflow_mode = 'any'` 설정을 사용할 때의 동작이 서로 다릅니다.

`after_having_exclusive` – `max_rows_to_group_by`를 통과하지 못한 행은 포함하지 않습니다. 즉, `max_rows_to_group_by`를 생략했을 때와 비교해 &#39;totals&#39;의 행 수는 같거나 더 적습니다.

`after_having_inclusive` – `max_rows_to_group_by`를 통과하지 못한 모든 행을 &#39;totals&#39;에 포함합니다. 즉, `max_rows_to_group_by`를 생략했을 때와 비교해 &#39;totals&#39;의 행 수는 같거나 더 많습니다.

`after_having_auto` – HAVING을 통과한 행 수를 계산합니다. 이 값이 일정 비율(기본값 50%)보다 크면 `max_rows_to_group_by`를 통과하지 못한 모든 행을 &#39;totals&#39;에 포함합니다. 그렇지 않으면 포함하지 않습니다.

`totals_auto_threshold` – 기본값은 0.5입니다. `after_having_auto`에 사용되는 계수입니다.

`max_rows_to_group_by`와 `group_by_overflow_mode = 'any'`를 사용하지 않으면 `after_having`의 모든 변형은 동일하므로, 그중 어느 것을 사용해도 됩니다(예: `after_having_auto`).

서브쿼리에서 `WITH TOTALS`를 사용할 수 있으며, [JOIN](/ko/sql-reference/statements/select/join.md) 절의 서브쿼리에서도 사용할 수 있습니다(이 경우 해당 합계 값이 함께 결합됩니다).

<div id="group-by-all">
  ## GROUP BY ALL
</div>

`GROUP BY ALL`은 집계 함수가 아닌 SELECT된 모든 표현식을 나열하는 것과 같습니다.

예시:

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY ALL
```

와 같습니다

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY a * 2, b
```

집계 함수와 다른 필드를 모두 인수로 받는 함수가 있는 특수한 경우에는, `GROUP BY` 키에 해당 함수에서 추출할 수 있는 비집계 필드가 최대한 포함됩니다.

예시는 다음과 같습니다:

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY ALL
```

와 같습니다

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY substring(a, 4, 2), substring(a, 1, 2)
```

<div id="examples">
  ## 예시
</div>

예시:

```sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

MySQL과 달리(표준 SQL을 준수하므로), 키 또는 집계 함수에 포함되지 않은 컬럼의 값은 가져올 수 없습니다(상수 표현식 제외). 이를 우회하려면 `any` 집계 함수(처음으로 발견된 값을 가져옴)나 `min/max`를 사용할 수 있습니다.

예시:

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

각기 다른 키 값마다 `GROUP BY`는 집계 함수 값의 집합을 계산합니다.

<div id="grouping-sets-modifier">
  ## GROUPING SETS 수정자
</div>

이는 가장 일반적인 수정자입니다.
이 수정자를 사용하면 여러 집계 키 집합(grouping set)을 수동으로 지정할 수 있습니다.
각 grouping set에 대해 집계가 개별적으로 수행되며, 그 후 모든 결과가 결합됩니다.
grouping set에 컬럼이 포함되지 않으면 해당 컬럼은 기본값으로 채워집니다.

즉, 위에서 설명한 수정자들은 `GROUPING SETS`로 표현할 수 있습니다.
`ROLLUP`, `CUBE`, `GROUPING SETS` 수정자가 사용된 쿼리는 구문적으로는 동일하지만, 실행 방식은 다를 수 있습니다.
`GROUPING SETS`는 모든 작업을 병렬로 실행하려고 시도하는 반면, `ROLLUP`과 `CUBE`는 집계 결과의 최종 병합을 단일 스레드에서 수행합니다.

원본 컬럼에 기본값이 포함되어 있으면, 어떤 행이 해당 컬럼을 키로 사용하는 집계의 일부인지 아닌지 구분하기 어려울 수 있습니다.
이 문제를 해결하려면 `GROUPING` 함수를 사용해야 합니다.

**예시**

다음 두 쿼리는 동일합니다.

```sql
-- Query 1
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;

-- Query 2
SELECT year, month, day, count(*) FROM t GROUP BY
GROUPING SETS
(
    (year, month, day),
    (year, month),
    (year),
    ()
);
```

**관련 항목**

* SQL 표준과의 호환성을 위한 [group&#95;by&#95;use&#95;nulls](/ko/operations/settings/settings.md#group_by_use_nulls) 설정.

<div id="implementation-details">
  ## 구현 세부 정보
</div>

집계는 컬럼 지향 DBMS의 가장 중요한 기능 중 하나이므로, 그 구현 역시 ClickHouse에서 가장 고도로 최적화된 부분 중 하나입니다. 기본적으로 집계는 해시 테이블을 사용해 메모리 내에서 수행됩니다. 이를 위해 &quot;그룹화 키&quot; 데이터 타입에 따라 자동으로 선택되는 40개 이상의 특수화가 마련되어 있습니다.

<div id="group-by-optimization-depending-on-table-sorting-key">
  ### 테이블 정렬 키에 따른 GROUP BY 최적화
</div>

테이블(table)이 특정 키를 기준으로 정렬되어 있고 `GROUP BY` 표현식에 정렬 키(sorting key)의 접두사(prefix) 또는 단사 함수가 하나 이상 포함되어 있으면 집계를 더 효율적으로 수행할 수 있습니다. 이 경우 테이블에서 새 키를 읽을 때마다 집계의 중간 결과를 확정하여 클라이언트로 보낼 수 있습니다. 이 동작은 [optimize&#95;aggregation&#95;in&#95;order](../../../operations/settings/settings.md#optimize_aggregation_in_order) 설정으로 활성화됩니다. 이러한 최적화는 집계 중 메모리 사용량을 줄여 주지만, 경우에 따라 쿼리 실행이 느려질 수 있습니다.

<div id="group-by-in-external-memory">
  ### 외부 메모리에서의 GROUP BY
</div>

`GROUP BY` 수행 중 메모리 사용량을 제한하기 위해 임시 데이터를 디스크에 덤프하도록 설정할 수 있습니다.
[max&#95;bytes&#95;before&#95;external&#95;group&#95;by](/ko/operations/settings/settings#max_bytes_before_external_group_by) 설정은 `GROUP BY` 임시 데이터를 파일 시스템에 덤프할 RAM 사용량 임계값을 결정합니다. 0으로 설정하면(기본값) 비활성화됩니다.
또는 [max&#95;bytes&#95;ratio&#95;before&#95;external&#95;group&#95;by](/ko/operations/settings/settings#max_bytes_ratio_before_external_group_by)를 설정할 수도 있습니다. 이 설정을 사용하면 쿼리의 메모리 사용량이 특정 임계값에 도달한 경우에만 외부 메모리에서 `GROUP BY`를 사용합니다.

`max_bytes_before_external_group_by`를 사용할 때는 `max_memory_usage`를 약 2배로 설정하는 것을 권장합니다(또는 `max_bytes_ratio_before_external_group_by=0.5`). 이는 집계에 2단계가 있기 때문입니다. 즉, 데이터를 읽고 중간 데이터를 생성하는 단계(1)와 중간 데이터를 병합하는 단계(2)입니다. 파일 시스템으로 데이터를 덤프할 수 있는 시점은 1단계뿐입니다. 임시 데이터가 덤프되지 않았다면 2단계에서 1단계와 거의 같은 양의 메모리가 필요할 수 있습니다.

예를 들어 [max&#95;memory&#95;usage](/ko/operations/settings/settings#max_memory_usage)를 10000000000으로 설정했고 외부 집계를 사용하려는 경우, `max_bytes_before_external_group_by`를 10000000000으로, `max_memory_usage`를 20000000000으로 설정하는 것이 적절합니다. 외부 집계가 트리거되면(즉, 임시 데이터가 한 번 이상 덤프된 경우) RAM 최대 사용량은 `max_bytes_before_external_group_by`보다 약간 큰 수준에 머뭅니다.

분산 쿼리 처리에서는 외부 집계가 원격 서버에서 수행됩니다. 요청 서버가 적은 양의 RAM만 사용하도록 하려면 `distributed_aggregation_memory_efficient`를 1로 설정하십시오.

디스크에 플러시된 데이터를 병합할 때와 `distributed_aggregation_memory_efficient` 설정이 활성화된 상태에서 원격 서버의 결과를 병합할 때는 전체 RAM 용량 중 최대 `1/256 * the_number_of_threads`를 사용합니다.

외부 집계가 활성화된 상태에서도 데이터 양이 `max_bytes_before_external_group_by`보다 적었다면(즉, 데이터가 플러시되지 않았다면) 쿼리는 외부 집계를 사용하지 않을 때와 동일한 속도로 실행됩니다. 임시 데이터가 한 번이라도 플러시되었다면 실행 시간은 몇 배 정도 더 길어집니다(대략 3배).

`GROUP BY` 뒤에 [LIMIT](/ko/sql-reference/statements/select/limit.md)가 있는 [ORDER BY](/ko/sql-reference/statements/select/order-by.md)가 있다면, 사용되는 RAM 양은 전체 테이블이 아니라 `LIMIT`에 포함되는 데이터 양에 따라 달라집니다. 하지만 `ORDER BY`에 `LIMIT`가 없다면 외부 정렬(`max_bytes_before_external_sort`)도 활성화해야 합니다.