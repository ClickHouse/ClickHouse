---
description: 'NOT IN, GLOBAL IN, GLOBAL NOT IN 연산자를 제외한 IN 연산자에 대한 문서입니다. 해당 연산자들은 별도로 설명합니다.'
slug: /sql-reference/operators/in
title: 'IN 연산자'
doc_type: '참고'
---

`IN`, `NOT IN`, `GLOBAL IN`, `GLOBAL NOT IN` 연산자는 기능이 매우 다양하므로 별도로 설명합니다.

연산자의 왼쪽은 단일 컬럼 또는 Tuple입니다.

예시:

```sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

왼쪽이 인덱스에 포함된 단일 컬럼이고 오른쪽이 상수 집합인 경우, 시스템은 쿼리 처리 시 인덱스를 활용합니다.

너무 많은 값을 명시적으로 나열하지 마십시오(예: 수백만 개). 데이터 집합이 큰 경우, 임시 테이블에 넣은 다음([쿼리 처리를 위한 외부 데이터](../../engines/table-engines/special/external-data.md) 섹션 참조) 서브쿼리를 사용하십시오.

연산자의 오른쪽에는 상수 표현식의 집합, 상수 표현식으로 구성된 튜플의 집합(위의 예시 참조), 데이터베이스 테이블 이름, 또는 괄호 안의 `SELECT` 서브쿼리를 사용할 수 있습니다.

하위 호환성을 위해, 오른쪽이 단일 `tuple` 표현식인 경우 `IN` 연산자의 왼쪽에 따라 값의 집합 또는 하나의 tuple value로 해석될 수 있습니다. 왼쪽이 스칼라 값이면 ClickHouse는 이 단일 오른쪽 `tuple` 표현식의 각 요소를 별개의 `IN` 값으로 처리합니다:

```sql title="Query"
SELECT
    1 IN (tuple(1, 2)) AS one_in_tuple,
    2 IN (tuple(1, 2)) AS two_in_tuple,
    3 IN (tuple(1, 2)) AS three_in_tuple;
```

```text title="Response"
┌─one_in_tuple─┬─two_in_tuple─┬─three_in_tuple─┐
│            1 │            1 │              0 │
└──────────────┴──────────────┴────────────────┘
```

이는 `SELECT 1 IN (1, 2)`와 동일하게 동작합니다. 왼쪽이 튜플인 경우, 오른쪽은 튜플 값의 집합으로 해석됩니다:

```sql title="Query"
SELECT tuple(1, 2) IN (tuple(1, 2)) AS tuple_in_tuple;
```

```text title="Response"
┌─tuple_in_tuple─┐
│              1 │
└────────────────┘
```

이 특수 처리는 오른쪽이 단일 `tuple` 표현식일 때만 적용됩니다. 스칼라 왼쪽 값은 여러 tuple 값을 포함하는 오른쪽과 매칭할 수 없습니다:

```sql title="Query"
SELECT 1 IN (tuple(1, 2), tuple(3, 4));
```

```text title="Response"
Code: 43. DB::Exception: Unsupported types for IN. First argument type UInt8. Second argument type Tuple(Tuple(UInt8, UInt8), Tuple(UInt8, UInt8)). (ILLEGAL_TYPE_OF_ARGUMENT)
```

ClickHouse는 `IN` 서브쿼리의 좌측과 우측 타입이 서로 달라도 허용합니다.
이 경우, 우측에 [accurateCastOrNull](/ko/sql-reference/functions/type-conversion-functions#accurateCastOrNull) 함수가 적용된 것처럼 우측 값을 좌측의 타입으로 변환합니다.

이는 데이터 타입이 [널 허용](../../sql-reference/data-types/nullable.md)으로 변환됨을 의미하며, 변환을 수행할 수 없는 경우 [NULL](/ko/operations/settings/formats#input_format_null_as_default)을 반환합니다.

**예시**

```sql title="Query"
SELECT '1' IN (SELECT 1);
```

```text title="Response"
┌─in('1', _subquery49)─┐
│                    1 │
└──────────────────────┘
```

연산자의 오른쪽이 테이블 이름인 경우(예: `UserID IN users`), 이는 서브쿼리 `UserID IN (SELECT * FROM users)`와 동일합니다. 쿼리와 함께 전송되는 외부 데이터를 처리할 때 이 방식을 사용하십시오. 예를 들어, 필터링이 필요한 사용자 ID 집합을 &#39;users&#39; 임시 테이블에 로드한 후 쿼리와 함께 전송할 수 있습니다.

연산자의 오른쪽이 Set 엔진을 사용하는 테이블 이름(항상 RAM에 유지되는 준비된 데이터 집합)인 경우, 데이터 집합은 쿼리마다 새로 생성되지 않습니다.

서브쿼리는 튜플 필터링을 위해 두 개 이상의 컬럼을 지정할 수 있습니다.

예시:

```sql title="Query"
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

`IN` 연산자의 왼쪽과 오른쪽에 있는 컬럼은 동일한 타입이어야 합니다.

`IN` 연산자와 서브쿼리는 집계 함수와 람다 함수를 포함해 쿼리의 어느 위치에서든 사용할 수 있습니다.
예시:

```sql title="Query"
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

```text title="Response"
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

3월 17일 이후의 각 날짜별로, 3월 17일에 사이트를 방문한 사용자가 발생시킨 페이지뷰의 비율을 계산합니다.
`IN` 절의 서브쿼리는 항상 단일 서버에서 한 번만 실행됩니다. 종속된 서브쿼리는 없습니다.

<div id="null-processing">
  ## NULL 처리
</div>

요청을 처리하는 동안 `IN` 연산자는 [NULL](/ko/operations/settings/formats#input_format_null_as_default)과의 연산 결과가 `NULL`이 연산자의 오른쪽에 있든 왼쪽에 있든 관계없이 항상 `0`과 같다고 가정합니다. `NULL` 값은 어떤 데이터셋에도 포함되지 않으며, 서로 대응하지도 않고, [transform&#95;null&#95;in = 0](../../operations/settings/settings.md#transform_null_in)인 경우 비교할 수도 없습니다.

다음은 `t_null` 테이블을 사용한 예시입니다:

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

쿼리 `SELECT x FROM t_null WHERE y IN (NULL,3)`를 실행하면 다음 결과가 반환됩니다:

```text
┌─x─┐
│ 2 │
└───┘
```

`y = NULL`인 행이 쿼리 결과에서 제외되는 것을 확인할 수 있습니다. 이는 ClickHouse가 `NULL`이 `(NULL,3)` 집합에 포함되는지 판단할 수 없어서 연산 결과로 `0`을 반환하고, `SELECT`가 이 행을 최종 결과에서 제외하기 때문입니다.

```sql
SELECT y IN (NULL, 3)
FROM t_null
```

```text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

<div id="distributed-subqueries">
  ## 분산 서브쿼리
</div>

서브쿼리와 함께 사용하는 `IN` 연산자에는 두 가지 옵션이 있습니다(`JOIN` 연산자와 유사): 일반 `IN` / `JOIN`과 `GLOBAL IN` / `GLOBAL JOIN`입니다. 두 옵션은 분산 쿼리 처리 시 실행 방식에 차이가 있습니다.

:::note
아래에 설명된 알고리즘은 [설정](../../operations/settings/settings.md)의 `distributed_product_mode` 설정값에 따라 동작 방식이 달라질 수 있습니다.
:::

일반 `IN`을 사용하면 쿼리가 원격 서버로 전송되고, 각 서버는 `IN` 또는 `JOIN` 절의 서브쿼리를 실행합니다.

`GLOBAL IN` / `GLOBAL JOIN`을 사용하면, 먼저 `GLOBAL IN` / `GLOBAL JOIN`에 대한 모든 서브쿼리가 실행되고 그 결과가 임시 테이블에 수집됩니다. 이후 임시 테이블이 각 원격 서버로 전송되고, 해당 서버에서는 이 임시 데이터를 사용하여 쿼리가 실행됩니다.

`GLOBAL ... JOIN`의 경우, 조인의 어느 쪽이 서브쿼리로 계산되는지는 조인 종류에 따라 달라집니다. `LEFT` 및 `INNER` 조인에서는 오른쪽 테이블이 계산되며, `RIGHT` 조인에서는 오른쪽 테이블이 보존되는 쪽이므로 세그먼트에서 읽어야 하기 때문에 왼쪽 테이블이 대신 계산됩니다.

분산 쿼리가 아닌 경우 일반 `IN` / `JOIN`을 사용하십시오.

분산 쿼리 처리 시 `IN` / `JOIN` 절에서 서브쿼리를 사용할 때는 주의하십시오.

몇 가지 예시를 살펴보겠습니다. 클러스터의 각 서버에 일반 **local&#95;table**이 있다고 가정합니다. 또한 각 서버에는 클러스터의 모든 서버를 참조하는 **Distributed**(분산) 유형의 **distributed&#95;table** 테이블도 있습니다.

**distributed&#95;table**에 대한 쿼리는 모든 원격 서버로 전송되며, 각 서버에서 **local&#95;table**을 사용하여 실행됩니다.

예를 들어, 다음 쿼리를

```sql
SELECT uniq(UserID) FROM distributed_table
```

모든 원격 서버에 다음과 같이 전송됩니다

```sql
SELECT uniq(UserID) FROM local_table
```

각각에서 병렬로 실행되며, 중간 결과를 결합할 수 있는 단계에 도달할 때까지 계속됩니다. 이후 중간 결과는 요청 서버로 반환되어 머지되고, 최종 결과가 클라이언트로 전송됩니다.

이제 `IN`을 사용한 쿼리를 살펴보겠습니다:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

* 두 사이트의 이용자 교집합 계산.

이 쿼리는 다음과 같이 모든 원격 서버로 전송됩니다

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

즉, `IN` 절의 데이터 집합은 각 서버에 로컬로 저장된 데이터만을 대상으로 각 서버에서 독립적으로 수집됩니다.

이 방식은 해당 상황을 미리 고려하여 단일 UserID의 데이터가 하나의 서버에 완전히 저장되도록 클러스터 서버 전체에 데이터를 분산해 둔 경우에만 올바르고 최적으로 동작합니다. 이 경우 필요한 모든 데이터를 각 서버에서 로컬로 조회할 수 있습니다. 그렇지 않으면 결과가 부정확해집니다. 이러한 쿼리 변형을 &quot;local IN&quot;이라고 합니다.

데이터가 클러스터 서버 전체에 무작위로 분산되어 있을 때 쿼리가 올바르게 동작하게 하려면, 서브쿼리 내부에 **distributed&#95;table**을 지정하십시오. 쿼리는 다음과 같습니다:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

이 쿼리는 다음과 같이 모든 원격 서버로 전송됩니다

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

서브쿼리는 각 원격 서버에서 실행됩니다. 서브쿼리가 분산 테이블을 사용하므로, 각 원격 서버의 서브쿼리는 다음과 같이 모든 원격 서버로 재전송됩니다:

```sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

예를 들어, 100대의 서버로 구성된 클러스터에서 전체 쿼리를 실행하면 10,000개의 기본 요청이 필요하며, 이는 일반적으로 허용할 수 없는 수준입니다.

이러한 경우에는 `IN` 대신 항상 `GLOBAL IN`을 사용해야 합니다. 다음 쿼리에서 어떻게 동작하는지 살펴보겠습니다:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

요청자 서버가 서브쿼리를 실행합니다:

```sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

결과는 RAM의 임시 테이블(temporary table)에 저장됩니다. 이후 각 원격 서버로 다음과 같이 요청(request)이 전송됩니다:

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

임시 테이블(temporary table) `_data1`은 쿼리와 함께 모든 원격 서버로 전송됩니다(임시 테이블 이름은 구현에 따라 달라집니다).

이는 일반 `IN`을 사용하는 것보다 더 효율적입니다. 하지만 다음 사항에 유의하십시오.

1. 임시 테이블을 생성할 때 데이터는 중복 제거되지 않습니다. 네트워크를 통해 전송되는 데이터 양을 줄이려면 서브쿼리에 DISTINCT를 지정하십시오. (일반 `IN`에서는 이렇게 할 필요가 없습니다.)
2. 임시 테이블은 모든 원격 서버로 전송됩니다. 이때 전송에서는 네트워크 토폴로지를 고려하지 않습니다. 예를 들어 10개의 원격 서버가 요청 서버에서 매우 멀리 떨어진 데이터 센터에 있는 경우, 해당 원격 데이터 센터로 연결되는 채널을 통해 데이터가 10번 전송됩니다. `GLOBAL IN`을 사용할 때는 큰 데이터 집합을 피하는 것이 좋습니다.
3. 데이터를 원격 서버로 전송할 때는 네트워크 대역폭 제한을 구성할 수 없습니다. 네트워크에 과부하를 일으킬 수 있습니다.
4. 평소에 `GLOBAL IN`을 사용할 필요가 없도록 데이터를 서버 전반에 분산시키십시오.
5. `GLOBAL IN`을 자주 사용해야 한다면, 레플리카 그룹 하나가 빠른 네트워크로 연결된 단일 데이터 센터에만 존재하도록 ClickHouse 클러스터의 위치를 설계하여, 쿼리가 하나의 데이터 센터 내에서 완전히 처리되도록 하십시오.

또한 이 로컬 테이블이 요청 서버에서만 사용 가능하고 그 데이터를 원격 서버에서도 사용하려는 경우에는 `GLOBAL IN` 절에 로컬 테이블을 지정하는 것도 적절합니다.

<div id="distributed-subqueries-and-max_rows_in_set">
  ### 분산 서브쿼리와 max_rows_in_set
</div>

분산 쿼리에서 전송되는 데이터 양을 제어하려면 [`max_rows_in_set`](/ko/operations/settings/settings#max_rows_in_set) 및 [`max_bytes_in_set`](/ko/operations/settings/settings#max_bytes_in_set)을 사용할 수 있습니다.

특히 `GLOBAL IN` 쿼리가 대량의 데이터를 반환할 때 매우 중요합니다. 다음 SQL을 살펴보십시오:

```sql
SELECT * FROM table1 WHERE col1 GLOBAL IN (SELECT col1 FROM table2 WHERE <some_predicate>)
```

`some_predicate`의 선택도가 충분히 높지 않으면 많은 양의 데이터를 반환해 성능 문제를 일으킬 수 있습니다. 이런 경우 네트워크를 통한 데이터 전송량을 제한하는 것이 좋습니다. 또한 [`set_overflow_mode`](/ko/operations/settings/settings#set_overflow_mode)는 `throw`(기본값)로 설정되어 있으므로, 이러한 임계값에 도달하면 예외가 발생한다는 점에도 유의하십시오.

<div id="distributed-subqueries-and-max_parallel_replicas">
  ### 분산 서브쿼리와 max_parallel_replicas
</div>

[max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas)가 1보다 크면 분산 쿼리가 한 번 더 변환됩니다.

예를 들어, 다음과 같습니다:

```sql
SELECT CounterID, count() FROM distributed_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS max_parallel_replicas=3
```

각 서버에서 다음과 같이 변환됩니다:

```sql
SELECT CounterID, count() FROM local_table_1 WHERE UserID IN (SELECT UserID FROM local_table_2 WHERE CounterID < 100)
SETTINGS parallel_replicas_count=3, parallel_replicas_offset=M
```

여기서 `M`은 로컬 쿼리가 실행되는 레플리카에 따라 `1`에서 `3` 사이의 값입니다.

이 설정은 쿼리에 포함된 모든 MergeTree 계열 테이블에 영향을 미치며, 각 테이블에 `SAMPLE 1/3 OFFSET (M-1)/3`를 적용한 것과 동일한 효과를 냅니다.

따라서 [max&#95;parallel&#95;replicas](#distributed-subqueries-and-max_parallel_replicas) 설정은 두 테이블이 동일한 복제 방식으로 구성되어 있고 UserID 또는 그 하위 키를 기준으로 샘플링되는 경우에만 올바른 결과를 생성합니다. 특히 `local_table_2`에 샘플링 키가 없으면 잘못된 결과가 생성됩니다. 동일한 규칙이 `JOIN`에도 적용됩니다.

`local_table_2`가 요구 사항을 충족하지 않는 경우의 한 가지 우회 방법은 `GLOBAL IN` 또는 `GLOBAL JOIN`을 사용하는 것입니다.

테이블에 샘플링 키가 없는 경우, [parallel&#95;replicas&#95;custom&#95;key](/ko/operations/settings/settings#parallel_replicas_custom_key)의 더 유연한 옵션을 사용할 수 있으며, 이에 따라 서로 다른 더 최적화된 동작을 얻을 수 있습니다.