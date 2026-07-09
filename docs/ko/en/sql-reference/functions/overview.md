---
description: '일반 함수에 대한 문서'
sidebar_label: '개요'
sidebar_position: 1
slug: /sql-reference/functions/overview
title: '일반 함수'
doc_type: 'reference'
---

함수에는 최소* 두 가지 유형이 있습니다. 일반 함수(보통 &quot;함수&quot;라고만 부름)와 집계 함수입니다. 이 둘은 완전히 다른 개념입니다. 일반 함수는 각 행에 개별적으로 적용되는 것처럼 동작합니다(각 행에서 함수의 결과는 다른 행에 영향을 받지 않습니다). 집계 함수는 여러 행의 값 집합을 누적합니다(즉, 전체 행 집합에 의존합니다).

이 절에서는 일반 함수에 대해 설명합니다. 집계 함수는 &quot;집계 함수&quot; 절을 참조하십시오.

:::note
세 번째 유형의 함수도 있으며, [`arrayJoin` 함수](../functions/array-join.md)가 여기에 속합니다. 또한 [테이블 함수](../table-functions/index.md)도 별도로 언급할 수 있습니다.
:::

<div id="strong-typing">
  ## 강한 타입 시스템
</div>

표준 SQL과 달리 ClickHouse는 강한 타입 시스템을 사용합니다. 다시 말해, 타입 간 암시적 변환을 수행하지 않습니다. 각 함수는 특정 타입 집합에서만 동작합니다. 따라서 경우에 따라 타입 변환 함수를 사용해야 합니다.

<div id="common-subexpression-elimination">
  ## 공통 부분표현식 제거
</div>

동일한 AST(동일한 레코드 또는 구문 parsing 결과가 같은 경우)를 가진 쿼리 내 모든 표현식은 동일한 값을 갖는 것으로 간주됩니다. 이러한 표현식은 하나로 결합되어 한 번만 실행됩니다. 동일한 서브쿼리도 같은 방식으로 제거됩니다.

<div id="types-of-results">
  ## 결과 타입
</div>

모든 함수는 결과로 단일 값을 반환합니다(여러 값을 반환하지 않으며, 값을 전혀 반환하지 않는 경우도 없습니다). 결과 타입은 일반적으로 값이 아니라 인수의 타입에 의해서만 정의됩니다. 예외는 tupleElement 함수(`a.N` 연산자)와 toFixedString 함수입니다.

<div id="constants">
  ## 상수
</div>

설명을 단순하게 하기 위해 일부 함수는 특정 인수에 대해서만 상수를 사용할 수 있습니다. 예를 들어, LIKE 연산자의 오른쪽 인수는 상수여야 합니다.
거의 모든 함수는 상수 인수에 대해 상수를 반환합니다. 예외는 난수를 생성하는 함수입니다.
&#39;now&#39; 함수는 서로 다른 시점에 실행된 쿼리마다 다른 값을 반환하지만, 상수성은 단일 쿼리 내에서만 중요하므로 그 결과는 상수로 간주됩니다.
상수 표현식도 상수로 간주됩니다(예를 들어, LIKE 연산자의 오른쪽 부분은 여러 상수를 조합해 만들 수 있습니다).

함수는 상수 인수와 비상수 인수에 대해 서로 다른 방식으로 구현될 수 있습니다(즉, 서로 다른 코드가 실행됩니다). 하지만 상수에 대한 결과와 동일한 값만 포함하는 실제 컬럼에 대한 결과는 서로 일치해야 합니다.

<div id="null-processing">
  ## NULL 처리
</div>

함수는 다음과 같은 방식으로 동작합니다.

* 함수의 인수 중 하나 이상이 `NULL`이면 함수 결과도 `NULL`입니다.
* 각 함수 설명에 개별적으로 지정된 특별한 동작이 있습니다. ClickHouse 소스 코드에서 이러한 함수에는 `UseDefaultImplementationForNulls=false`가 설정되어 있습니다.

<div id="constancy">
  ## 상수성
</div>

함수는 인수의 값을 직접 변경할 수 없으며, 변경된 값은 결과로 반환될 뿐입니다. 따라서 개별 함수를 계산한 결과는 쿼리에서 함수가 작성된 순서와 무관합니다.

<div id="higher-order-functions">
  ## 고차 함수
</div>

<div id="arrow-operator-and-lambda">
  ### `->` 연산자와 lambda(params, expr) 함수
</div>

고차 함수는 함수형 인수로 람다 함수만 받을 수 있습니다. 람다 함수를 고차 함수에 전달하려면 `->` 연산자를 사용합니다. 화살표의 왼쪽에는 형식 매개변수가 오며, 이는 임의의 ID 하나이거나 튜플 안의 여러 ID일 수 있습니다. 화살표의 오른쪽에는 이러한 형식 매개변수와 모든 테이블 컬럼을 사용할 수 있는 표현식이 옵니다.

예시:

```python
x -> 2 * x
str -> str != Referer
```

여러 인수를 받는 람다 함수도 고차 함수에 전달할 수 있습니다. 이 경우 고차 함수에는 해당 인수에 대응하는, 길이가 동일한 여러 배열이 전달됩니다.

일부 함수에서는 첫 번째 인수(람다 함수)를 생략할 수 있습니다. 이 경우 동일한 매핑이 사용된다고 가정합니다.

<div id="bare-function-names-as-lambdas">
  ### 람다로 사용하는 함수 이름
</div>

전체 람다 표현식을 작성하는 대신 함수 이름을 고차 함수에 직접 전달할 수 있습니다. 함수 이름은 자동으로 동일한 람다 표현식으로 변환됩니다.

예를 들어, 다음 쌍은 서로 같습니다:

```sql
SELECT arrayMap(negate, [1, 2, 3]);            -- [-1, -2, -3]
SELECT arrayMap(x -> negate(x), [1, 2, 3]);    -- [-1, -2, -3]

SELECT arrayMap(plus, [1, 2, 3], [10, 20, 30]);            -- [11, 22, 33]
SELECT arrayMap((x, y) -> plus(x, y), [1, 2, 3], [10, 20, 30]); -- [11, 22, 33]

SELECT arrayFilter(isNotNull, [1, NULL, 3, NULL, 5]);            -- [1, 3, 5]
SELECT arrayFilter(x -> isNotNull(x), [1, NULL, 3, NULL, 5]);    -- [1, 3, 5]

SELECT arrayFold(plus, [1, 2, 3, 4, 5], toUInt64(0));                      -- 15
SELECT arrayFold((acc, x) -> plus(acc, x), [1, 2, 3, 4, 5], toUInt64(0));  -- 15
```

이는 내장 함수, SQL UDF, 실행형 UDF, WebAssembly UDF에 적용됩니다. 모호한 경우에는 함수 이름보다 컬럼과 alias 이름이 우선합니다.

람다의 arity는 내부 함수에서 결정됩니다. 예를 들어 `arrayMap(plus, ...)`는 `plus`가 두 개의 인수를 받으므로 arity 2를 사용합니다. 따라서 `arrayMap(plus, [(1, 10), (2, 20)])`처럼 튜플 입력에도 사용할 수 있으며, 이 경우 튜플 요소는 람다 인수로 언패킹됩니다.

가변 인수 내부 함수(예: 개수에 관계없이 인수를 받을 수 있는 `concat`)의 경우, 람다 arity는 배열 인수의 개수로 결정됩니다. 이는 `arrayMap`, `arrayFilter`, `arrayFold` 같은 고차 함수(higher-order function)에서는 올바르게 동작합니다. 하지만 배열과 함께 고정된 비배열 매개변수도 받는 고차 함수(예: `arrayPartialSort(f, limit, arr)`)에서는 가변 인수 함수 이름만 그대로 사용하면 잘못된 arity가 적용될 수 있으므로, 이 경우 명시적인 람다가 필요합니다.

가변 인수 내부 함수는 튜플 입력도 자동으로 언패킹하지 않습니다. 예를 들어 `arrayMap(concat, [('a', 'b'), ('c', 'd')])`는 단항 람다로 재작성되며, `arrayMap((x, y) -> concat(x, y), [('a', 'b'), ('c', 'd')])`와 동일하지 않습니다. 튜플 요소를 구조 분해해 가변 인수 호출에 전달하려면 명시적인 람다를 사용하십시오.

<div id="user-defined-functions-udfs">
  ## 사용자 정의 함수(UDFs)
</div>

ClickHouse는 사용자 정의 함수를 지원합니다. 자세한 내용은 [UDFs](../functions/udf.md)를 참조하십시오.