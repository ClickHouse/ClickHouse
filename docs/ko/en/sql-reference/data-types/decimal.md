---
description: '정밀도를 설정할 수 있는 고정소수점 연산을 제공하는 ClickHouse의 Decimal 데이터 타입 문서'
sidebar_label: 'Decimal'
sidebar_position: 6
slug: /sql-reference/data-types/decimal
title: 'Decimal, Decimal(P), Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S),
  Decimal256(S)'
doc_type: 'reference'
---

덧셈, 뺄셈, 곱셈 연산에서는 정밀도를 유지하는 부호 있는 고정소수점 수입니다. 나눗셈에서는 최하위 자릿수를 버리며(반올림하지 않음).

<div id="parameters">
  ## 매개변수
</div>

* P - 정밀도입니다. 유효 범위: [ 1 : 76 ]입니다. 숫자가 가질 수 있는 전체 10진수 자릿수(소수 부분 포함)를 결정합니다. 기본 정밀도은 10입니다.
* S - 소수 자릿수입니다. 유효 범위: [ 0 : P ]입니다. 소수 부분이 가질 수 있는 10진수 자릿수를 결정합니다.

Decimal(P)는 Decimal(P, 0)과 동일합니다. 마찬가지로 구문 Decimal은 Decimal(10, 0)과 동일합니다.

P 매개변수 값에 따라 Decimal(P, S)는 다음의 동의어로 사용됩니다.

* P가 [ 1 : 9 ] 범위인 경우 - Decimal32(S)
* P가 [ 10 : 18 ] 범위인 경우 - Decimal64(S)
* P가 [ 19 : 38 ] 범위인 경우 - Decimal128(S)
* P가 [ 39 : 76 ] 범위인 경우 - Decimal256(S)

<div id="decimal-value-ranges">
  ## Decimal 값 범위
</div>

* Decimal(P, S) - ( -1 * 10^(P - S), 1 * 10^(P - S) )
* Decimal32(S) - ( -1 * 10^(9 - S), 1 * 10^(9 - S) )
* Decimal64(S) - ( -1 * 10^(18 - S), 1 * 10^(18 - S) )
* Decimal128(S) - ( -1 * 10^(38 - S), 1 * 10^(38 - S) )
* Decimal256(S) - ( -1 * 10^(76 - S), 1 * 10^(76 - S) )

예를 들어, Decimal32(4)는 -99999.9999부터 99999.9999까지의 숫자를 0.0001 간격으로 표현할 수 있습니다.

<div id="internal-representation">
  ## 내부 표현
</div>

내부적으로 데이터는 각 비트 폭에 해당하는 일반적인 부호 있는 정수로 표현됩니다. 메모리에 저장할 수 있는 실제 값의 범위는 위에 명시된 범위보다 약간 더 넓지만, 이는 문자열에서 변환할 때만 검사됩니다.

최신 CPU는 128비트 및 256비트 정수를 네이티브로 지원하지 않으므로 Decimal128 및 Decimal256에 대한 연산은 에뮬레이션됩니다. 따라서 Decimal128 및 Decimal256은 Decimal32/Decimal64보다 훨씬 느리게 동작합니다.

<div id="operations-and-result-type">
  ## 연산 및 결과 유형
</div>

Decimal에 대한 이항 연산의 결과는 더 큰 결과 유형이 됩니다(인수 순서와 관계없음).

* `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
* `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
* `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`
* `Decimal256(S1) <op> Decimal<32|64|128>(S2) -> Decimal256(S)`

소수 자릿수(scale)에 대한 규칙:

* 덧셈, 뺄셈: S = max(S1, S2).
* 곱셈: S = S1 + S2.
* 나눗셈: S = S1.

Decimal과 정수 사이의 유사한 연산에서는 결과가 Decimal 인수와 동일한 크기의 Decimal이 됩니다.

Decimal과 Float32/Float64 사이의 연산은 정의되어 있지 않습니다. 필요하다면 toDecimal32, toDecimal64, toDecimal128 또는 toFloat32, toFloat64 내장 함수를 사용해 인수 중 하나를 명시적으로 변환할 수 있습니다. 이 경우 정밀도가 손실될 수 있으며, 형 변환은 계산 비용이 큰 연산이라는 점에 유의하십시오.

Decimal에 대한 일부 함수는 결과를 Float64로 반환합니다(예: var 또는 stddev). 중간 계산은 여전히 Decimal로 수행될 수 있으므로, 같은 값을 가진 Float64 입력과 Decimal 입력의 결과가 서로 다를 수 있습니다.

<div id="overflow-checks">
  ## 오버플로우 검사
</div>

Decimal 계산 중에는 정수 오버플로우가 발생할 수 있습니다. 소수부의 자릿수가 너무 많으면 버려지며(반올림되지 않음), 정수부의 자릿수가 너무 많으면 예외가 발생합니다.

:::warning
Decimal128 및 Decimal256에는 오버플로우 검사가 구현되어 있지 않습니다. 오버플로우가 발생하면 잘못된 결과가 반환되며, 예외는 발생하지 않습니다.
:::

```sql
SELECT toDecimal32(2, 4) AS x, x / 3
```

```text
┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
│ 2.0000 │                       0.6666 │
└────────┴──────────────────────────────┘
```

```sql
SELECT toDecimal32(4.2, 8) AS x, x * x
```

```text
DB::Exception: Scale is out of bounds.
```

```sql
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

```text
DB::Exception: Decimal math overflow.
```

오버플로우 검사를 수행하면 작업 속도가 저하됩니다. 오버플로우가 발생할 가능성이 없다는 것을 알고 있다면 `decimal_check_overflow` 설정을 사용해 검사를 비활성화하는 것이 좋습니다. 검사를 비활성화한 상태에서 오버플로우가 발생하면 결과가 올바르지 않을 수 있습니다:

```sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

```text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

오버플로우 검사는 산술 연산뿐 아니라 값 비교 시에도 수행됩니다:

```sql
SELECT toDecimal32(1, 8) < 100
```

```text
DB::Exception: Can't compare.
```

**관련 항목**

* [isDecimalOverflow](/ko/sql-reference/functions/other-functions#isDecimalOverflow)
* [countDigits](/ko/sql-reference/functions/other-functions#countDigits)