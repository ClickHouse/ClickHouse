---
description: 'ClickHouse의 부동소수점 데이터 타입에 대한 문서: Float32,
  Float64 및 BFloat16'
sidebar_label: 'Float32 | Float64 | BFloat16'
sidebar_position: 4
slug: /sql-reference/data-types/float
title: 'Float32 | Float64 | BFloat16 타입'
doc_type: '참고'
---

:::note
정확한 계산이 필요하다면, 특히 높은 정밀도가 필요한 금융 또는 비즈니스 데이터를 다루는 경우에는 [Decimal](../data-types/decimal.md) 사용을 고려하십시오.

[Floating Point Numbers](https://en.wikipedia.org/wiki/IEEE_754)는 아래 예시와 같이 부정확한 결과를 초래할 수 있습니다:

```sql
CREATE TABLE IF NOT EXISTS float_vs_decimal
(
   my_float Float64,
   my_decimal Decimal64(3)
)
ENGINE=MergeTree
ORDER BY tuple();

# Generate 1 000 000 random numbers with 2 decimal places and store them as a float and as a decimal
INSERT INTO float_vs_decimal SELECT round(randCanonical(), 3) AS res, res FROM system.numbers LIMIT 1000000;
```

```sql
SELECT sum(my_float), sum(my_decimal) FROM float_vs_decimal;

┌──────sum(my_float)─┬─sum(my_decimal)─┐
│ 499693.60500000004 │      499693.605 │
└────────────────────┴─────────────────┘

SELECT sumKahan(my_float), sumKahan(my_decimal) FROM float_vs_decimal;

┌─sumKahan(my_float)─┬─sumKahan(my_decimal)─┐
│         499693.605 │           499693.605 │
└────────────────────┴──────────────────────┘
```

:::

ClickHouse와 C에서 서로 대응하는 타입은 아래와 같습니다.

* `Float32` — `float`.
* `Float64` — `double`.

ClickHouse의 Float 타입에는 다음과 같은 별칭이 있습니다.

* `Float32` — `FLOAT`, `REAL`, `SINGLE`.
* `Float64` — `DOUBLE`, `DOUBLE PRECISION`.

테이블을 생성할 때 부동소수점 수의 숫자 매개변수를 지정할 수 있지만(예: `FLOAT(12)`, `FLOAT(15, 22)`, `DOUBLE(12)`, `DOUBLE(4, 18)`), ClickHouse는 이를 무시합니다.

<div id="using-floating-point-numbers">
  ## 부동소수점 수 사용하기
</div>

* 부동소수점 수를 사용한 계산에서는 반올림 오류가 발생할 수 있습니다.

{/* */ }

```sql
SELECT 1 - 0.9

┌───────minus(1, 0.9)─┐
│ 0.09999999999999998 │
└─────────────────────┘
```

* 계산 결과는 계산 방식(컴퓨터 시스템의 프로세서 유형 및 아키텍처)에 따라 달라집니다.
* 부동소수점 계산에서는 무한대(`Inf`) 및 &quot;숫자가 아님&quot;(`NaN`)과 같은 값이 나올 수 있습니다. 계산 결과를 처리할 때는 이 점을 고려해야 합니다.
* 텍스트에서 부동소수점 수를 파싱할 때 결과가 기계에서 표현 가능한 가장 가까운 값이 아닐 수 있습니다.

<div id="nan-and-inf">
  ## NaN 및 Inf
</div>

표준 SQL과 달리 ClickHouse는 다음과 같은 부동소수점 수 범주를 지원합니다.

* `Inf` – 무한대입니다.

{/* */ }

```sql
SELECT 0.5 / 0

┌─divide(0.5, 0)─┐
│            inf │
└────────────────┘
```

* `-Inf` — 음의 무한대.

{/* */ }

```sql
SELECT -0.5 / 0

┌─divide(-0.5, 0)─┐
│            -inf │
└─────────────────┘
```

* `NaN` — 숫자가 아닌 값.

{/* */ }

```sql
SELECT 0 / 0

┌─divide(0, 0)─┐
│          nan │
└──────────────┘
```

`NaN` 정렬 규칙은 [ORDER BY 절](../../sql-reference/statements/select/order-by.md) 섹션을 참조하세요.

<div id="nan-values-in-set-semantics">
  ## 집합 의미론에서의 NaN 값
</div>

IEEE 754 표준에서는 `NaN`에 대해 스칼라 비교 `NaN = NaN`이 `false`를 반환하도록 정의합니다.
ClickHouse도 `=` 연산자에 이 규칙을 따릅니다.

하지만 `NaN`은 단일한 값이 아닙니다. 지수부(exponent)가 모두 1이고
가수(mantissa)가 0이 아닌 모든 비트 패턴이 `NaN`에 해당합니다. 서로 다른 연산과 서로 다른 CPU 아키텍처에서는
부호 비트나 가수 payload가 서로 다른 `NaN`
값이 생성될 수 있습니다. 예를 들면 다음과 같습니다.

* `0./0.`은 대부분의 x86 플랫폼에서 부호 비트가 1인 `NaN`을 생성합니다.
* 리터럴 `nan`은 부호 비트가 0인 `NaN`을 생성합니다.
* [PR #98230](https://github.com/ClickHouse/ClickHouse/pull/98230) 이후 AArch64 NEON 경로의
  `log`는 음수 입력에 대해 glibc의 스칼라 `log`와 부호 비트가 다른 `NaN`을 반환합니다.

ClickHouse의 해시 테이블은 키를 바이트 단위로 비교하므로, 서로 다른 `NaN` 비트 패턴은
서로 다른 버킷으로 해시되며 집합 의미론 연산에서는 서로 다른 값으로 처리됩니다. 여기에는
`DISTINCT`, `GROUP BY`, `uniqExact`, `countDistinct`, 그리고 `Float` 키에 대한 동등 `JOIN`이 포함됩니다:

```sql
SELECT countDistinct(arrayJoin([0./0., nan, log(-1.)]));
-- May return 2 or 3 depending on architecture and build, even though all three inputs are NaN.
```

이는 IEEE 754와 일치합니다(모든 `NaN`은 자기 자신을 포함한 다른 어떤 값과도 같지 않습니다).
하지만 다소 의외일 수 있습니다. 집합 의미론 연산에서 모든 `NaN` 값을 동일하게 취급해야 한다면
쿼리에서 이를 정규화하십시오:

```sql
-- Replace every NaN with a single canonical NaN value
SELECT countDistinct(if(isNaN(x), CAST('nan' AS Float64), x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 1.

-- Or exclude NaN values from the set entirely
SELECT countDistinct(if(isNaN(x), NULL, x))
FROM (SELECT arrayJoin([0./0., nan, log(-1.)]) AS x);
-- Returns 0.
```

같은 방식은 `DISTINCT`, `GROUP BY`, `JOIN` 키에도 적용됩니다.

<div id="bfloat16">
  ## BFloat16
</div>

`BFloat16`는 8비트 지수부, 1비트 부호, 7비트 가수를 갖는 16비트 부동소수점 데이터 타입입니다.
머신 러닝 및 AI 애플리케이션에 유용합니다.

ClickHouse는 `Float32`와 `BFloat16` 간의 변환을 지원하며,
이는 [`toFloat32()`](../functions/type-conversion-functions.md/#toFloat32) 또는 [`toBFloat16`](../functions/type-conversion-functions.md/#toBFloat16) 함수를 사용해 수행할 수 있습니다.

:::note
대부분의 다른 연산은 지원되지 않습니다.
:::