---
description: 'NumericIndexedVector 및 관련 함수 문서'
sidebar_label: 'NumericIndexedVector'
slug: /sql-reference/functions/numeric-indexed-vector-functions
title: 'NumericIndexedVector 함수'
doc_type: 'reference'
---

NumericIndexedVector는 벡터를 캡슐화하고 벡터 집계 및 원소별 연산을 구현하는 추상 데이터 구조입니다. Bit-Sliced Index는 이의 저장 방식으로 사용됩니다. 이론적 배경과 사용 시나리오는 논문 [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411)을 참고하십시오.

<div id="bit-sliced-index">
  ## BSI
</div>

BSI(Bit-Sliced Index) 저장 방식에서는 데이터를 [Bit-Sliced Index](https://dl.acm.org/doi/abs/10.1145/253260.253268) 형태로 저장한 뒤 [Roaring Bitmap](https://github.com/RoaringBitmap/RoaringBitmap)으로 압축합니다. 집계 연산과 원소별 연산은 압축된 데이터에 직접 수행되므로 저장 및 쿼리 효율을 크게 높일 수 있습니다.

벡터는 인덱스와 해당 인덱스에 대응하는 값으로 구성됩니다. 다음은 BSI 저장 모드에서 이 데이터 구조가 가지는 몇 가지 특성과 제약 사항입니다:

* 인덱스 타입은 `UInt8`, `UInt16`, `UInt32` 중 하나여야 합니다. **참고:** Roaring Bitmap의 64비트 구현 성능을 고려하면 BSI 포맷은 `UInt64`/`Int64`를 지원하지 않습니다.
* 값 타입은 `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, `Float64` 중 하나일 수 있습니다. **참고:** 값 타입은 자동으로 확장되지 않습니다. 예를 들어 값 타입으로 `UInt8`를 사용하면 `UInt8`의 범위를 초과하는 합계는 더 높은 타입으로 승격되지 않고 오버플로우가 발생합니다. 마찬가지로 정수에 대한 연산은 정수 결과를 반환합니다(예: 나눗셈 결과가 자동으로 부동소수점으로 변환되지 않음). 따라서 값 타입은 미리 충분히 계획하여 설계하는 것이 중요합니다. 실제 환경에서는 부동소수점 타입(`Float32`/`Float64`)을 일반적으로 사용합니다.
* 동일한 인덱스 타입과 값 타입을 가진 두 벡터만 서로 연산할 수 있습니다.
* 기반 저장소는 Bit-Sliced Index를 사용하며, 인덱스는 비트맵에 저장됩니다. 비트맵의 구체적인 구현체로는 Roaring Bitmap이 사용됩니다. 압축률과 쿼리 성능을 극대화하려면 인덱스를 가능한 한 적은 수의 Roaring Bitmap 컨테이너에 집중시키는 것이 가장 좋습니다.
* Bit-Sliced Index 메커니즘은 값을 이진수로 변환합니다. 부동소수점 타입의 경우 이 변환에는 고정소수점 표현이 사용되므로 정밀도 손실이 발생할 수 있습니다. 정밀도는 소수부에 사용할 비트 수를 조정해 설정할 수 있으며, 기본값은 24비트로 대부분의 시나리오에서 충분합니다. `-State`와 함께 집계 함수 groupNumericIndexedVector를 사용하여 NumericIndexedVector를 구성할 때 정수부 비트 수와 소수부 비트 수를 사용자 지정할 수 있습니다.
* 인덱스는 0이 아닌 값, 0 값, 존재하지 않는 값의 세 가지 경우로 나뉩니다. NumericIndexedVector에는 0이 아닌 값과 0 값만 저장됩니다. 또한 두 NumericIndexedVector 간의 원소별 연산에서는 존재하지 않는 인덱스의 값을 0으로 간주합니다. 나눗셈의 경우 제수가 0이면 결과는 0입니다.

<div id="create-numeric-indexed-vector-object">
  ## numericIndexedVector 객체 만들기
</div>

이 구조를 만드는 방법은 두 가지입니다. 하나는 집계 함수 `groupNumericIndexedVector`를 `-State`와 함께 사용하는 것입니다.
추가 조건을 지정하려면 접미사 `-if`를 붙일 수 있습니다.
집계 함수는 해당 조건을 만족하는 행만 처리합니다.
다른 하나는 `numericIndexedVectorBuild`를 사용해 맵에서 빌드하는 것입니다.
`groupNumericIndexedVectorState` 함수는 매개변수를 통해 정수 비트 수와 소수 비트 수를 사용자 지정할 수 있지만, `numericIndexedVectorBuild`는 지원하지 않습니다.

<div id="group-numeric-indexed-vector">
  ## groupNumericIndexedVector
</div>

두 개의 데이터 컬럼으로 NumericIndexedVector를 생성하고, 모든 값의 합을 `Float64` 유형으로 반환합니다. 접미사 `State`를 추가하면 NumericIndexedVector 객체를 반환합니다.

**구문**

```sql
groupNumericIndexedVectorState(col1, col2)
groupNumericIndexedVectorState(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

**매개변수**

* `type`: String, 선택 사항입니다. 저장 포맷을 지정합니다. 현재는 `'BSI'`만 지원합니다.
* `integer_bit_num`: `UInt32`, 선택 사항입니다. `'BSI'` 저장 포맷에서만 유효하며, 정수부에 사용되는 비트 수를 나타내는 매개변수입니다. 인덱스 유형이 정수 타입이면 기본값은 해당 인덱스를 저장하는 데 사용되는 비트 수와 같습니다. 예를 들어 인덱스 유형이 UInt16이면 기본 `integer_bit_num` 값은 16입니다. Float32 및 Float64 인덱스 유형의 경우 `integer_bit_num`의 기본값은 40이므로, 표현할 수 있는 데이터의 정수부 범위는 `[-2^39, 2^39 - 1]`입니다. 허용 범위는 `[0, 64]`입니다.
* `fraction_bit_num`: `UInt32`, 선택 사항입니다. `'BSI'` 저장 포맷에서만 유효하며, 소수부에 사용되는 비트 수를 나타내는 매개변수입니다. 값 유형이 정수이면 기본값은 0이고, 값 유형이 Float32 또는 Float64이면 기본값은 24입니다. 유효 범위는 `[0, 24]`입니다.
* 또한 `integer_bit_num + fraction_bit_num`의 유효 범위는 `[0, 64]`여야 합니다.
* `col1`: 인덱스 컬럼입니다. 지원되는 타입: `UInt8`/`UInt16`/`UInt32`/`Int8`/`Int16`/`Int32`.
* `col2`: 값 컬럼입니다. 지원되는 타입: `Int8`/`Int16`/`Int32`/`Int64`/`UInt8`/`UInt16`/`UInt32`/`UInt64`/`Float32`/`Float64`.

**반환 값**

모든 값의 합을 나타내는 `Float64` 값입니다.

**예시**

테스트 데이터:

```text
UserID  PlayTime
1       10
2       20
3       30
```

쿼리 &amp; 결과:

```sql
SELECT groupNumericIndexedVector(UserID, PlayTime) AS num FROM t;
┌─num─┐
│  60 │
└─────┘

SELECT groupNumericIndexedVectorState(UserID, PlayTime) as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)─────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8)  │ 60                                    │
└─────┴─────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf('BSI', 32, 0)(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)──────────────────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction('BSI', 32, 0)(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────┘
```

:::note
아래 문서는 `system.functions` 시스템 테이블(system table)을 기반으로 생성되었습니다.
:::

{/* 
  아래 태그는 시스템 테이블을 기반으로 문서를 생성하는 데 사용되므로 제거해서는 안 됩니다.
  자세한 내용은 https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md 를 참조하십시오.
  */ }

{/*AUTOGENERATED_START*/ }

{/*AUTOGENERATED_END*/ }