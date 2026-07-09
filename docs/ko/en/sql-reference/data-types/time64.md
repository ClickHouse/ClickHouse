---
description: '달력 날짜 구성 요소 없이 초 미만 정밀도의 시간을 저장하는 ClickHouse
  Time64 데이터 타입 문서'
slug: /sql-reference/data-types/time64
sidebar_position: 17
sidebar_label: 'Time64'
title: 'Time64'
doc_type: 'reference'
---

데이터 타입 `Time64`는 소수 초를 포함하는 시각(time-of-day)을 나타냅니다.
이 타입에는 달력 날짜 구성 요소(일, 월, 연도)가 없습니다.
`precision` 매개변수는 소수 자릿수를 정의하며, 그에 따라 틱 크기도 결정됩니다.

틱 크기(precision): 10<sup>-precision</sup>초입니다. 유효 범위는 0..9입니다. 일반적으로 3(밀리초), 6(마이크로초), 9(나노초)를 사용합니다.

**구문:**

```sql
Time64(precision)
```

내부적으로 `Time64`는 소수 초(fractional seconds)를 나타내는 부호 있는 64비트 Decimal(Decimal64) 값을 저장합니다.
틱 해상도는 `precision` 매개변수로 결정됩니다.
시간대는 지원되지 않습니다. `Time64`에 시간대를 지정하면 오류가 발생합니다.

`DateTime64`와 달리 `Time64`는 날짜 구성 요소를 저장하지 않습니다.
관련 항목 [`Time`](../../sql-reference/data-types/time.md).

텍스트 표현 범위: `precision = 3`일 때 [-999:59:59.000, 999:59:59.999]입니다. 일반적으로 최솟값은 `-999:59:59`이고 최댓값은 `999:59:59`이며, 소수 자릿수는 최대 `precision`자리까지 지원됩니다(`precision = 9`일 때 최솟값은 `-999:59:59.999999999`입니다).

<div id="implementation-details">
  ## 구현 세부 사항
</div>

**표현**.
소수 초를 나타내는 부호 있는 `Decimal64` 값이며, 소수 자릿수는 `precision`입니다.

**정규화**.
문자열을 `Time64`로 파싱할 때 시간 구성 요소는 정규화되며 유효성 검사는 수행되지 않습니다.
예를 들어 `25:70:70`은 `26:11:10`으로 해석됩니다.

**음수 값**.
선행 마이너스 기호가 지원되며 그대로 유지됩니다.
음수 값은 일반적으로 `Time64` 값에 대한 산술 연산으로 발생합니다.
`Time64`의 경우 텍스트 입력(예: `'-01:02:03.123'`)과 숫자 입력(예: `-3723.123`) 모두에서 음수 입력이 유지됩니다.

**포화**.
구성 요소로 변환하거나 텍스트로 직렬화할 때 시각(time-of-day) 구성 요소는 [-999:59:59.xxx, 999:59:59.xxx] 범위로 제한됩니다.
저장된 숫자 값은 이 범위를 초과할 수 있지만, 구성 요소 추출(시, 분, 초)과 텍스트 표현에는 모두 포화된 값이 사용됩니다.

**시간대**.
`Time64`는 시간대를 지원하지 않습니다.
`Time64` 유형이나 값을 만들 때 시간대를 지정하면 오류가 발생합니다.
마찬가지로 `Time64` 컬럼에 시간대를 적용하거나 변경하는 것도 지원되지 않으며 오류가 발생합니다.

<div id="examples">
  ## 예시
</div>

1. `Time64` 유형의 컬럼이 있는 테이블을 생성하고 데이터를 삽입하는 예시:

```sql
CREATE TABLE tab64
(
    `event_id` UInt8,
    `time` Time64(3)
)
ENGINE = TinyLog;
```

```sql
-- Parse Time64
-- - from string,
-- - from a number of seconds since 00:00:00 (fractional part according to precision).
INSERT INTO tab64 VALUES (1, '14:30:25'), (2, 52225.123), (3, '14:30:25');

SELECT * FROM tab64 ORDER BY event_id;
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        2 │ 14:30:25.123 │
3. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

2. `Time64` 값 필터링

```sql
SELECT * FROM tab64 WHERE time = toTime64('14:30:25', 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

```sql
SELECT * FROM tab64 WHERE time = toTime64(52225.123, 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        2 │ 14:30:25.123 │
   └──────────┴──────────────┘
```

참고: `toTime64`는 숫자 리터럴을 지정된 정밀도에 따라 소수 부분이 포함된 초 단위 값으로 해석하므로, 의도한 소수 자릿수를 명시적으로 지정하십시오.

3. 결과 유형 확인:

```sql
SELECT CAST('14:30:25.250' AS Time64(3)) AS column, toTypeName(column) AS type;
```

```text
   ┌────────column─┬─type──────┐
1. │ 14:30:25.250 │ Time64(3) │
   └───────────────┴───────────┘
```

<div id="addition-with-date">
  ## Date와의 덧셈
</div>

[Time64](time64.md) 값은 [Date](date.md) 또는 [Date32](date32.md) 값에 더할 수 있으며, 그 결과 `Time64`와 동일한 scale의 [DateTime64](datetime64.md)가 생성됩니다:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
   ┌─────────────────────────dt─┬─toTypeName(dt)─┐
1. │ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
   └────────────────────────────┴────────────────┘
```

지원되는 모든 조합과 결과 타입에 대한 자세한 내용은 [날짜 및 시간 덧셈](../operators/index.md#date-time-addition)을 참조하십시오.

**관련 항목**

* [타입 변환 함수](../../sql-reference/functions/type-conversion-functions.md)
* [날짜 및 시간을 처리하는 함수](../../sql-reference/functions/date-time-functions.md)
* [`date_time_input_format` 설정](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 설정](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` 서버 구성 매개변수](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 설정](../../operations/settings/settings.md#session_timezone)
* [날짜 및 시간을 처리하는 연산자](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [`Date` 데이터 타입](../../sql-reference/data-types/date.md)
* [`Time` 데이터 타입](../../sql-reference/data-types/time.md)
* [`DateTime` 데이터 타입](../../sql-reference/data-types/datetime.md)