---
description: '초 단위 정밀도의 시간 범위를 저장하는 ClickHouse의 Time 데이터 타입 문서'
slug: /sql-reference/data-types/time
sidebar_position: 15
sidebar_label: 'Time'
title: 'Time'
doc_type: 'reference'
---

데이터 타입 `Time`은 시, 분, 초로 이루어진 시간을 나타냅니다.
달력 날짜와 무관하며, 일, 월, 연도 정보가 필요 없는 값에 적합합니다.

구문:

```sql
Time
```

텍스트 표현 범위: [-999:59:59, 999:59:59].

해상도: 1초.

<div id="implementation-details">
  ## 구현 세부 사항
</div>

**표현 및 성능**.
데이터 유형 `Time`은 내부적으로 초를 인코딩하는 부호 있는 32비트 정수를 저장합니다.
`Time` 및 `DateTime` 유형의 값은 바이트 크기가 같으므로 성능도 비슷합니다.

**정규화**.
문자열을 `Time`으로 파싱할 때 시각 구성 요소는 정규화되며 유효성 검사는 수행되지 않습니다.
예를 들어, `25:70:70`은 `26:11:10`으로 해석됩니다.

**음수 값**.
선행 마이너스 기호가 지원되며 그대로 유지됩니다.
음수 값은 일반적으로 `Time` 값에 대한 산술 연산으로 인해 발생합니다.
`Time` 유형에서는 텍스트 입력(예: `'-01:02:03'`)과 숫자 입력(예: `-3723`) 모두에서 음수 입력이 유지됩니다.

**포화**.
시각 구성 요소는 [-999:59:59, 999:59:59] 범위로 제한됩니다.
시간이 999를 초과하는 값(또는 -999보다 작은 값)은 텍스트로 `999:59:59`(또는 `-999:59:59`)로 표현되며, 다시 읽어와도 동일하게 유지됩니다.

**시간대**.
`Time`은 시간대를 지원하지 않으며, 즉 `Time` 값은 지역적 맥락 없이 해석됩니다.
유형 매개변수로 또는 값을 생성할 때 `Time`에 시간대를 지정하면 오류가 발생합니다.
마찬가지로 `Time` 컬럼에 시간대를 적용하거나 변경하려는 시도는 지원되지 않으며 오류가 발생합니다.
`Time` 값은 서로 다른 시간대에 따라 자동으로 재해석되지 않습니다.

<div id="examples">
  ## 예시
</div>

**1.** `Time` 유형 컬럼이 있는 테이블을 생성하고 데이터를 삽입하는 예:

```sql
CREATE TABLE tab
(
    `event_id` UInt8,
    `time` Time
)
ENGINE = TinyLog;
```

```sql
-- Parse Time
-- - from string,
-- - from integer interpreted as number of seconds since 00:00:00.
INSERT INTO tab VALUES (1, '14:30:25'), (2, 52225);

SELECT * FROM tab ORDER BY event_id;
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**2.** `Time` 값 기준 필터링

```sql
SET use_legacy_to_time = 0;
SELECT * FROM tab WHERE time = toTime('14:30:25')
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

`Time` 컬럼 값은 `WHERE` 프레디케이트에서 문자열 값으로 필터링할 수 있습니다. 이 값은 자동으로 `Time`으로 변환됩니다:

```sql
SELECT * FROM tab WHERE time = '14:30:25'
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**3.** 결과 유형을 확인합니다:

```sql
SELECT CAST('14:30:25' AS Time) AS column, toTypeName(column) AS type
```

```text
   ┌────column─┬─type─┐
1. │ 14:30:25 │ Time │
   └───────────┴──────┘
```

<div id="addition-with-date">
  ## Date와의 덧셈
</div>

[Time](time.md) 값은 [Date](date.md) 또는 [Date32](date32.md) 값에 더해 [DateTime](datetime.md) 또는 [DateTime64](datetime64.md) 값을 생성할 수 있습니다:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') as datetime;
```

```text
   ┌────────────datetime─┐
1. │ 2024-07-15 14:30:25 │
   └─────────────────────┘
```

지원되는 모든 조합과 결과 타입에 대한 자세한 내용은 [날짜 및 시간 덧셈](../operators/index.md#date-time-addition)을 참조하세요.

<div id="see-also">
  ## 관련 항목
</div>

* [형 변환 함수](../functions/type-conversion-functions.md)
* [날짜와 시간 관련 함수](../functions/date-time-functions.md)
* [배열 관련 함수](../functions/array-functions.md)
* [`date_time_input_format` 설정](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 설정](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` 서버 구성 매개변수](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 설정](../../operations/settings/settings.md#session_timezone)
* [`DateTime` 데이터 타입](datetime.md)
* [`Date` 데이터 타입](date.md)