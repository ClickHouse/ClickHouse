---
description: 'ClickHouse의 Date 데이터 타입에 대한 문서'
sidebar_label: 'Date'
sidebar_position: 12
slug: /sql-reference/data-types/date
title: 'Date'
doc_type: 'reference'
---

날짜입니다. 1970-01-01 이후 경과한 일 수를 2바이트(부호 없음)로 저장합니다. Unix epoch가 시작된 직후부터 컴파일 단계에서 상수로 정의된 상한 임계값까지의 값을 저장할 수 있습니다(현재는 2149년까지이지만, 최종적으로 완전히 지원되는 연도는 2148년입니다).

지원되는 값 범위: [1970-01-01, 2149-06-06].

날짜 값은 시간대 없이 저장됩니다.

**예시**

`Date` 타입의 컬럼이 있는 테이블을 생성하고 데이터를 삽입합니다:

```sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01', 1), (17897, 2), (1546300800, 3);

SELECT * FROM dt;
```

```text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
│ 2019-01-01 │        3 │
└────────────┴──────────┘
```

**관련 항목**

* [날짜 및 시간 함수](../../sql-reference/functions/date-time-functions.md)
* [날짜 및 시간 연산자](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [`DateTime` 데이터 타입](../../sql-reference/data-types/datetime.md)