---
description: 'Date보다 더 넓은 범위의 날짜를 저장하는 ClickHouse의 Date32 데이터 타입 문서'
sidebar_label: 'Date32'
sidebar_position: 14
slug: /sql-reference/data-types/date32
title: 'Date32'
doc_type: 'reference'
---

날짜를 나타냅니다. [DateTime64](../../sql-reference/data-types/datetime64.md)와 동일한 날짜 범위를 지원합니다. 값은 `1900-01-01`부터 경과한 일 수를 나타내며, 네이티브 바이트 순서의 부호 있는 32비트 정수로 저장됩니다. **중요!** 0은 `1970-01-01`을 나타내며, 음수 값은 `1970-01-01` 이전 날짜를 나타냅니다.

**예시**

`Date32` 타입 컬럼이 있는 테이블을 생성하고 여기에 데이터를 삽입합니다:

```sql
CREATE TABLE dt32
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt32 VALUES ('2100-01-01', 1), (47482, 2), (4102444800, 3);

SELECT * FROM dt32;
```

```text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
│ 2100-01-01 │        3 │
└────────────┴──────────┘
```

**관련 항목**

* [toDate32](../../sql-reference/functions/type-conversion-functions.md#toDate32)
* [toDate32OrZero](/ko/sql-reference/functions/type-conversion-functions#toDate32OrZero)
* [toDate32OrNull](/ko/sql-reference/functions/type-conversion-functions#toDate32OrNull)