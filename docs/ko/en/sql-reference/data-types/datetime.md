---
description: '초 단위 정밀도의 타임스탬프를 저장하는 ClickHouse의 DateTime 데이터 타입에 대한 문서'
sidebar_label: 'DateTime'
sidebar_position: 16
slug: /sql-reference/data-types/datetime
title: 'DateTime'
doc_type: 'reference'
---

달력상의 날짜와 하루 중 시간으로 표현할 수 있는 시점을 저장할 수 있습니다.

구문:

```sql
DateTime([timezone])
```

지원되는 값의 범위: [1970-01-01 00:00:00, 2106-02-07 06:28:15].

해상도: 1초.

<div id="speed">
  ## 속도
</div>

대부분의 경우 `Date` 데이터 타입은 `DateTime`보다 더 빠릅니다.

`Date` 타입은 2바이트의 저장 공간이 필요하고, `DateTime`은 4바이트가 필요합니다. 하지만 압축 시에는 `Date`와 `DateTime`의 크기 차이가 더 크게 나타납니다. 이는 `DateTime`의 분과 초 값이 압축에 덜 유리하기 때문입니다. 또한 `DateTime` 대신 `Date`를 필터링하고 집계하는 것이 더 빠릅니다.

<div id="usage-remarks">
  ## 사용 시 참고사항
</div>

특정 시점은 시간대나 일광 절약 시간제와 관계없이 [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time)로 저장됩니다. 시간대는 `DateTime` 타입 값이 텍스트 포맷으로 어떻게 표시되는지, 그리고 문자열로 지정된 값(&#39;2020-01-01 05:00:01&#39;)이 어떻게 파싱되는지에 영향을 줍니다.

시간대와 무관한 Unix timestamp가 테이블에 저장되며, 시간대는 데이터 가져오기/내보내기 시 이를 텍스트 포맷으로 변환하거나 다시 되돌릴 때, 또는 값에 대해 달력 계산을 수행할 때(예시: `toDate`, `toHour` 함수 등) 사용됩니다. 시간대는 테이블의 행(또는 결과 집합)에 저장되지 않고 컬럼 메타데이터에 저장됩니다.

지원되는 시간대 목록은 [IANA Time Zone Database](https://www.iana.org/time-zones)에서 확인할 수 있으며, `SELECT * FROM system.time_zones`로도 조회할 수 있습니다. [이 목록](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)은 Wikipedia에서도 확인할 수 있습니다.

테이블을 생성할 때 `DateTime` 타입 컬럼의 시간대를 명시적으로 설정할 수 있습니다. 예시: `DateTime('UTC')`. 시간대가 설정되지 않은 경우, ClickHouse는 ClickHouse 서버가 시작될 때의 서버 설정에 있는 [timezone](../../operations/server-configuration-parameters/settings.md#timezone) 매개변수 값 또는 운영 체제 설정을 사용합니다.

[clickhouse-client](../../interfaces/client.md)는 데이터 타입을 초기화할 때 시간대가 명시적으로 설정되지 않으면 기본적으로 서버 시간대를 적용합니다. 클라이언트 시간대를 사용하려면 `--use_client_time_zone` 매개변수와 함께 `clickhouse-client`를 실행하십시오.

ClickHouse는 [date&#95;time&#95;output&#95;format](../../operations/settings/settings-formats.md#date_time_output_format) 설정 값에 따라 값을 출력합니다. 기본 텍스트 포맷은 `YYYY-MM-DD hh:mm:ss`입니다. 또한 [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatDateTime) 함수를 사용해 출력 형식을 변경할 수 있습니다.

ClickHouse에 데이터를 삽입할 때는 [date&#95;time&#95;input&#95;format](../../operations/settings/settings-formats.md#date_time_input_format) 설정 값에 따라 다양한 날짜 및 시간 문자열 포맷을 사용할 수 있습니다.

<div id="examples">
  ## 예시
</div>

**1.** `DateTime` 타입의 컬럼이 있는 테이블을 생성하고 데이터를 삽입하는 예시:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime('Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse DateTime
-- - from string,
-- - from integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01 00:00:00', 1), (1546300800, 2);

SELECT * FROM dt;
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
│ 2019-01-01 03:00:00 │        2 │
└─────────────────────┴──────────┘
```

* datetime 값을 정수로 삽입하면 Unix Timestamp(UTC)로 처리됩니다. `1546300800`은 UTC 기준 `'2019-01-01 00:00:00'`을 나타냅니다. 하지만 `timestamp` 컬럼에 `Asia/Istanbul`(UTC+3) 시간대가 지정되어 있으므로, 문자열로 출력하면 값이 `'2019-01-01 03:00:00'`으로 표시됩니다.
* 문자열 값을 datetime으로 삽입하면 컬럼의 시간대로 처리됩니다. `'2019-01-01 00:00:00'`은 `Asia/Istanbul` 시간대로 간주되어 `1546290000`으로 저장됩니다.

**2.** `DateTime` 값 필터링

```sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Asia/Istanbul')
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

`DateTime` 컬럼 값은 `WHERE` 프레디케이트에서 문자열 값으로 필터링할 수 있습니다. 이 값은 자동으로 `DateTime`으로 변환됩니다:

```sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** `DateTime` 타입 컬럼의 시간대 확인:

```sql
SELECT toDateTime(now(), 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Asia/Istanbul') │
└─────────────────────┴───────────────────────────┘
```

**4.** 시간대 변환

```sql
SELECT
toDateTime(timestamp, 'Europe/London') AS lon_time,
toDateTime(timestamp, 'Asia/Istanbul') AS istanbul_time
FROM dt
```

```text
┌───────────lon_time──┬───────istanbul_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

시간대 변환은 메타데이터만 변경하므로 연산 비용이 발생하지 않습니다.

<div id="limitations-on-time-zones-support">
  ## 시간대 지원의 제한 사항
</div>

일부 시간대는 완전히 지원되지 않을 수 있습니다. 대표적인 경우는 다음과 같습니다.

UTC로부터의 오프셋이 15분의 배수가 아니면 시와 분 계산이 부정확할 수 있습니다. 예를 들어 라이베리아 몬로비아의 시간대는 1972년 1월 7일 이전에 UTC -0:44:30 오프셋을 사용했습니다. Monrovia 시간대의 과거 시각에 대해 계산을 수행하면 시간 처리 함수가 잘못된 결과를 반환할 수 있습니다. 다만 1972년 1월 7일 이후의 결과는 정확합니다.

시간 전환(일광 절약 시간 또는 기타 이유로 인한 변경)이 15분의 배수가 아닌 시점에 이루어진 경우에도, 해당 날짜에는 부정확한 결과가 나올 수 있습니다.

달력 날짜가 단조롭게 증가하지 않는 경우도 있습니다. 예를 들어 Happy Valley - Goose Bay에서는 2010년 11월 7일 00:01:00에 시간이 1시간 뒤로 조정되었습니다(자정 1분 후). 따라서 11월 6일이 끝난 뒤 사람들은 11월 7일의 1분을 먼저 경험했고, 이후 시간이 다시 11월 6일 23:01로 돌아간 다음 59분이 더 지난 뒤 11월 7일이 다시 시작되었습니다. ClickHouse는 이런 종류의 특이한 상황을 (아직) 지원하지 않습니다. 이 날짜에는 시간 처리 함수의 결과가 약간 부정확할 수 있습니다.

이와 비슷한 문제는 2010년 Casey Antarctic station에서도 있었습니다. 이곳에서는 3월 5일 02:00에 시간을 3시간 뒤로 조정했습니다. 남극 기지에서 작업하더라도 ClickHouse를 사용하는 것을 걱정할 필요는 없습니다. 다만 시간대를 UTC로 설정하거나, 부정확성이 있을 수 있다는 점을 유의하십시오.

며칠에 걸쳐 시간대가 이동하는 경우도 있습니다. 일부 태평양 섬은 시간대 오프셋을 UTC+14에서 UTC-12로 변경했습니다. 이는 문제없지만, 전환이 발생한 날짜의 과거 시점에 대해 해당 시간대로 계산을 수행할 경우 일부 부정확성이 발생할 수 있습니다.

<div id="handling-daylight-saving-time-dst">
  ## 일광 절약 시간제(DST) 처리
</div>

시간대가 포함된 ClickHouse의 DateTime 타입은 일광 절약 시간제(DST) 전환 중 예상치 못한 동작을 보일 수 있으며, 특히 다음과 같은 경우에 그렇습니다.

* [`date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format)이 `simple`로 설정된 경우
* 시계가 뒤로 이동(&quot;Fall Back&quot;)하여 1시간이 중복되는 경우
* 시계가 앞으로 이동(&quot;Spring Forward&quot;)하여 1시간의 공백이 생기는 경우

기본적으로 ClickHouse는 중복되는 시간대에서는 항상 더 이른 시각을 선택하며, 시간이 앞으로 이동할 때는 존재하지 않는 시각도 해석할 수 있습니다.

예를 들어, 다음과 같이 일광 절약 시간제(DST)에서 표준시로 전환되는 경우를 살펴보겠습니다.

* 2023년 10월 29일 02:00:00에 시계가 01:00:00으로 뒤로 이동합니다(BST → GMT).
* 01:00:00 – 01:59:59 구간은 두 번 나타납니다(BST에서 한 번, GMT에서 한 번).
* ClickHouse는 항상 첫 번째 시각(BST)을 선택하므로, 시간 인터벌을 추가할 때 예상치 못한 결과가 발생합니다.

```sql
SELECT '2023-10-29 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-10-29 01:30:00 │ 2023-10-29 01:30:00 │
└─────────────────────┴─────────────────────┘
```

마찬가지로, 표준시(Standard Time)에서 일광 절약 시간(Daylight Saving Time)으로 전환되는 동안에는 한 시간이 건너뛰어진 것처럼 보일 수 있습니다.

예시는 다음과 같습니다.

* 2023년 3월 26일 `00:59:59`에 시계는 02:00:00으로 한 시간 앞당겨집니다(GMT → BST).
* `01:00:00` – `01:59:59` 시간대는 존재하지 않습니다.

```sql
SELECT '2023-03-26 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-03-26 00:30:00 │ 2023-03-26 02:30:00 │
└─────────────────────┴─────────────────────┘
```

이 경우 ClickHouse는 존재하지 않는 시간인 `2023-03-26 01:30:00`을 `2023-03-26 00:30:00`으로 앞당깁니다.

<div id="see-also">
  ## 관련 항목
</div>

* [타입 변환 함수](../../sql-reference/functions/type-conversion-functions.md)
* [날짜 및 시간 관련 함수](../../sql-reference/functions/date-time-functions.md)
* [배열 관련 함수](../../sql-reference/functions/array-functions.md)
* [`date_time_input_format` 설정](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 설정](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` 서버 구성 매개변수](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 설정](../../operations/settings/settings.md#session_timezone)
* [날짜 및 시간 관련 연산자](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [`Date` 데이터 타입](../../sql-reference/data-types/date.md)