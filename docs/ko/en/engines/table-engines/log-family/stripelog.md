---
description: 'StripeLog 테이블 엔진에 대한 문서'
slug: /engines/table-engines/log-family/stripelog
toc_priority: 32
toc_title: 'StripeLog'
title: 'StripeLog 테이블 엔진'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="stripelog-table-engine">
  # StripeLog 테이블 엔진
</div>

<CloudNotSupportedBadge />

이 엔진은 log engines 계열에 속합니다. 공통 속성과 차이점은 [Log Engine Family](../../../engines/table-engines/log-family/index.md) 문서를 참조하십시오.

이 엔진은 적은 양의 데이터(100만 행 미만)를 담은 테이블을 많이 써야 하는 경우에 사용하십시오. 예를 들어, 원자적 처리가 필요한 변환 작업을 위해 들어오는 데이터 배치를 저장하는 데 이 테이블을 사용할 수 있습니다. ClickHouse 서버에서는 이 유형의 테이블 인스턴스 10만 개까지 사용할 수 있습니다. 많은 수의 테이블이 필요할 때는 [Log](./log.md)보다 이 테이블 엔진을 우선적으로 사용하는 것이 좋습니다. 다만 그만큼 읽기 효율성은 떨어집니다.

<div id="table_engines-stripelog-creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

자세한 내용은 [CREATE TABLE](/ko/sql-reference/statements/create/table) 쿼리 설명을 참조하십시오.

<div id="table_engines-stripelog-writing-the-data">
  ## 데이터 쓰기
</div>

`StripeLog` 엔진은 모든 컬럼을 하나의 파일에 저장합니다. 각 `INSERT` 쿼리마다 ClickHouse는 데이터 블록을 테이블 파일의 끝에 추가하고, 컬럼을 하나씩 기록합니다.

각 테이블마다 ClickHouse는 다음 파일을 기록합니다:

* `data.bin` — 데이터 파일.
* `index.mrk` — 마크 파일입니다. 마크에는 삽입된 각 데이터 블록의 각 컬럼에 대한 오프셋이 들어 있습니다.

`StripeLog` 엔진은 `ALTER UPDATE` 및 `ALTER DELETE` 작업을 지원하지 않습니다.

<div id="table_engines-stripelog-reading-the-data">
  ## 데이터 읽기
</div>

마크 파일을 사용하면 ClickHouse가 데이터 읽기를 병렬로 처리할 수 있습니다. 즉, `SELECT` 쿼리가 예측할 수 없는 순서로 행을 반환할 수 있습니다. 행을 정렬하려면 `ORDER BY` 절을 사용하십시오.

<div id="table_engines-stripelog-example-of-use">
  ## 사용 예시
</div>

테이블 생성:

```sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

데이터 삽입:

```sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

두 개의 `INSERT` 쿼리를 사용해 `data.bin` 파일 안에 2개의 데이터 블록을 만들었습니다.

ClickHouse는 데이터를 조회할 때 여러 스레드를 사용합니다. 각 스레드는 서로 다른 데이터 블록을 읽고, 작업이 끝나는 대로 결과 행을 독립적으로 반환합니다. 따라서 대부분의 경우 출력에 나타나는 행 블록의 순서는 입력에 있는 동일한 블록의 순서와 일치하지 않습니다. 예를 들면 다음과 같습니다:

```sql
SELECT * FROM stripe_log_table
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

결과를 정렬합니다(기본값은 오름차순):

```sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```