---
description: 'Log에 대한 문서'
slug: /engines/table-engines/log-family/log
toc_priority: 33
toc_title: 'Log'
title: 'Log 테이블 엔진'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="log-table-engine">
  # Log 테이블 엔진
</div>

<CloudNotSupportedBadge />

이 엔진은 `Log` 엔진 계열에 속합니다. `Log` 엔진의 공통 속성과 차이점은 [Log Engine Family](../../../engines/table-engines/log-family/index.md) 문서를 참조하십시오.

`Log`는 컬럼 파일과 함께 작은 &quot;마크&quot; 파일이 있다는 점에서 [TinyLog](../../../engines/table-engines/log-family/tinylog.md)와 다릅니다. 이 마크는 각 데이터 블록에 기록되며, 지정된 수의 행을 건너뛰기 위해 파일 읽기를 시작할 위치를 나타내는 오프셋을 포함합니다. 따라서 여러 스레드에서 테이블 데이터를 읽을 수 있습니다.
동시에 데이터에 접근할 때는 읽기 작업을 병렬로 수행할 수 있지만, 쓰기 작업은 읽기 작업과 다른 쓰기 작업을 차단합니다.
`Log` 엔진은 인덱스를 지원하지 않습니다. 또한 테이블 쓰기에 실패하면 테이블이 손상되며, 이 테이블을 읽으려고 하면 오류가 반환됩니다. `Log` 엔진은 임시 데이터, 한 번만 쓰는 테이블, 테스트 또는 데모 용도에 적합합니다.

<div id="table_engines-log-creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = Log
```

[CREATE TABLE](/ko/sql-reference/statements/create/table) 쿼리에 대한 자세한 설명은 해당 문서를 참조하십시오.

<div id="table_engines-log-writing-the-data">
  ## 데이터 쓰기
</div>

`Log` 엔진은 각 컬럼을 개별 파일에 기록해 데이터를 효율적으로 저장합니다. 각 테이블에 대해 `Log` 엔진은 지정된 저장소 경로에 다음 파일을 기록합니다:

* `<column>.bin`: 각 컬럼의 데이터 파일로, 직렬화되고 압축된 데이터를 포함합니다.
  `__marks.mrk`: 삽입된 각 데이터 블록의 오프셋과 행 수를 저장하는 마크 파일입니다. 마크는 읽기 중 관련 없는 데이터 블록을 엔진이 건너뛸 수 있게 해, 쿼리를 효율적으로 실행할 수 있도록 합니다.

<div id="writing-process">
  ### 쓰기 과정
</div>

데이터가 `Log` 테이블에 기록되면:

1. 데이터가 블록 단위로 직렬화 및 압축됩니다.
2. 각 컬럼에 대해 압축된 데이터가 해당 `<column>.bin` 파일에 추가됩니다.
3. 새로 삽입된 데이터의 오프셋과 행 수를 기록하기 위해 해당 엔트리가 `__marks.mrk` 파일에 추가됩니다.

<div id="table_engines-log-reading-the-data">
  ## 데이터 읽기
</div>

마크 파일을 사용하면 ClickHouse가 데이터 읽기를 병렬화할 수 있습니다. 즉, `SELECT` 쿼리가 예측할 수 없는 순서로 행을 반환할 수 있습니다. 행을 정렬하려면 `ORDER BY` 절을 사용하십시오.

<div id="table_engines-log-example-of-use">
  ## 사용 예시
</div>

테이블 생성:

```sql
CREATE TABLE log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = Log
```

데이터 삽입:

```sql
INSERT INTO log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

두 개의 `INSERT` 쿼리를 사용해 `<column>.bin` 파일 내부에 2개의 데이터 블록을 생성했습니다.

ClickHouse는 데이터를 조회할 때 여러 스레드를 사용합니다. 각 스레드는 서로 다른 데이터 블록을 읽고, 처리를 마치는 대로 결과 행을 독립적으로 반환합니다. 따라서 출력에서 행 블록의 순서는 입력에서 해당 블록이 있던 순서와 일치하지 않을 수 있습니다. 예를 들면 다음과 같습니다.

```sql
SELECT * FROM log_table
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

결과를 정렬합니다 (기본값은 오름차순):

```sql
SELECT * FROM log_table ORDER BY timestamp
```

```text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```