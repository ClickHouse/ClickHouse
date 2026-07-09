---
description: '`Merge` 엔진(`MergeTree`와 혼동하지 마십시오)은 자체적으로 데이터를 저장하지 않지만, 임의 개수의 다른 테이블을 동시에 읽을 수 있도록 합니다.'
sidebar_label: 'Merge'
sidebar_position: 30
slug: /engines/table-engines/special/merge
title: 'Merge 테이블 엔진'
doc_type: '참고'
---

`Merge` 엔진(`MergeTree`와 혼동하지 마십시오)은 자체적으로 데이터를 저장하지 않지만, 임의 개수의 다른 테이블을 동시에 읽을 수 있도록 합니다.

읽기는 자동으로 병렬 처리됩니다. 테이블에 대한 쓰기는 지원되지 않습니다. 읽기 시에는 실제로 읽는 테이블에 인덱스가 있으면 해당 인덱스를 사용합니다.

<div id="creating-a-table">
  ## 테이블 생성
</div>

```sql
CREATE TABLE ... Engine=Merge(db_name, tables_regexp)
```

<div id="engine-parameters">
  ## 엔진 매개변수
</div>

<div id="db_name">
  ### `db_name`
</div>

`db_name` — 가능한 값:

* 데이터베이스 이름,
  * 예를 들어 `currentDatabase()``와` 같이 데이터베이스 이름 문자열을 반환하는 상수 표현식,
  * `REGEXP(expression)`, 여기서 `expression`은 DB 이름과 일치하는 정규식입니다.

<div id="tables_regexp">
  ### `tables_regexp`
</div>

`tables_regexp` — 지정된 DB 또는 여러 DB에서 테이블 이름과 일치하는 정규식입니다.

정규식 — [re2](https://github.com/google/re2) (PCRE의 부분 집합 지원), 대소문자를 구분합니다.
정규식에서 기호를 이스케이프하는 방법에 대한 참고 사항은 &quot;match&quot; 섹션을 참조하십시오.

<div id="usage">
  ## 사용법
</div>

읽을 테이블을 선택할 때는 정규식과 일치하더라도 `Merge` 테이블 자체는 선택되지 않습니다. 루프를 방지하기 위해서입니다.
서로의 데이터를 읽으려고 무한히 시도하는 2개의 `Merge` 테이블을 만들 수도 있지만, 좋은 방법은 아닙니다.

`Merge` 엔진은 일반적으로 많은 수의 `TinyLog` 테이블을 하나의 테이블처럼 다룰 때 사용합니다.

<div id="examples">
  ## 예시
</div>

**예시 1**

두 개의 데이터베이스(database) `ABC_corporate_site`와 `ABC_store`를 가정합니다. `all_visitors` 테이블(table)에는 두 데이터베이스의 `visitors` 테이블에 있는 ID가 모두 저장됩니다.

```sql
CREATE TABLE all_visitors (id UInt32) ENGINE=Merge(REGEXP('ABC_*'), 'visitors');
```

**예시 2**

기존 테이블 `WatchLog_old`이 있고, 데이터를 새 테이블 `WatchLog_new`로 옮기지 않은 채 파티셔닝을 변경했으며, 두 테이블의 데이터를 모두 확인해야 하는 경우를 가정해 보겠습니다.

```sql
CREATE TABLE WatchLog_old(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
ORDER BY (date, UserId, EventType);

INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(
    date Date,
    UserId Int64,
    EventType String,
    Cnt UInt64
)
ENGINE=MergeTree
PARTITION BY date
ORDER BY (UserId, EventType)
SETTINGS index_granularity=8192;

INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog AS WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT * FROM WatchLog;
```

```text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

<div id="virtual-columns">
  ## 가상 컬럼
</div>

* `_table` — 데이터를 읽어 온 테이블의 이름입니다. 유형: [String](../../../sql-reference/data-types/string.md).

  `_table`로 필터링하면(예: `WHERE _table='xyz'`) 필터 조건을 만족하는 테이블만 읽습니다.

* `_database` — 데이터를 읽어 온 데이터베이스의 이름을 포함합니다. 유형: [String](../../../sql-reference/data-types/string.md).

**관련 항목**

* [가상 컬럼](../../../engines/table-engines/index.md#table_engines-virtual_columns)
* [merge](../../../sql-reference/table-functions/merge.md) 테이블 함수