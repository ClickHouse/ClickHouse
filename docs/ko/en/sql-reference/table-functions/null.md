---
description: '지정된 구조의 Null table engine을 사용하는 임시 테이블을 생성합니다. 이 함수는 테스트 작성 및 데모를 쉽게 하기 위해 사용됩니다.'
sidebar_label: 'null 함수'
sidebar_position: 140
slug: /sql-reference/table-functions/null
title: 'null'
doc_type: 'reference'
---

지정된 구조의 [Null](../../engines/table-engines/special/null.md) table engine을 사용하는 임시 테이블을 생성합니다. `Null` 엔진의 속성에 따라 테이블 데이터는 무시되며, 테이블 자체는 쿼리 실행 직후 즉시 삭제됩니다. 이 함수는 테스트 작성 및 데모를 쉽게 하기 위해 사용됩니다.

<div id="syntax">
  ## 구문
</div>

```sql
null('structure')
```

<div id="argument">
  ## 인수
</div>

* `structure` — 컬럼 목록과 각 컬럼의 타입입니다. [String](../../sql-reference/data-types/string.md).

<div id="returned_value">
  ## 반환 값
</div>

지정한 구조의 임시 `Null` 엔진 테이블입니다.

<div id="example">
  ## 예시
</div>

`null` 함수를 사용한 쿼리:

```sql
INSERT INTO function null('x UInt64') SELECT * FROM numbers_mt(1000000000);
```

3개의 쿼리를 대체할 수 있습니다:

```sql
CREATE TABLE t (x UInt64) ENGINE = Null;
INSERT INTO t SELECT * FROM numbers_mt(1000000000);
DROP TABLE IF EXISTS t;
```

<div id="related">
  ## 관련 문서
</div>

* [Null table engine](../../engines/table-engines/special/null.md)