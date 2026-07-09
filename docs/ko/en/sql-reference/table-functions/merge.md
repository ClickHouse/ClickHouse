---
description: '임시 Merge 테이블을 생성합니다. 테이블 스키마는 기반이 되는 테이블들의 컬럼 합집합을 사용하고 공통 타입을 추론하여 도출됩니다.'
sidebar_label: 'merge'
sidebar_position: 130
slug: /sql-reference/table-functions/merge
title: 'merge'
doc_type: 'reference'
---

임시 [Merge](../../engines/table-engines/special/merge.md) 테이블을 생성합니다.
테이블 스키마는 기반이 되는 테이블들의 컬럼 합집합을 사용하고 공통 타입을 추론하여 도출됩니다.
[Merge](../../engines/table-engines/special/merge.md) 테이블 엔진과 동일한 가상 컬럼을 사용할 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
merge(['db_name',] 'tables_regexp')
```

<div id="arguments">
  ## 인수
</div>

| 인수              | 설명                                                                                                                                                                                                  |
| --------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `db_name`       | 가능한 값(선택 사항, 기본값은 `currentDatabase()`):<br />    - 데이터베이스 이름,<br />    - 데이터베이스 이름 문자열을 반환하는 상수 표현식(예: `currentDatabase()`),<br />    - `REGEXP(expression)`, 여기서 `expression`은 DB 이름과 일치하는 정규식입니다. |
| `tables_regexp` | 지정된 DB 또는 여러 DB의 테이블 이름과 일치하는 정규식입니다.                                                                                                                                                               |

<div id="related">
  ## 관련 항목
</div>

* [Merge](../../engines/table-engines/special/merge.md) 테이블 엔진