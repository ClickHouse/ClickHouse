---
description: 'PARALLEL WITH 절 문서'
sidebar_label: 'PARALLEL WITH'
sidebar_position: 53
slug: /sql-reference/statements/parallel_with
title: 'PARALLEL WITH 절'
doc_type: 'reference'
---

여러 SQL 문을 병렬로 실행할 수 있습니다.

<div id="syntax">
  ## 구문
</div>

```sql
statement1 PARALLEL WITH statement2 [PARALLEL WITH statement3 ...]
```

SQL 문 `statement1`, `statement2`, `statement3`, ...를 서로 병렬로 실행합니다. 해당 SQL 문의 결과는 폐기됩니다.

많은 경우 동일한 SQL 문의 시퀀스를 순차적으로 실행하는 것보다 병렬로 실행하는 편이 더 빠를 수 있습니다. 예를 들어, `statement1 PARALLEL WITH statement2 PARALLEL WITH statement3`는 `statement1; statement2; statement3`보다 더 빠를 가능성이 높습니다.

<div id="examples">
  ## 예시
</div>

두 개의 테이블(table)을 병렬로 생성합니다:

```sql
CREATE TABLE table1(x Int32) ENGINE = MergeTree ORDER BY tuple()
PARALLEL WITH
CREATE TABLE table2(y String) ENGINE = MergeTree ORDER BY tuple();
```

테이블 2개를 병렬로 삭제합니다:

```sql
DROP TABLE table1
PARALLEL WITH
DROP TABLE table2;
```

<div id="settings">
  ## 설정
</div>

[max&#95;threads](../../operations/settings/settings.md#max_threads) 설정은 생성할 스레드 수를 제어합니다.

<div id="comparison-with-union">
  ## UNION과의 비교
</div>

`PARALLEL WITH` 절은 피연산자를 병렬로 실행한다는 점에서 [UNION](select/union.md)과 다소 비슷합니다. 하지만 몇 가지 차이점이 있습니다.

* `PARALLEL WITH`는 피연산자 실행 결과를 반환하지 않으며, 예외가 발생한 경우 해당 예외를 다시 발생시킬 수만 있습니다.
* `PARALLEL WITH`는 피연산자가 동일한 결과 컬럼 집합을 가질 필요가 없습니다.
* `PARALLEL WITH`는 `SELECT`뿐 아니라 모든 SQL 문을 실행할 수 있습니다.