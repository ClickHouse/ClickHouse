---
description: '주어진 쿼리 문자열에 무작위 변형을 가합니다.'
sidebar_label: 'fuzzQuery'
sidebar_position: 75
slug: /sql-reference/table-functions/fuzzQuery
title: 'fuzzQuery'
doc_type: 'reference'
---

주어진 쿼리 문자열에 무작위 변형을 가합니다.

<div id="syntax">
  ## 구문
</div>

```sql
fuzzQuery(query[, max_query_length[, random_seed]])
```

<div id="arguments">
  ## 인수
</div>

| Argument           | Description                               |
| ------------------ | ----------------------------------------- |
| `query`            | (String) - 퍼징 대상이 되는 원본 쿼리입니다.            |
| `max_query_length` | (UInt64) - 퍼징 과정에서 쿼리가 도달할 수 있는 최대 길이입니다. |
| `random_seed`      | (UInt64) - 일관된 결과를 생성하기 위한 랜덤 시드입니다.      |

<div id="returned_value">
  ## 반환 값
</div>

교란된 쿼리 문자열이 포함된 단일 컬럼의 테이블 객체입니다.

<div id="usage-example">
  ## 사용 예시
</div>

```sql
SELECT * FROM fuzzQuery('SELECT materialize(\'a\' AS key) GROUP BY key') LIMIT 2;
```

```response
   ┌─query──────────────────────────────────────────────────────────┐
1. │ SELECT 'a' AS key GROUP BY key                                 │
2. │ EXPLAIN PIPELINE compact = true SELECT 'a' AS key GROUP BY key │
   └────────────────────────────────────────────────────────────────┘
```