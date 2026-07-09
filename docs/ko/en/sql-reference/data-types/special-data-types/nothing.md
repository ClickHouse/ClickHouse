---
description: 'Nothing 데이터 타입 문서'
sidebar_label: 'Nothing'
sidebar_position: 60
slug: /sql-reference/data-types/special-data-types/nothing
title: 'Nothing'
doc_type: 'reference'
---

이 데이터 타입의 유일한 목적은 값이 필요하지 않은 경우를 나타내는 것입니다. 따라서 `Nothing` 타입의 값은 생성할 수 없습니다.

예를 들어, 리터럴 [NULL](/ko/sql-reference/syntax#null)의 타입은 `Nullable(Nothing)`입니다. [Nullable](../../../sql-reference/data-types/nullable.md)에 대해 자세히 알아보십시오.

`Nothing` 타입은 빈 배열을 나타내는 데에도 사용할 수 있습니다:

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```