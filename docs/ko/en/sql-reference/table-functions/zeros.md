---
description: '테스트 목적으로 많은 행을 생성하는 가장 빠른 방법으로 사용됩니다.
  `system.zeros` 및 `system.zeros_mt` 시스템 테이블과 유사합니다.'
sidebar_label: 'zeros'
sidebar_position: 145
slug: /sql-reference/table-functions/zeros
title: 'zeros'
doc_type: 'reference'
---

* `zeros(N)` – 정수 0이 `N`번 포함된 단일 &#39;zero&#39; 컬럼(UInt8)을 가진 테이블을 반환합니다
* `zeros_mt(N)` – `zeros`와 동일하지만 여러 스레드를 사용합니다.

이 함수는 테스트 목적으로 많은 행을 생성하는 가장 빠른 방법으로 사용됩니다. `system.zeros` 및 `system.zeros_mt` 시스템 테이블과 유사합니다.

다음 쿼리는 서로 동일합니다:

```sql
SELECT * FROM zeros(10);
SELECT * FROM system.zeros LIMIT 10;
SELECT * FROM zeros_mt(10);
SELECT * FROM system.zeros_mt LIMIT 10;
```

```response
┌─zero─┐
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
└──────┘
```