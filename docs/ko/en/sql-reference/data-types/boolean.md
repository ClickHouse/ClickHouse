---
description: 'ClickHouse의 Boolean 데이터 타입 참고 문서'
sidebar_label: 'Boolean'
sidebar_position: 33
slug: /sql-reference/data-types/boolean
title: 'Bool'
doc_type: 'reference'
---

`bool` 유형은 내부적으로 UInt8로 저장됩니다. 가능한 값은 `true` (1) 및 `false` (0)입니다.

```sql
SELECT true AS col, toTypeName(col);
┌─col──┬─toTypeName(true)─┐
│ true │ Bool             │
└──────┴──────────────────┘

select true == 1 as col, toTypeName(col);
┌─col─┬─toTypeName(equals(true, 1))─┐
│   1 │ UInt8                       │
└─────┴─────────────────────────────┘
```

```sql
CREATE TABLE test_bool
(
    `A` Int64,
    `B` Bool
)
ENGINE = Memory;

INSERT INTO test_bool VALUES (1, true),(2,0);

SELECT * FROM test_bool;
┌─A─┬─B─────┐
│ 1 │ true  │
│ 2 │ false │
└───┴───────┘
```