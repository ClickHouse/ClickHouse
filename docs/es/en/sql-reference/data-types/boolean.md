---
description: 'Documentación del tipo de dato Boolean de ClickHouse'
sidebar_label: 'Boolean'
sidebar_position: 33
slug: /sql-reference/data-types/boolean
title: 'Bool'
doc_type: 'reference'
---

El tipo `bool` se almacena internamente como UInt8. Los valores posibles son `true` (1) y `false` (0).

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