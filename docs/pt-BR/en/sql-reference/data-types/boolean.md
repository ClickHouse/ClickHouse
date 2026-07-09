---
description: 'Documentação do tipo de dado booleano no ClickHouse'
sidebar_label: 'Booleano'
sidebar_position: 33
slug: /sql-reference/data-types/boolean
title: 'Bool'
doc_type: 'reference'
---

O tipo `bool` é armazenado internamente como UInt8. Os valores possíveis são `true` (1) e `false` (0).

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