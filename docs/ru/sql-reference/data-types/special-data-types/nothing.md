---
slug: /ru/sql-reference/data-types/special-data-types/nothing
sidebar_position: 60
sidebar_label: Nothing
---

# Nothing {#nothing}

Этот тип данных предназначен только для того, чтобы представлять [NULL](../../../sql-reference/syntax.md#null-literal), т.е. отсутствие значения.

Невозможно создать значение типа `Nothing`, поэтому он используется там, где значение не подразумевается. Например, `NULL` записывается как `Nullable(Nothing)` ([Nullable](../../../sql-reference/data-types/nullable.md) — это тип данных, позволяющий хранить `NULL` в таблицах). Также тип `Nothing` используется для обозначения пустых массивов:

``` sql
SELECT toTypeName(Array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```
