---
description: 'Документация по специальному типу данных Nothing'
sidebar_label: 'Nothing'
sidebar_position: 60
slug: /sql-reference/data-types/special-data-types/nothing
title: 'Nothing'
doc_type: 'reference'
---

Единственное назначение этого типа данных — обозначать случаи, когда значение не ожидается. Поэтому создать значение типа `Nothing` нельзя.

Например, литерал [NULL](/ru/sql-reference/syntax#null) имеет тип `Nullable(Nothing)`. Подробнее см. в разделе [Nullable](../../../sql-reference/data-types/nullable.md).

Тип `Nothing` также может использоваться для обозначения пустых массивов:

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```