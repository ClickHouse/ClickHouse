---
description: 'Documentação do tipo de dado especial Nothing'
sidebar_label: 'Nothing'
sidebar_position: 60
slug: /sql-reference/data-types/special-data-types/nothing
title: 'Nothing'
doc_type: 'reference'
---

A única finalidade desse tipo de dado é representar casos em que não se espera um valor. Portanto, não é possível criar um valor do tipo `Nothing`.

Por exemplo, o literal [NULL](/pt-BR/sql-reference/syntax#null) tem o tipo `Nullable(Nothing)`. Veja mais sobre [Nullable](../../../sql-reference/data-types/nullable.md).

O tipo `Nothing` também pode ser usado para representar arrays vazios:

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```