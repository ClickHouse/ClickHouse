---
description: 'Documentación del tipo de dato especial Nothing'
sidebar_label: 'Nothing'
sidebar_position: 60
slug: /sql-reference/data-types/special-data-types/nothing
title: 'Nothing'
doc_type: 'referencia'
---

La única finalidad de este tipo de dato es representar casos en los que no se espera ningún valor. Por lo tanto, no se puede crear un valor de tipo `Nothing`.

Por ejemplo, el literal [NULL](/es/sql-reference/syntax#null) es de tipo `Nullable(Nothing)`. Consulta más información sobre [Nullable](../../../sql-reference/data-types/nullable.md).

El tipo `Nothing` también puede usarse para representar arrays vacíos:

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```