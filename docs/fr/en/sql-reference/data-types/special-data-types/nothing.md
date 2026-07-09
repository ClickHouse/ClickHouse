---
description: 'Documentation du type de données spécial Nothing'
sidebar_label: 'Nothing'
sidebar_position: 60
slug: /sql-reference/data-types/special-data-types/nothing
title: 'Nothing'
doc_type: 'reference'
---

Le seul rôle de ce type de données est de représenter les cas où aucune valeur n&#39;est attendue. Vous ne pouvez donc pas créer de valeur de type `Nothing`.

Par exemple, le littéral [NULL](/fr/sql-reference/syntax#null) est de type `Nullable(Nothing)`. Pour en savoir plus sur [Nullable](../../../sql-reference/data-types/nullable.md).

Le type `Nothing` peut également être utilisé pour désigner des tableaux vides :

```sql
SELECT toTypeName(array())
```

```text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```