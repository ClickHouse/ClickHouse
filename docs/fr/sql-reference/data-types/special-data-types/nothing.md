---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: Rien
---

# Rien {#nothing}

Le seul but de ce type de données est de représenter les cas où une valeur n'est pas prévu. Donc vous ne pouvez pas créer un `Nothing` type de valeur.

Par exemple, littéral [NULL](../../../sql-reference/syntax.md#null-literal) a type de `Nullable(Nothing)`. Voir plus sur [Nullable](../../../sql-reference/data-types/nullable.md).

Le `Nothing` type peut également être utilisé pour désigner des tableaux vides:

``` sql
SELECT toTypeName(array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```

[Article Original](https://clickhouse.tech/docs/en/data_types/special_data_types/nothing/) <!--hide-->
