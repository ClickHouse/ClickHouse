---
machine_translated: true
---

# Rien {#nothing}

Le seul but de ce type de données est de représenter les cas où une valeur n'est pas prévu. Donc vous ne pouvez pas créer un `Nothing` type de valeur.

Par exemple, littéral [NULL](../../query_language/syntax.md#null-literal) a type de `Nullable(Nothing)`. Voir plus sur [Nullable](../../data_types/nullable.md).

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
