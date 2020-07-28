---
machine_translated: true
machine_translated_rev: e8cd92bba3269f47787db090899f7c242adf7818
toc_priority: 60
toc_title: "Hi\xE7bir \u015Fey"
---

# Hiçbir şey {#nothing}

Bu veri türünün tek amacı, bir değerin beklenmediği durumları temsil etmektir. Yani bir oluşturamazsınız `Nothing` type value.

Örneğin, literal [NULL](../../../sql_reference/syntax.md#null-literal) türü vardır `Nullable(Nothing)`. Daha fazla görmek [Nullable](../../../sql_reference/data_types/nullable.md).

Bu `Nothing` tür boş dizileri belirtmek için de kullanılabilir:

``` sql
SELECT toTypeName(array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/special_data_types/nothing/) <!--hide-->
