---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: Nada
---

# Nada {#nothing}

El único propósito de este tipo de datos es representar casos en los que no se espera un valor. Entonces no puedes crear un `Nothing` valor de tipo.

Por ejemplo, literal [NULL](../../../sql-reference/syntax.md#null-literal) tiene tipo de `Nullable(Nothing)`. Ver más sobre [NULL](../../../sql-reference/data-types/nullable.md).

El `Nothing` tipo puede también se utiliza para denotar matrices vacías:

``` sql
SELECT toTypeName(array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/data_types/special_data_types/nothing/) <!--hide-->
