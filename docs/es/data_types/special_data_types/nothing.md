---
machine_translated: true
---

# Nada {#nothing}

El único propósito de este tipo de datos es representar casos en los que no se espera un valor. Entonces no puedes crear un `Nothing` valor de tipo.

Por ejemplo, literal [NULO](../../query_language/syntax.md#null-literal) tiene tipo de `Nullable(Nothing)`. Ver más sobre [NULO](../../data_types/nullable.md).

El `Nothing` tipo puede también se utiliza para denotar matrices vacías:

``` sql
SELECT toTypeName(array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/es/data_types/special_data_types/nothing/) <!--hide-->
