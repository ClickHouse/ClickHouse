---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: Funciones agregadas
toc_priority: 33
toc_title: "Implantaci\xF3n"
---

# Funciones agregadas {#aggregate-functions}

Las funciones agregadas funcionan en el [normal](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) forma esperada por los expertos en bases de datos.

ClickHouse también es compatible:

-   [Funciones agregadas paramétricas](parametric-functions.md#aggregate_functions_parametric) que aceptan otros parámetros además de las columnas.
-   [Combinadores](combinators.md#aggregate_functions_combinators), que cambian el comportamiento de las funciones agregadas.

## Procesamiento NULL {#null-processing}

Durante la agregación, todos `NULL`s se omiten.

**Ejemplos:**

Considere esta tabla:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Digamos que necesita sumar los valores en el `y` columna:

``` sql
SELECT sum(y) FROM t_null_big
```

    ┌─sum(y)─┐
    │      7 │
    └────────┘

El `sum` función interpreta `NULL` como `0`. En particular, esto significa que si la función recibe la entrada de una selección donde todos los valores son `NULL`, entonces el resultado será `0`, ni `NULL`.

Ahora puedes usar el `groupArray` función para crear una matriz a partir de la `y` columna:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` no incluye `NULL` en la matriz resultante.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
