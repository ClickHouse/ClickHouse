# Funciones agregadas {#aggregate-functions}

Las funciones agregadas funcionan en el [normal](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) forma esperada por los expertos en bases de datos.

ClickHouse también es compatible:

-   [Funciones agregadas paramétricas](parametric_functions.md#aggregate_functions_parametric) que aceptan otros parámetros además de las columnas.
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

Supongamos que necesita sumar los valores en el `y` columna:

``` sql
SELECT sum(y) FROM t_null_big
```

Método de codificación de datos:
│ 7 │
¿Qué puedes encontrar en Neodigit

El `sum` función interpreta `NULL` como `0`. En particular, esto significa que si la función recibe la entrada de una selección donde todos los valores son `NULL`, entonces el resultado será `0`Nuestra `NULL`.

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

[Artículo Original](https://clickhouse.tech/docs/es/query_language/agg_functions/) <!--hide-->
