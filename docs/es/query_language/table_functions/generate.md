# generateRandom {#generaterandom}

Genera datos aleatorios con un esquema dado.
Permite rellenar tablas de prueba con datos.
Admite todos los tipos de datos que se pueden almacenar en la tabla, excepto `LowCardinality` y `AggregateFunction`.

``` sql
generateRandom('name TypeName[, name TypeName]...', [, 'random_seed'[, 'max_string_length'[, 'max_array_length']]]);
```

**Parámetros**

-   `name` — Nombre de la columna correspondiente.
-   `TypeName` — Tipo de columna correspondiente.
-   `limit` — Número de filas a generar.
-   `max_array_length` — Longitud máxima de matriz para todas las matrices generadas. Por defecto `10`.
-   `max_string_length` — Longitud máxima de cadena para todas las cadenas generadas. Por defecto `10`.
-   `random_seed` — Especifique manualmente la semilla aleatoria para producir resultados estables. Si NULL — semilla se genera aleatoriamente.

**Valor devuelto**

Un objeto de tabla con el esquema solicitado.

## Ejemplo de uso {#usage-example}

``` sql
SELECT * FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2);
```

``` text
┌─a────────┬────────────d─┬─c──────────────────────────────────────────────────────────────────┐
│ [77]     │ -124167.6723 │ ('2061-04-17 21:59:44.573','3f72f405-ec3e-13c8-44ca-66ef335f7835') │
│ [32,110] │ -141397.7312 │ ('1979-02-09 03:43:48.526','982486d1-5a5d-a308-e525-7bd8b80ffa73') │
│ [68]     │  -67417.0770 │ ('2080-03-12 14:17:31.269','110425e5-413f-10a6-05ba-fa6b3e929f15') │
└──────────┴──────────────┴────────────────────────────────────────────────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/es/query_language/table_functions/generate/) <!--hide-->
