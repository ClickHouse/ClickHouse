# AggregateFunction(name, types\_of\_arguments…) {#data-type-aggregatefunction}

El estado intermedio de una función agregada. Para obtenerlo, use funciones agregadas con el `-State` sufijo. Para obtener datos agregados en el futuro, debe utilizar las mismas funciones agregadas con el `-Merge`sufijo.

`AggregateFunction` — parametric data type.

**Parámetros**

-   Nombre de la función de agregado.

        If the function is parametric specify its parameters too.

-   Tipos de los argumentos de la función agregada.

**Ejemplo**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[uniq](../../query_language/agg_functions/reference.md#agg_function-uniq), anyIf ([cualquier](../../query_language/agg_functions/reference.md#agg_function-any)+[Si](../../query_language/agg_functions/combinators.md#agg-functions-combinator-if)) y [cantiles](../../query_language/agg_functions/reference.md) son las funciones agregadas admitidas en ClickHouse.

## Uso {#usage}

### Inserción de datos {#data-insertion}

Para insertar datos, utilice `INSERT SELECT` con agregado `-State`- función.

**Ejemplos de funciones**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

En contraste con las funciones correspondientes `uniq` y `quantiles`, `-State`- funciones devuelven el estado, en lugar del valor final. En otras palabras, devuelven un valor de `AggregateFunction` tipo.

En los resultados de `SELECT` consulta, los valores de `AggregateFunction` tipo tiene representación binaria específica de la implementación para todos los formatos de salida de ClickHouse. Si volcar datos en, por ejemplo, `TabSeparated` Formato con `SELECT` Consulta entonces este volcado se puede cargar de nuevo usando `INSERT` consulta.

### Selección de datos {#data-selection}

Al seleccionar datos de `AggregatingMergeTree` mesa, uso `GROUP BY` cláusula y las mismas funciones agregadas que al insertar datos, pero usando `-Merge`sufijo.

Una función agregada con `-Merge` sufijo toma un conjunto de estados, los combina y devuelve el resultado de la agregación de datos completa.

Por ejemplo, las siguientes dos consultas devuelven el mismo resultado:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Ejemplo de uso {#usage-example}

Ver [AgregaciónMergeTree](../../operations/table_engines/aggregatingmergetree.md) Descripción del motor.

[Artículo Original](https://clickhouse.tech/docs/es/data_types/nested_data_structures/aggregatefunction/) <!--hide-->
