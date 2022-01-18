---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "Agregar funci\xF3n (nombre, types_of_arguments)...)"
---

# AggregateFunction(name, types_of_arguments…) {#data-type-aggregatefunction}

Aggregate functions can have an implementation-defined intermediate state that can be serialized to an AggregateFunction(…) data type and stored in a table, usually, by means of [una vista materializada](../../sql-reference/statements/create.md#create-view). La forma común de producir un estado de función agregada es llamando a la función agregada con el `-State` sufijo. Para obtener el resultado final de la agregación en el futuro, debe utilizar la misma función de agregado con el `-Merge`sufijo.

`AggregateFunction` — parametric data type.

**Parámetros**

-   Nombre de la función de agregado.

        If the function is parametric, specify its parameters too.

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

[uniq](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq), anyIf ([cualquier](../../sql-reference/aggregate-functions/reference.md#agg_function-any)+[Si](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-if)) y [cantiles](../../sql-reference/aggregate-functions/reference.md) son las funciones agregadas admitidas en ClickHouse.

## Uso {#usage}

### Inserción de datos {#data-insertion}

Para insertar datos, utilice `INSERT SELECT` con agregado `-State`- función.

**Ejemplos de funciones**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

En contraste con las funciones correspondientes `uniq` y `quantiles`, `-State`- funciones devuelven el estado, en lugar del valor final. En otras palabras, devuelven un valor de `AggregateFunction` tipo.

En los resultados de `SELECT` consulta, los valores de `AggregateFunction` tipo tiene representación binaria específica de la implementación para todos los formatos de salida de ClickHouse. Si volcar datos en, por ejemplo, `TabSeparated` formato con `SELECT` consulta, entonces este volcado se puede cargar de nuevo usando `INSERT` consulta.

### Selección de datos {#data-selection}

Al seleccionar datos de `AggregatingMergeTree` mesa, uso `GROUP BY` cláusula y las mismas funciones agregadas que al insertar datos, pero usando `-Merge`sufijo.

Una función agregada con `-Merge` sufijo toma un conjunto de estados, los combina y devuelve el resultado de la agregación de datos completa.

Por ejemplo, las siguientes dos consultas devuelven el mismo resultado:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Ejemplo de uso {#usage-example}

Ver [AgregaciónMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) Descripción del motor.

[Artículo Original](https://clickhouse.tech/docs/en/data_types/nested_data_structures/aggregatefunction/) <!--hide-->
