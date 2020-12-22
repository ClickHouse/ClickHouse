# SimpleAggregateFunction {#data-type-simpleaggregatefunction}

El tipo de dato `SimpleAggregateFunction(name, types_of_arguments…)` almacena el valor actual de la función agregada, no almacena su estado completo como hace [`AggregateFunction`](aggregatefunction.md). Esta optimización se puede aplicar a las funciones con la siguiente propiedad: el resultado de aplicar una función `f` a un conjunto de filas `S1 UNION ALL S2` se puede obtener aplicando `f` a partes de la fila establecida por separado, y luego aplicar de nuevo `f` los resultados: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. Un ejemplo de función con esta propiedad es la suma. Esta propiedad garantiza que los resultados de agregación parcial son suficientes para calcular el combinado, por lo que no tenemos que almacenar y procesar ningún dato adicional.

La lista de functiones de agregación soportadas son:

-   [`any`](../../sql-reference/aggregate-functions/reference.md#agg_function-any)
-   [`anyLast`](../../sql-reference/aggregate-functions/reference.md#anylastx)
-   [`min`](../../sql-reference/aggregate-functions/reference.md#agg_function-min)
-   [`max`](../../sql-reference/aggregate-functions/reference.md#agg_function-max)
-   [`sum`](../../sql-reference/aggregate-functions/reference.md#agg_function-sum)
-   [`groupBitAnd`](../../sql-reference/aggregate-functions/reference.md#groupbitand)
-   [`groupBitOr`](../../sql-reference/aggregate-functions/reference.md#groupbitor)
-   [`groupBitXor`](../../sql-reference/aggregate-functions/reference.md#groupbitxor)

Cuando se usa `SimpleAggregateFunction(func, Type)` el resultado es almacenado datos `Type` así que no necesita aplicar los suficos `-Merge`/`-State` a las funciones de agregación para usarlas. Para la misma función de agregación `SimpleAggregateFunction` tiene un mejor rendimiento que `AggregateFunction`.

**Parámetros**

-   Nombre de la función de agregado.
-   Tipos de los argumentos de la función agregada.

**Ejemplo**

``` sql
CREATE TABLE t
(
    column1 SimpleAggregateFunction(sum, UInt64),
    column2 SimpleAggregateFunction(any, String)
) ENGINE = ...
```

[Artículo Original](https://clickhouse.tech/docs/en/data_types/simpleaggregatefunction/) <!--hide-->
