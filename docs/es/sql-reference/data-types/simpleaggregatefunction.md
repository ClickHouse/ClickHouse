---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# SimpleAggregateFunction {#data-type-simpleaggregatefunction}

`SimpleAggregateFunction(name, types_of_arguments…)` tipo de datos almacena el valor actual de la función agregada, y no almacena su estado completo como [`AggregateFunction`](aggregatefunction.md) hacer. Esta optimización se puede aplicar a funciones para las que se contiene la siguiente propiedad: el resultado de aplicar una función `f` a un conjunto de filas `S1 UNION ALL S2` se puede obtener aplicando `f` a partes de la fila establecida por separado, y luego aplicar de nuevo `f` los resultados: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`. Esta propiedad garantiza que los resultados de agregación parcial son suficientes para calcular el combinado, por lo que no tenemos que almacenar y procesar ningún dato adicional.

Se admiten las siguientes funciones agregadas:

-   [`any`](../../sql-reference/aggregate-functions/reference.md#agg_function-any)
-   [`anyLast`](../../sql-reference/aggregate-functions/reference.md#anylastx)
-   [`min`](../../sql-reference/aggregate-functions/reference.md#agg_function-min)
-   [`max`](../../sql-reference/aggregate-functions/reference.md#agg_function-max)
-   [`sum`](../../sql-reference/aggregate-functions/reference.md#agg_function-sum)
-   [`groupBitAnd`](../../sql-reference/aggregate-functions/reference.md#groupbitand)
-   [`groupBitOr`](../../sql-reference/aggregate-functions/reference.md#groupbitor)
-   [`groupBitXor`](../../sql-reference/aggregate-functions/reference.md#groupbitxor)

Valores de la `SimpleAggregateFunction(func, Type)` y almacenado de la misma manera que `Type`, por lo que no necesita aplicar funciones con `-Merge`/`-State` sufijos. `SimpleAggregateFunction` tiene un mejor rendimiento que `AggregateFunction` con la misma función de agregación.

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
