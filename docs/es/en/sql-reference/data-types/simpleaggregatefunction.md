---
description: 'Documentación del tipo de dato SimpleAggregateFunction'
sidebar_label: 'SimpleAggregateFunction'
sidebar_position: 48
slug: /sql-reference/data-types/simpleaggregatefunction
title: 'Tipo de dato SimpleAggregateFunction'
doc_type: 'reference'
---

<div id="description">
  ## Descripción
</div>

El tipo de dato `SimpleAggregateFunction` almacena el estado intermedio de una
función de agregación, pero no su estado completo, como sí lo hace el tipo [`AggregateFunction`](../../sql-reference/data-types/aggregatefunction.md).

Esta optimización puede aplicarse a funciones para las que se cumple la siguiente propiedad:

> el resultado de aplicar una función `f` a un conjunto de filas `S1 UNION ALL S2` puede
> obtenerse aplicando `f` por separado a partes del conjunto de filas y, a continuación,
> aplicando de nuevo `f` a los resultados: `f(S1 UNION ALL S2) = f(f(S1) UNION ALL f(S2))`.

Esta propiedad garantiza que los resultados parciales de la agregación son suficientes para calcular
el resultado combinado, por lo que no es necesario almacenar ni procesar datos adicionales. Por
ejemplo, el resultado de las funciones `min` o `max` no requiere pasos adicionales para
calcular el resultado final a partir de los pasos intermedios, mientras que la función `avg`
requiere llevar un registro de una suma y un recuento, que se dividirán para obtener el
promedio en un paso final de `Merge` que combina los estados intermedios.

Los valores de funciones de agregación suelen generarse llamando a una función de agregación
con el combinador [`-SimpleState`](/es/sql-reference/aggregate-functions/combinators#-simplestate) añadido al nombre de la función.

<div id="syntax">
  ## Sintaxis
</div>

```sql
SimpleAggregateFunction(aggregate_function_name, types_of_arguments...)
```

**Parámetros**

* `aggregate_function_name` - Nombre de una función de agregación.
* `Type` - Tipos de argumentos de la función de agregación.

<div id="supported-functions">
  ## Funciones compatibles
</div>

Se admiten las siguientes funciones de agregación:

* [`any`](/es/sql-reference/aggregate-functions/reference/any.md)
* [`any_respect_nulls`](/es/sql-reference/aggregate-functions/reference/any.md)
* [`anyLast`](/es/sql-reference/aggregate-functions/reference/anyLast.md)
* [`anyLast_respect_nulls`](/es/sql-reference/aggregate-functions/reference/anyLast.md)
* [`min`](/es/sql-reference/aggregate-functions/reference/min.md)
* [`max`](/es/sql-reference/aggregate-functions/reference/max.md)
* [`sum`](/es/sql-reference/aggregate-functions/reference/sum.md)
* [`sumWithOverflow`](/es/sql-reference/aggregate-functions/reference/sumWithOverflow.md)
* [`groupBitAnd`](/es/sql-reference/aggregate-functions/reference/groupBitAnd.md)
* [`groupBitOr`](/es/sql-reference/aggregate-functions/reference/groupBitOr.md)
* [`groupBitXor`](/es/sql-reference/aggregate-functions/reference/groupBitXor.md)
* [`groupArrayArray`](/es/sql-reference/aggregate-functions/reference/groupArrayArray.md)
* [`groupUniqArrayArray`](../../sql-reference/aggregate-functions/reference/groupUniqArray.md)
* [`groupUniqArrayArrayMap`](../../sql-reference/aggregate-functions/combinators#-map)
* [`sumMap` (`sumMappedArrays`)](/es/sql-reference/aggregate-functions/reference/sumMappedArrays.md)
* [`minMap` (`minMappedArrays`)](/es/sql-reference/aggregate-functions/reference/minMappedArrays.md)
* [`maxMap` (`maxMappedArrays`)](/es/sql-reference/aggregate-functions/reference/maxMappedArrays.md)

:::note
Los valores de `SimpleAggregateFunction(func, Type)` tienen el mismo `Type`,
por lo que, a diferencia del tipo `AggregateFunction`, no es necesario aplicar
los combinadores `-Merge`/`-State`.

El tipo `SimpleAggregateFunction` ofrece mejor rendimiento que `AggregateFunction`
para las mismas funciones de agregación.
:::

<div id="example">
  ## Ejemplo
</div>

```sql
CREATE TABLE simple (id UInt64, val SimpleAggregateFunction(sum, Double)) ENGINE=AggregatingMergeTree ORDER BY id;
```

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Uso de combinadores de agregación en ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)    - Blog: [Uso de combinadores de agregación en ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)
* Tipo [AggregateFunction](/es/sql-reference/data-types/aggregatefunction).