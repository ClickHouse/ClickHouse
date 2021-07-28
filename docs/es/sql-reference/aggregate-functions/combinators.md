---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: Combinadores
---

# Combinadores de funciones agregadas {#aggregate_functions_combinators}

El nombre de una función agregada puede tener un sufijo anexado. Esto cambia la forma en que funciona la función de agregado.

## -Si {#agg-functions-combinator-if}

The suffix -If can be appended to the name of any aggregate function. In this case, the aggregate function accepts an extra argument – a condition (Uint8 type). The aggregate function processes only the rows that trigger the condition. If the condition was not triggered even once, it returns a default value (usually zeros or empty strings).

Ejemplos: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` y así sucesivamente.

Con las funciones de agregado condicional, puede calcular agregados para varias condiciones a la vez, sin utilizar subconsultas y `JOIN`Por ejemplo, en Yandex.Metrica, las funciones de agregado condicional se utilizan para implementar la funcionalidad de comparación de segmentos.

## -Matriz {#agg-functions-combinator-array}

El sufijo -Array se puede agregar a cualquier función agregada. En este caso, la función de agregado toma argumentos del ‘Array(T)’ tipo (arrays) en lugar de ‘T’ argumentos de tipo. Si la función de agregado acepta varios argumentos, deben ser matrices de igual longitud. Al procesar matrices, la función de agregado funciona como la función de agregado original en todos los elementos de la matriz.

Ejemplo 1: `sumArray(arr)` - Totales de todos los elementos de todos ‘arr’ matriz. En este ejemplo, podría haber sido escrito más simplemente: `sum(arraySum(arr))`.

Ejemplo 2: `uniqArray(arr)` – Counts the number of unique elements in all ‘arr’ matriz. Esto podría hacerse de una manera más fácil: `uniq(arrayJoin(arr))`, pero no siempre es posible agregar ‘arrayJoin’ a una consulta.

-If y -Array se pueden combinar. Obstante, ‘Array’ debe venir primero, entonces ‘If’. Ejemplos: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. Debido a este pedido, el ‘cond’ argumento no será una matriz.

## -Estado {#agg-functions-combinator-state}

Si aplica este combinador, la función de agregado no devuelve el valor resultante (como el número de valores únicos para el [uniq](reference.md#agg_function-uniq) función), pero un estado intermedio de la agregación (para `uniq`, esta es la tabla hash para calcular el número de valores únicos). Este es un `AggregateFunction(...)` que puede ser utilizado para su posterior procesamiento o almacenado en una tabla para terminar de agregar más tarde.

Para trabajar con estos estados, use:

-   [AgregaciónMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) motor de mesa.
-   [finalizeAggregation](../../sql-reference/functions/other-functions.md#function-finalizeaggregation) función.
-   [runningAccumulate](../../sql-reference/functions/other-functions.md#function-runningaccumulate) función.
-   [-Fusionar](#aggregate_functions_combinators-merge) combinador.
-   [-MergeState](#aggregate_functions_combinators-mergestate) combinador.

## -Fusionar {#aggregate_functions_combinators-merge}

Si aplica este combinador, la función de agregado toma el estado de agregación intermedio como argumento, combina los estados para finalizar la agregación y devuelve el valor resultante.

## -MergeState {#aggregate_functions_combinators-mergestate}

Combina los estados de agregación intermedios de la misma manera que el combinador -Merge. Sin embargo, no devuelve el valor resultante, sino un estado de agregación intermedio, similar al combinador -State.

## -ForEach {#agg-functions-combinator-foreach}

Convierte una función de agregado para tablas en una función de agregado para matrices que agrega los elementos de matriz correspondientes y devuelve una matriz de resultados. Por ejemplo, `sumForEach` para las matrices `[1, 2]`, `[3, 4, 5]`y`[6, 7]`devuelve el resultado `[10, 13, 5]` después de agregar los elementos de la matriz correspondientes.

## -OPor defecto {#agg-functions-combinator-ordefault}

Cambia el comportamiento de una función agregada.

Si una función agregada no tiene valores de entrada, con este combinador devuelve el valor predeterminado para su tipo de datos de retorno. Se aplica a las funciones agregadas que pueden tomar datos de entrada vacíos.

`-OrDefault` se puede utilizar con otros combinadores.

**Sintaxis**

``` sql
<aggFunction>OrDefault(x)
```

**Parámetros**

-   `x` — Aggregate function parameters.

**Valores devueltos**

Devuelve el valor predeterminado del tipo devuelto de una función de agregado si no hay nada que agregar.

El tipo depende de la función de agregado utilizada.

**Ejemplo**

Consulta:

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

Resultado:

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

También `-OrDefault` se puede utilizar con otros combinadores. Es útil cuando la función de agregado no acepta la entrada vacía.

Consulta:

``` sql
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

Resultado:

``` text
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

## -OrNull {#agg-functions-combinator-ornull}

Cambia el comportamiento de una función agregada.

Este combinador convierte un resultado de una función agregada en [NULL](../data-types/nullable.md) tipo de datos. Si la función de agregado no tiene valores para calcular devuelve [NULL](../syntax.md#null-literal).

`-OrNull` se puede utilizar con otros combinadores.

**Sintaxis**

``` sql
<aggFunction>OrNull(x)
```

**Parámetros**

-   `x` — Aggregate function parameters.

**Valores devueltos**

-   El resultado de la función de agregado, convertida a la `Nullable` tipo de datos.
-   `NULL`, si no hay nada que agregar.

Tipo: `Nullable(aggregate function return type)`.

**Ejemplo**

Añadir `-orNull` hasta el final de la función agregada.

Consulta:

``` sql
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

Resultado:

``` text
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

También `-OrNull` se puede utilizar con otros combinadores. Es útil cuando la función de agregado no acepta la entrada vacía.

Consulta:

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

Resultado:

``` text
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

## -Remuestrear {#agg-functions-combinator-resample}

Permite dividir los datos en grupos y, a continuación, agregar por separado los datos de esos grupos. Los grupos se crean dividiendo los valores de una columna en intervalos.

``` sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**Parámetros**

-   `start` — Starting value of the whole required interval for `resampling_key` valor.
-   `stop` — Ending value of the whole required interval for `resampling_key` valor. Todo el intervalo no incluye el `stop` valor `[start, stop)`.
-   `step` — Step for separating the whole interval into subintervals. The `aggFunction` se ejecuta sobre cada uno de esos subintervalos de forma independiente.
-   `resampling_key` — Column whose values are used for separating data into intervals.
-   `aggFunction_params` — `aggFunction` parámetros.

**Valores devueltos**

-   Matriz de `aggFunction` resultados para cada subintervalo.

**Ejemplo**

Considere el `people` con los siguientes datos:

``` text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

Obtengamos los nombres de las personas cuya edad se encuentra en los intervalos de `[30,60)` y `[60,75)`. Como usamos la representación entera para la edad, obtenemos edades en el `[30, 59]` y `[60,74]` intervalo.

Para agregar nombres en una matriz, usamos el [Método de codificación de datos:](reference.md#agg_function-grouparray) función de agregado. Se necesita un argumento. En nuestro caso, es el `name` columna. El `groupArrayResample` función debe utilizar el `age` columna para agregar nombres por edad. Para definir los intervalos requeridos, pasamos el `30, 75, 30` discusiones sobre el `groupArrayResample` función.

``` sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

``` text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

Considera los resultados.

`Jonh` est? fuera de la muestra porque es demasiado joven. Otras personas se distribuyen de acuerdo con los intervalos de edad especificados.

Ahora vamos a contar el número total de personas y su salario promedio en los intervalos de edad especificados.

``` sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

``` text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/agg_functions/combinators/) <!--hide-->
