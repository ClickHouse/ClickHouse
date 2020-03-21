# Combinadores de funciones agregadas {#aggregate-functions-combinators}

El nombre de una función agregada puede tener un sufijo anexado. Esto cambia la forma en que funciona la función de agregado.

## -Si {#agg-functions-combinator-if}

El sufijo -If se puede anexar al nombre de cualquier función agregada. En este caso, la función de agregado acepta un argumento adicional: una condición (tipo Uint8). La función de agregado procesa solo las filas que desencadenan la condición. Si la condición no se desencadenó ni una sola vez, devuelve un valor predeterminado (normalmente ceros o cadenas vacías).

Ejemplos: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` y así sucesivamente.

Con las funciones de agregado condicional, puede calcular agregados para varias condiciones a la vez, sin utilizar subconsultas y `JOIN`Por ejemplo, en Yandex.Metrica, las funciones de agregado condicional se utilizan para implementar la funcionalidad de comparación de segmentos.

## -Matriz {#agg-functions-combinator-array}

El sufijo -Array se puede agregar a cualquier función agregada. En este caso, la función de agregado toma argumentos del ‘Array(T)’ tipo (arrays) en lugar de ‘T’ argumentos de tipo. Si la función de agregado acepta varios argumentos, deben ser matrices de igual longitud. Al procesar matrices, la función de agregado funciona como la función de agregado original en todos los elementos de la matriz.

Ejemplo 1: `sumArray(arr)` - Totales de todos los elementos de todos ‘arr’ matriz. En este ejemplo, podría haber sido escrito más simplemente: `sum(arraySum(arr))`.

Ejemplo 2: `uniqArray(arr)` – Cuenta el número de elementos únicos ‘arr’ matriz. Esto podría hacerse de una manera más fácil: `uniq(arrayJoin(arr))` pero no siempre es posible añadir ‘arrayJoin’ a una consulta.

-If y -Array se pueden combinar. Obstante, ‘Array’ debe venir primero, entonces ‘If’. Ejemplos: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. Debido a este pedido, el ‘cond’ el argumento no será una matriz.

## -Estado {#agg-functions-combinator-state}

Si aplica este combinador, la función de agregado no devuelve el valor resultante (como el número de valores únicos [uniq](reference.md#agg_function-uniq) función), pero un estado intermedio de la agregación (para `uniq`, esta es la tabla hash para calcular el número de valores únicos). Este es un `AggregateFunction(...)` que puede ser utilizado para su posterior procesamiento o almacenado en una tabla para terminar de agregar más tarde.

Para trabajar con estos estados, use:

-   [AgregaciónMergeTree](../../operations/table_engines/aggregatingmergetree.md) motor de mesa.
-   [finalizeAggregation](../functions/other_functions.md#function-finalizeaggregation) función.
-   [runningAccumulate](../functions/other_functions.md#function-runningaccumulate) función.
-   [-Fusionar](#aggregate_functions_combinators_merge) combinador.
-   [-MergeState](#aggregate_functions_combinators_mergestate) combinador.

## -Fusionar {#aggregate-functions-combinators-merge}

Si aplica este combinador, la función de agregado toma el estado de agregación intermedio como argumento, combina los estados para finalizar la agregación y devuelve el valor resultante.

## -MergeState {#aggregate-functions-combinators-mergestate}

Combina los estados de agregación intermedios de la misma manera que el combinador -Merge. Sin embargo, no devuelve el valor resultante, sino un estado de agregación intermedio, similar al combinador -State.

## -ForEach {#agg-functions-combinator-foreach}

Convierte una función de agregado para tablas en una función de agregado para matrices que agrega los elementos de matriz correspondientes y devuelve una matriz de resultados. Por ejemplo, `sumForEach` para las matrices `[1, 2]`, `[3, 4, 5]`y`[6, 7]`devuelve el resultado `[10, 13, 5]` después de agregar los elementos de la matriz correspondientes.

## -OPor defecto {#agg-functions-combinator-ordefault}

Rellena el valor predeterminado del tipo devuelto de la función de agregado si no hay nada que agregar.

``` sql
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

``` text
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

## -OrNull {#agg-functions-combinator-ornull}

Llenar `null` si no hay nada que agregar. La columna de retorno será anulable.

``` sql
SELECT avg(number), avgOrNull(number) FROM numbers(0)
```

``` text
┌─avg(number)─┬─avgOrNull(number)─┐
│         nan │              ᴺᵁᴸᴸ │
└─────────────┴───────────────────┘
```

-OrDefault y -OrNull se pueden combinar con otros combinadores. Es útil cuando la función de agregado no acepta la entrada vacía.

``` sql
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

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

-   `start` — Valor inicial de todo el intervalo requerido para `resampling_key` valor.
-   `stop` — valor final de todo el intervalo requerido para `resampling_key` valor. Todo el intervalo no incluye el `stop` valor `[start, stop)`.
-   `step` — Paso para separar todo el intervalo en subintervalos. El `aggFunction` se ejecuta sobre cada uno de esos subintervalos de forma independiente.
-   `resampling_key` — Columna cuyos valores se utilizan para separar los datos en intervalos.
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

Vamos a obtener los nombres de las personas cuya edad se encuentra en los intervalos de `[30,60)` y `[60,75)`. Como usamos la representación entera para la edad, obtenemos edades en el `[30, 59]` y `[60,74]` intervalo.

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

`Jonh` está fuera de la muestra porque es demasiado joven. Otras personas se distribuyen de acuerdo con los intervalos de edad especificados.

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

[Artículo Original](https://clickhouse.tech/docs/es/query_language/agg_functions/combinators/) <!--hide-->
