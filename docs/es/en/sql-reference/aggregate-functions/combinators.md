---
description: 'Documentación sobre los combinadores de funciones de agregación'
sidebar_label: 'Combinadores'
sidebar_position: 37
slug: /sql-reference/aggregate-functions/combinators
title: 'Combinadores de funciones de agregación'
doc_type: 'reference'
---

Al nombre de una función de agregación se le puede añadir un sufijo. Esto cambia la forma en que funciona.

<div id="-if">
  ## -If
</div>

El sufijo -If puede añadirse al nombre de cualquier función de agregación. En este caso, la función de agregación acepta un argumento adicional: una condición (de tipo Uint8). La función de agregación procesa solo las filas que cumplen la condición. Si la condición no se cumple ni una sola vez, devuelve un valor por defecto (normalmente ceros o cadenas vacías).

Ejemplos: `sumIf(column, cond)`, `countIf(cond)`, `avgIf(x, cond)`, `quantilesTimingIf(level1, level2)(x, cond)`, `argMinIf(arg, val, cond)` y así sucesivamente.

Con las funciones de agregación condicionales, puede calcular agregados para varias condiciones a la vez, sin usar subconsultas ni `JOIN`s. Por ejemplo, las funciones de agregación condicionales pueden usarse para implementar la funcionalidad de comparación de segmentos.

<div id="-array">
  ## -Array
</div>

El sufijo -Array puede añadirse a cualquier función de agregación. En este caso, la función de agregación toma argumentos del tipo &#39;Array(T)&#39; (arrays) en lugar de argumentos de tipo &#39;T&#39;. Si la función de agregación acepta varios argumentos, estos deben ser arrays de la misma longitud. Al procesar arrays, la función de agregación se comporta como la función de agregación original aplicada a todos los elementos de los arrays.

Ejemplo 1: `sumArray(arr)` - Suma todos los elementos de todos los arrays &#39;arr&#39;. En este ejemplo, podría haberse escrito de forma más simple: `sum(arraySum(arr))`.

Ejemplo 2: `uniqArray(arr)` – Cuenta el número de elementos únicos de todos los arrays &#39;arr&#39;. Esto podría hacerse de forma más sencilla: `uniq(arrayJoin(arr))`, pero no siempre es posible añadir &#39;arrayJoin&#39; a una consulta.

-If y -Array pueden combinarse. Sin embargo, &#39;Array&#39; debe ir primero y luego &#39;If&#39;. Ejemplos: `uniqArrayIf(arr, cond)`, `quantilesTimingArrayIf(level1, level2)(arr, cond)`. Debido a este orden, el argumento &#39;cond&#39; no será un array.

<div id="-map">
  ## -Map
</div>

El sufijo `-Map` puede añadirse a cualquier función de agregación. Esto crea una función de agregación que recibe un tipo Map como argumento y agrega por separado los valores de cada clave del mapa mediante la función de agregación especificada. El resultado también es de tipo Map.

**Ejemplo**

```sql
CREATE TABLE map_map(
    date Date,
    timeslot DateTime,
    status Map(String, UInt64)
) ENGINE = MergeTree
ORDER BY ();

INSERT INTO map_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', (['a', 'b', 'c'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', (['c', 'd', 'e'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['d', 'e', 'f'], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', (['f', 'g', 'g'], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(status),
    avgMap(status),
    minMap(status)
FROM map_map
GROUP BY timeslot;

┌────────────timeslot─┬─sumMap(status)───────────────────────┬─avgMap(status)───────────────────────┬─minMap(status)───────────────────────┐
│ 2000-01-01 00:00:00 │ {'a':10,'b':10,'c':20,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │ {'a':10,'b':10,'c':10,'d':10,'e':10} │
│ 2000-01-01 00:01:00 │ {'d':10,'e':10,'f':20,'g':20}        │ {'d':10,'e':10,'f':10,'g':10}        │ {'d':10,'e':10,'f':10,'g':10}        │
└─────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┴──────────────────────────────────────┘
```

<div id="-simplestate">
  ## -SimpleState
</div>

Si se aplica este combinador, la función de agregación devuelve el mismo valor, pero con un tipo diferente. Se trata de una [SimpleAggregateFunction(...)](../../sql-reference/data-types/simpleaggregatefunction.md) que puede almacenarse en una tabla para trabajar con tablas [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).

**Sintaxis**

```sql
<aggFunction>SimpleState(x)
```

**Argumentos**

* `x` — Parámetros de la función de agregación.

**Valores devueltos**

El valor de una función de agregación del tipo `SimpleAggregateFunction(...)`.

**Ejemplo**

```sql title="Query"
WITH anySimpleState(number) AS c SELECT toTypeName(c), c FROM numbers(1);
```

```text title="Response"
┌─toTypeName(c)────────────────────────┬─c─┐
│ SimpleAggregateFunction(any, UInt64) │ 0 │
└──────────────────────────────────────┴───┘
```

<div id="-state">
  ## -State
</div>

Si aplica este combinador, la función de agregación no devuelve el valor resultante (como el número de valores únicos de la función [uniq](/es/sql-reference/aggregate-functions/reference/uniq)), sino un estado intermedio de la agregación (para `uniq`, se trata de la tabla hash para calcular el número de valores únicos). Es una `AggregateFunction(...)` que puede usarse para un procesamiento posterior o almacenarse en una tabla para completar la agregación más adelante.

:::note
Tenga en cuenta que -MapState no es invariante para los mismos datos, ya que el orden de los datos en el estado intermedio puede cambiar, aunque esto no afecta a la ingestión de estos datos.
:::

Para trabajar con estos estados, use:

* el motor de tabla [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md).
* la función [finalizeAggregation](/es/sql-reference/functions/other-functions#finalizeAggregation).
* la función [runningAccumulate](../../sql-reference/functions/other-functions.md#runningAccumulate).
* el combinador [-Merge](#-merge).
* el combinador [-MergeState](#-mergestate).

<div id="-merge">
  ## -Merge
</div>

Si se aplica este combinador, la función de agregación toma el estado intermedio de agregación como argumento, combina los estados para finalizar la agregación y devuelve el valor resultante.

<div id="-mergestate">
  ## -MergeState
</div>

Combina los estados de agregación intermedios de la misma forma que el combinador -Merge. Sin embargo, no devuelve el valor resultante, sino un estado de agregación intermedio, similar al combinador -State.

<div id="-foreach">
  ## -ForEach
</div>

Convierte una función de agregación para tablas en una función de agregación para arrays que agrega los elementos correspondientes de cada array y devuelve un array de resultados. Por ejemplo, `sumForEach` para los arrays `[1, 2]`, `[3, 4, 5]` y `[6, 7]` devuelve el resultado `[10, 13, 5]` tras sumar los elementos correspondientes de cada array.

<div id="-tuple">
  ## -Tuple
</div>

El sufijo `-Tuple` puede añadirse a cualquier función de agregación. La función combinada toma un argumento de tipo `Tuple` por cada argumento de la función de agregación subyacente; todas las tuplas deben tener el mismo número de elementos. La agregación se aplica de forma independiente en la posición de cada elemento, tomando el elemento correspondiente de cada `Tuple`, y devuelve una `Tuple` con los resultados.

Si la primera `Tuple` de entrada tiene nombres de elemento explícitos, estos se conservan en el resultado.

Las funciones de agregación que manejan por sí mismas los valores `NULL` (`anyRespectNulls`, `anyLastRespectNulls`, el modificador `RESPECT NULLS`) no admiten el tipo `Nullable(Tuple(...))` como argumento; use elementos `Nullable` en su lugar.

**Sintaxis**

```sql
<aggFunction>Tuple(tuple1[, tuple2, ...])
```

**Argumentos**

* `tuple1[, tuple2, ...]` — Columnas de tipo `Tuple`, una por cada argumento de la función de agregación subyacente, todas con el mismo número de elementos. Cada elemento debe ser de un tipo compatible con la función de agregación subyacente en esa posición.

**Valores devueltos**

* Un `Tuple` que contiene el resultado de aplicar la función de agregación a cada elemento de forma independiente.

Tipo: `Tuple(aggFunction(element1), aggFunction(element2), ...)`.

**Ejemplo**

Consulta:

```sql
SELECT sumTuple(t) FROM
(
    SELECT tuple(toInt64(1), toFloat64(2.5)) AS t
    UNION ALL
    SELECT tuple(toInt64(3), toFloat64(4.5))
    UNION ALL
    SELECT tuple(toInt64(5), toFloat64(6.5))
);
```

Resultado:

```text
┌─sumTuple(t)─┐
│ (9,13.5)    │
└─────────────┘
```

Uso de `GROUP BY`:

```sql
SELECT
    k,
    avgTuple(t)
FROM
(
    SELECT
        number % 2 AS k,
        tuple(toInt64(number), toFloat64(number) * 1.5) AS t
    FROM numbers(6)
)
GROUP BY k
ORDER BY k;
```

```text
┌─k─┬─avgTuple(t)─┐
│ 0 │ (2,3)       │
│ 1 │ (3,4.5)     │
└───┴─────────────┘
```

Uso con una función de agregación con varios argumentos: cada argumento `Tuple` aporta un argumento de la función subyacente y los elementos se emparejan por posición:

```text
corrTuple((a1, a2), (b1, b2)) = (corr(a1, b1), corr(a2, b2))
```

```sql
SELECT corrTuple((a1, a2), (b1, b2))
FROM
(
    SELECT
        toFloat64(number) AS a1,
        toFloat64(number * 2) AS a2,
        toFloat64(100 - number) AS b1,
        toFloat64(number * 3) AS b2
    FROM numbers(10)
);
```

```text
┌─corrTuple((a1, a2), (b1, b2))─┐
│ (-1,1)                        │
└───────────────────────────────┘
```

`a1` y `b1` están anticorrelacionados, mientras que `a2` y `b2` son proporcionales, por lo que el resultado es `(-1, 1)`.

`-Tuple` se puede combinar con otros combinadores, como `-If`. Por ejemplo: `sumTupleIf(tuple_column, cond)`.

<div id="-distinct">
  ## -Distinct
</div>

Cada combinación única de argumentos se agregará una sola vez. Los valores repetidos se ignoran.
Ejemplos: `sum(DISTINCT x)` (o `sumDistinct(x)`), `groupArray(DISTINCT x)` (o `groupArrayDistinct(x)`), `corrStable(DISTINCT x, y)` (o `corrStableDistinct(x, y)`) y así sucesivamente.

<div id="-ordefault">
  ## -OrDefault
</div>

Cambia el comportamiento de una función de agregación.

Si una función de agregación no tiene valores de entrada, este combinador devuelve el valor predeterminado de su tipo de dato de retorno. Se aplica a las funciones de agregación que pueden aceptar datos de entrada vacíos.

`-OrDefault` puede usarse con otros combinadores.

**Sintaxis**

```sql
<aggFunction>OrDefault(x)
```

**Argumentos**

* `x` — Parámetros de la función de agregación.

**Valores devueltos**

Devuelve el valor predeterminado del tipo de retorno de una función de agregación si no hay nada que agregar.

El tipo depende de la función de agregación que se utilice.

**Ejemplo**

```sql title="Query"
SELECT avg(number), avgOrDefault(number) FROM numbers(0)
```

```text title="Response"
┌─avg(number)─┬─avgOrDefault(number)─┐
│         nan │                    0 │
└─────────────┴──────────────────────┘
```

Además, `-OrDefault` también puede usarse con otros combinadores. Resulta útil cuando la función de agregación no admite entradas vacías.

```sql title="Query"
SELECT avgOrDefaultIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrDefaultIf(x, greater(x, 10))─┐
│                              0.00 │
└───────────────────────────────────┘
```

<div id="-ornull">
  ## -OrNull
</div>

Cambia el comportamiento de una función de agregación.

Este combinador convierte el resultado de una función de agregación en el tipo de dato [Nullable](../../sql-reference/data-types/nullable.md). Si la función de agregación no tiene valores para calcular, devuelve [NULL](/es/operations/settings/formats#input_format_null_as_default).

`-OrNull` puede usarse con otros combinadores.

**Sintaxis**

```sql
<aggFunction>OrNull(x)
```

**Argumentos**

* `x` — Parámetros de la función de agregación.

**Valores devueltos**

* El resultado de la función de agregación, convertido al tipo de dato `Nullable`.
* `NULL`, si no hay valores para agregar.

Tipo: `Nullable(tipo de retorno de la función de agregación)`.

**Ejemplo**

Añada `-orNull` al final del nombre de la función de agregación.

```sql title="Query"
SELECT sumOrNull(number), toTypeName(sumOrNull(number)) FROM numbers(10) WHERE number > 10
```

```text title="Response"
┌─sumOrNull(number)─┬─toTypeName(sumOrNull(number))─┐
│              ᴺᵁᴸᴸ │ Nullable(UInt64)              │
└───────────────────┴───────────────────────────────┘
```

Además, `-OrNull` también puede usarse con otros combinadores. Es útil cuando la función de agregación no admite una entrada vacía.

```sql title="Query"
SELECT avgOrNullIf(x, x > 10)
FROM
(
    SELECT toDecimal32(1.23, 2) AS x
)
```

```text title="Response"
┌─avgOrNullIf(x, greater(x, 10))─┐
│                           ᴺᵁᴸᴸ │
└────────────────────────────────┘
```

<div id="-resample">
  ## -Resample
</div>

Permite dividir los datos en grupos y agregarlos por separado dentro de cada grupo. Los grupos se crean dividiendo los valores de una columna en intervalos.

```sql
<aggFunction>Resample(start, end, step)(<aggFunction_params>, resampling_key)
```

**Argumentos**

* `start` — Valor inicial del intervalo completo requerido para los valores de `resampling_key`.
* `stop` — Valor final del intervalo completo requerido para los valores de `resampling_key`. El intervalo completo no incluye el valor `stop`: `[start, stop)`.
* `step` — Paso para dividir el intervalo completo en subintervalos. `aggFunction` se ejecuta en cada uno de estos subintervalos de forma independiente.
* `resampling_key` — Columna cuyos valores se utilizan para separar los datos en intervalos.
* `aggFunction_params` — Parámetros de `aggFunction`.

**Valores devueltos**

* Array de resultados de `aggFunction` para cada subintervalo.

**Ejemplo**

Considere la tabla `people` con los siguientes datos:

```text
┌─name───┬─age─┬─wage─┐
│ John   │  16 │   10 │
│ Alice  │  30 │   15 │
│ Mary   │  35 │    8 │
│ Evelyn │  48 │ 11.5 │
│ David  │  62 │  9.9 │
│ Brian  │  60 │   16 │
└────────┴─────┴──────┘
```

Obtengamos los nombres de las personas cuyas edades se encuentran en los intervalos `[30,60)` y `[60,75)`. Como usamos una representación entera para la edad, obtenemos edades en los intervalos `[30, 59]` y `[60,74]`.

Para agregar nombres en un Array, usamos la función de agregación [groupArray](/es/sql-reference/aggregate-functions/reference/grouparray). Acepta un argumento. En nuestro caso, es la columna `name`. La función `groupArrayResample` debe usar la columna `age` para agregar los nombres por edad. Para definir los intervalos necesarios, pasamos los argumentos `30, 75, 30` a la función `groupArrayResample`.

```sql
SELECT groupArrayResample(30, 75, 30)(name, age) FROM people
```

```text
┌─groupArrayResample(30, 75, 30)(name, age)─────┐
│ [['Alice','Mary','Evelyn'],['David','Brian']] │
└───────────────────────────────────────────────┘
```

Veamos los resultados.

`John` queda fuera de la muestra porque es demasiado joven. Las demás personas se distribuyen según los intervalos de edad especificados.

Ahora contemos el número total de personas y su salario medio en los intervalos de edad especificados.

```sql
SELECT
    countResample(30, 75, 30)(name, age) AS amount,
    avgResample(30, 75, 30)(wage, age) AS avg_wage
FROM people
```

```text
┌─amount─┬─avg_wage──────────────────┐
│ [3,2]  │ [11.5,12.949999809265137] │
└────────┴───────────────────────────┘
```

<div id="-argmin">
  ## -ArgMin
</div>

El sufijo -ArgMin puede añadirse al nombre de cualquier función de agregación. En este caso, la función de agregación acepta un argumento adicional, que puede ser cualquier expresión comparable. La función de agregación procesa solo las filas que tienen el valor mínimo de la expresión adicional especificada.

Ejemplos: `sumArgMin(column, expr)`, `countArgMin(expr)`, `avgArgMin(x, expr)` y así sucesivamente.

<div id="-argmax">
  ## -ArgMax
</div>

Similar al sufijo -ArgMin, pero solo procesa las filas que tienen el valor máximo de la expresión adicional especificada.

<div id="related-content">
  ## Contenido relacionado
</div>

* Blog: [Uso de los combinadores de agregación en ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)