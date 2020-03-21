# Funciones de orden superior {#higher-order-functions}

## `->` operador, función lambda (params, expr) {#operator-lambdaparams-expr-function}

Permite describir una función lambda para pasar a una función de orden superior. El lado izquierdo de la flecha tiene un parámetro formal, que es cualquier ID, o múltiples parámetros formales: cualquier ID en una tupla. El lado derecho de la flecha tiene una expresión que puede usar estos parámetros formales, así como cualquier columnas de tabla.

Ejemplos: `x -> 2 * x, str -> str != Referer.`

Las funciones de orden superior solo pueden aceptar funciones lambda como su argumento funcional.

Una función lambda que acepta múltiples argumentos se puede pasar a una función de orden superior. En este caso, a la función de orden superior se le pasan varias matrices de idéntica longitud a las que corresponderán estos argumentos.

Para algunas funciones, tales como [arrayCount](#higher_order_functions-array-count) o [arraySum](#higher_order_functions-array-count), el primer argumento (la función lambda) se puede omitir. En este caso, se supone un mapeo idéntico.

No se puede omitir una función lambda para las siguientes funciones:

-   [arrayMap](#higher_order_functions-array-map)
-   [arrayFilter](#higher_order_functions-array-filter)
-   [arrayFill](#higher_order_functions-array-fill)
-   [arrayReverseFill](#higher_order_functions-array-reverse-fill)
-   [arraySplit](#higher_order_functions-array-split)
-   [arrayReverseSplit](#higher_order_functions-array-reverse-split)
-   [arrayFirst](#higher_order_functions-array-first)
-   [arrayFirstIndex](#higher_order_functions-array-first-index)

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-map}

Devuelve una matriz obtenida de la aplicación original `func` función a cada elemento en el `arr` matriz.

Ejemplos:

``` sql
SELECT arrayMap(x -> (x + 2), [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,4,5] │
└─────────┘
```

En el ejemplo siguiente se muestra cómo crear una tupla de elementos de diferentes matrices:

``` sql
SELECT arrayMap((x, y) -> (x, y), [1, 2, 3], [4, 5, 6]) AS res
```

``` text
┌─res─────────────────┐
│ [(1,4),(2,5),(3,6)] │
└─────────────────────┘
```

Tenga en cuenta que el primer argumento (función lambda) no se puede omitir en el `arrayMap` función.

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-filter}

Devuelve una matriz que contiene sólo los elementos en `arr1` para los cuales `func` devuelve algo distinto de 0.

Ejemplos:

``` sql
SELECT arrayFilter(x -> x LIKE '%World%', ['Hello', 'abc World']) AS res
```

``` text
┌─res───────────┐
│ ['abc World'] │
└───────────────┘
```

``` sql
SELECT
    arrayFilter(
        (i, x) -> x LIKE '%World%',
        arrayEnumerate(arr),
        ['Hello', 'abc World'] AS arr)
    AS res
```

``` text
┌─res─┐
│ [2] │
└─────┘
```

Tenga en cuenta que el primer argumento (función lambda) no se puede omitir en el `arrayFilter` función.

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-fill}

Escanear a través de `arr1` desde el primer elemento hasta el último elemento y reemplazar `arr1[i]` por `arr1[i - 1]` si `func` devuelve 0. El primer elemento de `arr1` no será reemplazado.

Ejemplos:

``` sql
SELECT arrayFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res──────────────────────────────┐
│ [1,1,3,11,12,12,12,5,6,14,14,14] │
└──────────────────────────────────┘
```

Tenga en cuenta que el primer argumento (función lambda) no se puede omitir en el `arrayFill` función.

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-reverse-fill}

Escanear a través de `arr1` del último elemento al primer elemento y reemplace `arr1[i]` por `arr1[i + 1]` si `func` devuelve 0. El último elemento de `arr1` no será reemplazado.

Ejemplos:

``` sql
SELECT arrayReverseFill(x -> not isNull(x), [1, null, 3, 11, 12, null, null, 5, 6, 14, null, null]) AS res
```

``` text
┌─res────────────────────────────────┐
│ [1,3,3,11,12,5,5,5,6,14,NULL,NULL] │
└────────────────────────────────────┘
```

Tenga en cuenta que el primer argumento (función lambda) no se puede omitir en el `arrayReverseFill` función.

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-split}

Dividir `arr1` en múltiples matrices. Cuando `func` devuelve algo distinto de 0, la matriz se dividirá en el lado izquierdo del elemento. La matriz no se dividirá antes del primer elemento.

Ejemplos:

``` sql
SELECT arraySplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res─────────────┐
│ [[1,2,3],[4,5]] │
└─────────────────┘
```

Tenga en cuenta que el primer argumento (función lambda) no se puede omitir en el `arraySplit` función.

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-reverse-split}

Dividir `arr1` en múltiples matrices. Cuando `func` devuelve algo distinto de 0, la matriz se dividirá en el lado derecho del elemento. La matriz no se dividirá después del último elemento.

Ejemplos:

``` sql
SELECT arrayReverseSplit((x, y) -> y, [1, 2, 3, 4, 5], [1, 0, 0, 1, 0]) AS res
```

``` text
┌─res───────────────┐
│ [[1],[2,3,4],[5]] │
└───────────────────┘
```

Tenga en cuenta que el primer argumento (función lambda) no se puede omitir en el `arraySplit` función.

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-count}

Devuelve el número de elementos de la matriz arr para los cuales func devuelve algo distinto de 0. Si ‘func’ no se especifica, devuelve el número de elementos distintos de cero en la matriz.

### ¿Cómo puedo hacerlo?, …) {#arrayexistsfunc-arr1}

Devuelve 1 si hay al menos un elemento en ‘arr’ para los cuales ‘func’ devuelve algo distinto de 0. De lo contrario, devuelve 0.

### ¿Cómo puedo hacerlo?, …) {#arrayallfunc-arr1}

Devuelve 1 si ‘func’ devuelve algo distinto de 0 para todos los elementos en ‘arr’. De lo contrario, devuelve 0.

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-sum}

Devuelve la suma de la ‘func’ valor. Si se omite la función, simplemente devuelve la suma de los elementos de la matriz.

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-first}

Devuelve el primer elemento en el ‘arr1’ matriz para la cual ‘func’ devuelve algo distinto de 0.

Tenga en cuenta que el primer argumento (función lambda) no se puede omitir en el `arrayFirst` función.

### ¿Cómo puedo hacerlo?, …) {#higher-order-functions-array-first-index}

Devuelve el índice del primer elemento ‘arr1’ matriz para la cual ‘func’ devuelve algo distinto de 0.

Tenga en cuenta que el primer argumento (función lambda) no se puede omitir en el `arrayFirstIndex` función.

### ¿Cómo puedo hacerlo?, …) {#arraycumsumfunc-arr1}

Devuelve una matriz de sumas parciales de elementos en la matriz de origen (una suma en ejecución). Si el `func` se especifica la función, luego los valores de los elementos de la matriz se convierten mediante esta función antes de sumar.

Ejemplo:

``` sql
SELECT arrayCumSum([1, 1, 1, 1]) AS res
```

``` text
┌─res──────────┐
│ [1, 2, 3, 4] │
└──────────────┘
```

### Información de archivo) {#arraycumsumnonnegativearr}

Lo mismo que `arrayCumSum`, devuelve una matriz de sumas parciales de elementos en la matriz de origen (una suma en ejecución). Diferente `arrayCumSum`, cuando el valor devuelto contiene un valor menor que cero, el valor se reemplaza con cero y el cálculo posterior se realiza con cero parámetros. Por ejemplo:

``` sql
SELECT arrayCumSumNonNegative([1, 1, -4, 1]) AS res
```

``` text
┌─res───────┐
│ [1,2,0,1] │
└───────────┘
```

### ¿Cómo puedo hacerlo?, …) {#arraysortfunc-arr1}

Devuelve una matriz como resultado de ordenar los elementos de `arr1` en orden ascendente. Si el `func` se especifica la función, el orden de clasificación se determina por el resultado de la función `func` aplicado a los elementos de la matriz (arrays)

El [Transformación de Schwartzian](https://en.wikipedia.org/wiki/Schwartzian_transform) se utiliza para mejorar la eficiencia de clasificación.

Ejemplo:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]);
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Para obtener más información sobre el `arraySort` método, véase el [Funciones para trabajar con matrices](array_functions.md#array_functions-sort) apartado.

### ¿Cómo puedo hacerlo?, …) {#arrayreversesortfunc-arr1}

Devuelve una matriz como resultado de ordenar los elementos de `arr1` en orden descendente. Si el `func` se especifica la función, el orden de clasificación se determina por el resultado de la función `func` Aplicado a los elementos de la matriz (arrays).

Ejemplo:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

Para obtener más información sobre el `arrayReverseSort` método, véase el [Funciones para trabajar con matrices](array_functions.md#array_functions-reverse-sort) apartado.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/higher_order_functions/) <!--hide-->
