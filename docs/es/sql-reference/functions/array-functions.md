---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: Trabajar con matrices
---

# Funciones para trabajar con matrices {#functions-for-working-with-arrays}

## vaciar {#function-empty}

Devuelve 1 para una matriz vacía, o 0 para una matriz no vacía.
El tipo de resultado es UInt8.
La función también funciona para cadenas.

## notEmpty {#function-notempty}

Devuelve 0 para una matriz vacía, o 1 para una matriz no vacía.
El tipo de resultado es UInt8.
La función también funciona para cadenas.

## longitud {#array_functions-length}

Devuelve el número de elementos de la matriz.
El tipo de resultado es UInt64.
La función también funciona para cadenas.

## Para obtener más información, consulta nuestra Política de privacidad y nuestras Condiciones de uso {#emptyarrayuint8-emptyarrayuint16-emptyarrayuint32-emptyarrayuint64}

## Para obtener más información, consulta nuestra Política de privacidad y nuestras Condiciones de uso {#emptyarrayint8-emptyarrayint16-emptyarrayint32-emptyarrayint64}

## Para obtener más información, consulta nuestra Política de privacidad y nuestras Condiciones de uso {#emptyarrayfloat32-emptyarrayfloat64}

## emptyArrayDate, emptyArrayDateTime {#emptyarraydate-emptyarraydatetime}

## emptyArrayString {#emptyarraystring}

Acepta cero argumentos y devuelve una matriz vacía del tipo apropiado.

## emptyArrayToSingle {#emptyarraytosingle}

Acepta una matriz vacía y devuelve una matriz de un elemento que es igual al valor predeterminado.

## rango(final), rango(inicio, fin \[, paso\]) {#rangeend-rangestart-end-step}

Devuelve una matriz de números de principio a fin-1 por paso.
Si el argumento `start` no se especifica, el valor predeterminado es 0.
Si el argumento `step` no se especifica, el valor predeterminado es 1.
Se comporta casi como pitónico `range`. Pero la diferencia es que todos los tipos de argumentos deben ser `UInt` numero.
Por si acaso, se produce una excepción si se crean matrices con una longitud total de más de 100,000,000 de elementos en un bloque de datos.

## array(x1, …), operator \[x1, …\] {#arrayx1-operator-x1}

Crea una matriz a partir de los argumentos de la función.
Los argumentos deben ser constantes y tener tipos que tengan el tipo común más pequeño. Se debe pasar al menos un argumento, porque de lo contrario no está claro qué tipo de matriz crear. Es decir, no puede usar esta función para crear una matriz vacía (para hacerlo, use el ‘emptyArray\*’ función descrita anteriormente).
Devuelve un ‘Array(T)’ tipo resultado, donde ‘T’ es el tipo común más pequeño de los argumentos pasados.

## arrayConcat {#arrayconcat}

Combina matrices pasadas como argumentos.

``` sql
arrayConcat(arrays)
```

**Parámetros**

-   `arrays` – Arbitrary number of arguments of [Matriz](../../sql-reference/data-types/array.md) tipo.
    **Ejemplo**

<!-- -->

``` sql
SELECT arrayConcat([1, 2], [3, 4], [5, 6]) AS res
```

``` text
┌─res───────────┐
│ [1,2,3,4,5,6] │
└───────────────┘
```

## Por ejemplo, el operador arr\[n\] {#arrayelementarr-n-operator-arrn}

Obtener el elemento con el índice `n` de la matriz `arr`. `n` debe ser de cualquier tipo entero.
Los índices de una matriz comienzan desde uno.
Los índices negativos son compatibles. En este caso, selecciona el elemento correspondiente numerado desde el final. Por ejemplo, `arr[-1]` es el último elemento de la matriz.

Si el índice cae fuera de los límites de una matriz, devuelve algún valor predeterminado (0 para números, una cadena vacía para cadenas, etc.), a excepción del caso con una matriz no constante y un índice constante 0 (en este caso habrá un error `Array indices are 1-based`).

## Tiene(arr, elem) {#hasarr-elem}

Comprueba si el ‘arr’ la matriz tiene el ‘elem’ elemento.
Devuelve 0 si el elemento no está en la matriz, o 1 si es.

`NULL` se procesa como un valor.

``` sql
SELECT has([1, 2, NULL], NULL)
```

``` text
┌─has([1, 2, NULL], NULL)─┐
│                       1 │
└─────────────────────────┘
```

## TieneTodo {#hasall}

Comprueba si una matriz es un subconjunto de otra.

``` sql
hasAll(set, subset)
```

**Parámetros**

-   `set` – Array of any type with a set of elements.
-   `subset` – Array of any type with elements that should be tested to be a subset of `set`.

**Valores de retorno**

-   `1`, si `set` contiene todos los elementos de `subset`.
-   `0`, de lo contrario.

**Propiedades peculiares**

-   Una matriz vacía es un subconjunto de cualquier matriz.
-   `Null` procesado como un valor.
-   El orden de los valores en ambas matrices no importa.

**Ejemplos**

`SELECT hasAll([], [])` devoluciones 1.

`SELECT hasAll([1, Null], [Null])` devoluciones 1.

`SELECT hasAll([1.0, 2, 3, 4], [1, 3])` devoluciones 1.

`SELECT hasAll(['a', 'b'], ['a'])` devoluciones 1.

`SELECT hasAll([1], ['a'])` devuelve 0.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [3, 5]])` devuelve 0.

## TieneCualquier {#hasany}

Comprueba si dos matrices tienen intersección por algunos elementos.

``` sql
hasAny(array1, array2)
```

**Parámetros**

-   `array1` – Array of any type with a set of elements.
-   `array2` – Array of any type with a set of elements.

**Valores de retorno**

-   `1`, si `array1` y `array2` tienen un elemento similar al menos.
-   `0`, de lo contrario.

**Propiedades peculiares**

-   `Null` procesado como un valor.
-   El orden de los valores en ambas matrices no importa.

**Ejemplos**

`SELECT hasAny([1], [])` devoluciones `0`.

`SELECT hasAny([Null], [Null, 1])` devoluciones `1`.

`SELECT hasAny([-128, 1., 512], [1])` devoluciones `1`.

`SELECT hasAny([[1, 2], [3, 4]], ['a', 'c'])` devoluciones `0`.

`SELECT hasAll([[1, 2], [3, 4]], [[1, 2], [1, 2]])` devoluciones `1`.

## ¿Cómo puedo hacerlo?) {#indexofarr-x}

Devuelve el índice de la primera ‘x’ elemento (comenzando desde 1) si está en la matriz, o 0 si no lo está.

Ejemplo:

``` sql
SELECT indexOf([1, 3, NULL, NULL], NULL)
```

``` text
┌─indexOf([1, 3, NULL, NULL], NULL)─┐
│                                 3 │
└───────────────────────────────────┘
```

Elementos establecidos en `NULL` se manejan como valores normales.

## Cuenta igual (arr, x) {#countequalarr-x}

Devuelve el número de elementos de la matriz igual a x. Equivalente a arrayCount (elem -\> elem = x, arr).

`NULL` los elementos se manejan como valores separados.

Ejemplo:

``` sql
SELECT countEqual([1, 2, NULL, NULL], NULL)
```

``` text
┌─countEqual([1, 2, NULL, NULL], NULL)─┐
│                                    2 │
└──────────────────────────────────────┘
```

## Información detallada) {#array_functions-arrayenumerate}

Returns the array \[1, 2, 3, …, length (arr) \]

Esta función se utiliza normalmente con ARRAY JOIN. Permite contar algo solo una vez para cada matriz después de aplicar ARRAY JOIN . Ejemplo:

``` sql
SELECT
    count() AS Reaches,
    countIf(num = 1) AS Hits
FROM test.hits
ARRAY JOIN
    GoalsReached,
    arrayEnumerate(GoalsReached) AS num
WHERE CounterID = 160656
LIMIT 10
```

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

En este ejemplo, Reaches es el número de conversiones (las cadenas recibidas después de aplicar ARRAY JOIN) y Hits es el número de páginas vistas (cadenas antes de ARRAY JOIN). En este caso particular, puede obtener el mismo resultado de una manera más fácil:

``` sql
SELECT
    sum(length(GoalsReached)) AS Reaches,
    count() AS Hits
FROM test.hits
WHERE (CounterID = 160656) AND notEmpty(GoalsReached)
```

``` text
┌─Reaches─┬──Hits─┐
│   95606 │ 31406 │
└─────────┴───────┘
```

Esta función también se puede utilizar en funciones de orden superior. Por ejemplo, puede usarlo para obtener índices de matriz para elementos que coinciden con una condición.

## arrayEnumerateUniq(arr, …) {#arrayenumerateuniqarr}

Devuelve una matriz del mismo tamaño que la matriz de origen, indicando para cada elemento cuál es su posición entre los elementos con el mismo valor.
Por ejemplo: arrayEnumerateUniq(\[10, 20, 10, 30\]) = \[1, 1, 2, 1\].

Esta función es útil cuando se utiliza ARRAY JOIN y la agregación de elementos de matriz.
Ejemplo:

``` sql
SELECT
    Goals.ID AS GoalID,
    sum(Sign) AS Reaches,
    sumIf(Sign, num = 1) AS Visits
FROM test.visits
ARRAY JOIN
    Goals,
    arrayEnumerateUniq(Goals.ID) AS num
WHERE CounterID = 160656
GROUP BY GoalID
ORDER BY Reaches DESC
LIMIT 10
```

``` text
┌──GoalID─┬─Reaches─┬─Visits─┐
│   53225 │    3214 │   1097 │
│ 2825062 │    3188 │   1097 │
│   56600 │    2803 │    488 │
│ 1989037 │    2401 │    365 │
│ 2830064 │    2396 │    910 │
│ 1113562 │    2372 │    373 │
│ 3270895 │    2262 │    812 │
│ 1084657 │    2262 │    345 │
│   56599 │    2260 │    799 │
│ 3271094 │    2256 │    812 │
└─────────┴─────────┴────────┘
```

En este ejemplo, cada ID de objetivo tiene un cálculo del número de conversiones (cada elemento de la estructura de datos anidados Objetivos es un objetivo alcanzado, al que nos referimos como conversión) y el número de sesiones. Sin ARRAY JOIN, habríamos contado el número de sesiones como sum(Sign) . Pero en este caso particular, las filas se multiplicaron por la estructura de Objetivos anidados, por lo que para contar cada sesión una vez después de esto, aplicamos una condición al valor de la función arrayEnumerateUniq(Goals.ID) .

La función arrayEnumerateUniq puede tomar varias matrices del mismo tamaño que los argumentos. En este caso, la singularidad se considera para tuplas de elementos en las mismas posiciones en todas las matrices.

``` sql
SELECT arrayEnumerateUniq([1, 1, 1, 2, 2, 2], [1, 1, 2, 1, 1, 2]) AS res
```

``` text
┌─res───────────┐
│ [1,2,1,1,2,1] │
└───────────────┘
```

Esto es necesario cuando se utiliza ARRAY JOIN con una estructura de datos anidados y una agregación adicional a través de múltiples elementos de esta estructura.

## arrayPopBack {#arraypopback}

Quita el último elemento de la matriz.

``` sql
arrayPopBack(array)
```

**Parámetros**

-   `array` – Array.

**Ejemplo**

``` sql
SELECT arrayPopBack([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [1,2] │
└───────┘
```

## arrayPopFront {#arraypopfront}

Quita el primer elemento de la matriz.

``` sql
arrayPopFront(array)
```

**Parámetros**

-   `array` – Array.

**Ejemplo**

``` sql
SELECT arrayPopFront([1, 2, 3]) AS res
```

``` text
┌─res───┐
│ [2,3] │
└───────┘
```

## arrayPushBack {#arraypushback}

Agrega un elemento al final de la matriz.

``` sql
arrayPushBack(array, single_value)
```

**Parámetros**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` tipo para el tipo de datos de la matriz. Para obtener más información sobre los tipos de datos en ClickHouse, consulte “[Tipos de datos](../../sql-reference/data-types/index.md#data_types)”. Puede ser `NULL`. La función agrega un `NULL` elemento de matriz a una matriz, y el tipo de elementos de matriz se convierte en `Nullable`.

**Ejemplo**

``` sql
SELECT arrayPushBack(['a'], 'b') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayPushFront {#arraypushfront}

Agrega un elemento al principio de la matriz.

``` sql
arrayPushFront(array, single_value)
```

**Parámetros**

-   `array` – Array.
-   `single_value` – A single value. Only numbers can be added to an array with numbers, and only strings can be added to an array of strings. When adding numbers, ClickHouse automatically sets the `single_value` tipo para el tipo de datos de la matriz. Para obtener más información sobre los tipos de datos en ClickHouse, consulte “[Tipos de datos](../../sql-reference/data-types/index.md#data_types)”. Puede ser `NULL`. La función agrega un `NULL` elemento de matriz a una matriz, y el tipo de elementos de matriz se convierte en `Nullable`.

**Ejemplo**

``` sql
SELECT arrayPushFront(['b'], 'a') AS res
```

``` text
┌─res───────┐
│ ['a','b'] │
└───────────┘
```

## arrayResize {#arrayresize}

Cambia la longitud de la matriz.

``` sql
arrayResize(array, size[, extender])
```

**Parámetros:**

-   `array` — Array.
-   `size` — Required length of the array.
    -   Si `size` es menor que el tamaño original de la matriz, la matriz se trunca desde la derecha.
-   Si `size` es mayor que el tamaño inicial de la matriz, la matriz se extiende a la derecha con `extender` valores predeterminados para el tipo de datos de los elementos de la matriz.
-   `extender` — Value for extending an array. Can be `NULL`.

**Valor devuelto:**

Una matriz de longitud `size`.

**Ejemplos de llamadas**

``` sql
SELECT arrayResize([1], 3)
```

``` text
┌─arrayResize([1], 3)─┐
│ [1,0,0]             │
└─────────────────────┘
```

``` sql
SELECT arrayResize([1], 3, NULL)
```

``` text
┌─arrayResize([1], 3, NULL)─┐
│ [1,NULL,NULL]             │
└───────────────────────────┘
```

## arraySlice {#arrayslice}

Devuelve una porción de la matriz.

``` sql
arraySlice(array, offset[, length])
```

**Parámetros**

-   `array` – Array of data.
-   `offset` – Indent from the edge of the array. A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the array items begins with 1.
-   `length` - La longitud de la porción requerida. Si especifica un valor negativo, la función devuelve un segmento abierto `[offset, array_length - length)`. Si omite el valor, la función devuelve el sector `[offset, the_end_of_array]`.

**Ejemplo**

``` sql
SELECT arraySlice([1, 2, NULL, 4, 5], 2, 3) AS res
```

``` text
┌─res────────┐
│ [2,NULL,4] │
└────────────┘
```

Elementos de matriz establecidos en `NULL` se manejan como valores normales.

## arraySort(\[func,\] arr, …) {#array_functions-sort}

Ordena los elementos del `arr` matriz en orden ascendente. Si el `func` se especifica la función, el orden de clasificación está determinado por el resultado `func` función aplicada a los elementos de la matriz. Si `func` acepta múltiples argumentos, el `arraySort` función se pasa varias matrices que los argumentos de `func` corresponderá a. Los ejemplos detallados se muestran al final de `arraySort` descripci.

Ejemplo de clasificación de valores enteros:

``` sql
SELECT arraySort([1, 3, 3, 0]);
```

``` text
┌─arraySort([1, 3, 3, 0])─┐
│ [0,1,3,3]               │
└─────────────────────────┘
```

Ejemplo de ordenación de valores de cadena:

``` sql
SELECT arraySort(['hello', 'world', '!']);
```

``` text
┌─arraySort(['hello', 'world', '!'])─┐
│ ['!','hello','world']              │
└────────────────────────────────────┘
```

Considere el siguiente orden de clasificación `NULL`, `NaN` y `Inf` valor:

``` sql
SELECT arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]);
```

``` text
┌─arraySort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf])─┐
│ [-inf,-4,1,2,3,inf,nan,nan,NULL,NULL]                     │
└───────────────────────────────────────────────────────────┘
```

-   `-Inf` los valores son los primeros en la matriz.
-   `NULL` los valores son los últimos en la matriz.
-   `NaN` los valores están justo antes `NULL`.
-   `Inf` los valores están justo antes `NaN`.

Tenga en cuenta que `arraySort` es una [función de orden superior](higher-order-functions.md). Puede pasarle una función lambda como primer argumento. En este caso, el orden de clasificación está determinado por el resultado de la función lambda aplicada a los elementos de la matriz.

Consideremos el siguiente ejemplo:

``` sql
SELECT arraySort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [3,2,1] │
└─────────┘
```

For each element of the source array, the lambda function returns the sorting key, that is, \[1 –\> -1, 2 –\> -2, 3 –\> -3\]. Since the `arraySort` función ordena las teclas en orden ascendente, el resultado es \[3, 2, 1\]. Por lo tanto, el `(x) –> -x` la función lambda establece la [orden descendente](#array_functions-reverse-sort) en una clasificación.

La función lambda puede aceptar múltiples argumentos. En este caso, debe pasar el `arraySort` función varias matrices de idéntica longitud a las que corresponderán los argumentos de la función lambda. La matriz resultante constará de elementos de la primera matriz de entrada; los elementos de la siguiente matriz de entrada especifican las claves de clasificación. Por ejemplo:

``` sql
SELECT arraySort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res────────────────┐
│ ['world', 'hello'] │
└────────────────────┘
```

Aquí, los elementos que se pasan en la segunda matriz (\[2, 1\]) definen una clave de ordenación para el elemento correspondiente de la matriz de origen (\[‘hello’, ‘world’Es decir,, \[‘hello’ –\> 2, ‘world’ –\> 1\]. Since the lambda function doesn't use `x`, los valores reales de la matriz de origen no afectan el orden en el resultado. Tan, ‘hello’ será el segundo elemento en el resultado, y ‘world’ será la primera.

Otros ejemplos se muestran a continuación.

``` sql
SELECT arraySort((x, y) -> y, [0, 1, 2], ['c', 'b', 'a']) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

``` sql
SELECT arraySort((x, y) -> -y, [0, 1, 2], [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [2,1,0] │
└─────────┘
```

!!! note "Nota"
    Para mejorar la eficiencia de clasificación, el [Transformación de Schwartzian](https://en.wikipedia.org/wiki/Schwartzian_transform) se utiliza.

## arrayReverseSort(\[func,\] arr, …) {#array_functions-reverse-sort}

Ordena los elementos del `arr` matriz en orden descendente. Si el `func` se especifica la función, `arr` se ordena de acuerdo con el resultado de la `func` función aplicada a los elementos de la matriz, y luego la matriz ordenada se invierte. Si `func` acepta múltiples argumentos, el `arrayReverseSort` función se pasa varias matrices que los argumentos de `func` corresponderá a. Los ejemplos detallados se muestran al final de `arrayReverseSort` descripci.

Ejemplo de clasificación de valores enteros:

``` sql
SELECT arrayReverseSort([1, 3, 3, 0]);
```

``` text
┌─arrayReverseSort([1, 3, 3, 0])─┐
│ [3,3,1,0]                      │
└────────────────────────────────┘
```

Ejemplo de ordenación de valores de cadena:

``` sql
SELECT arrayReverseSort(['hello', 'world', '!']);
```

``` text
┌─arrayReverseSort(['hello', 'world', '!'])─┐
│ ['world','hello','!']                     │
└───────────────────────────────────────────┘
```

Considere el siguiente orden de clasificación `NULL`, `NaN` y `Inf` valor:

``` sql
SELECT arrayReverseSort([1, nan, 2, NULL, 3, nan, -4, NULL, inf, -inf]) as res;
```

``` text
┌─res───────────────────────────────────┐
│ [inf,3,2,1,-4,-inf,nan,nan,NULL,NULL] │
└───────────────────────────────────────┘
```

-   `Inf` los valores son los primeros en la matriz.
-   `NULL` los valores son los últimos en la matriz.
-   `NaN` los valores están justo antes `NULL`.
-   `-Inf` los valores están justo antes `NaN`.

Tenga en cuenta que el `arrayReverseSort` es una [función de orden superior](higher-order-functions.md). Puede pasarle una función lambda como primer argumento. Ejemplo se muestra a continuación.

``` sql
SELECT arrayReverseSort((x) -> -x, [1, 2, 3]) as res;
```

``` text
┌─res─────┐
│ [1,2,3] │
└─────────┘
```

La matriz se ordena de la siguiente manera:

1.  Al principio, la matriz de origen (\[1, 2, 3\]) se ordena de acuerdo con el resultado de la función lambda aplicada a los elementos de la matriz. El resultado es una matriz \[3, 2, 1\].
2.  Matriz que se obtiene en el paso anterior, se invierte. Entonces, el resultado final es \[1, 2, 3\].

La función lambda puede aceptar múltiples argumentos. En este caso, debe pasar el `arrayReverseSort` función varias matrices de idéntica longitud a las que corresponderán los argumentos de la función lambda. La matriz resultante constará de elementos de la primera matriz de entrada; los elementos de la siguiente matriz de entrada especifican las claves de clasificación. Por ejemplo:

``` sql
SELECT arrayReverseSort((x, y) -> y, ['hello', 'world'], [2, 1]) as res;
```

``` text
┌─res───────────────┐
│ ['hello','world'] │
└───────────────────┘
```

En este ejemplo, la matriz se ordena de la siguiente manera:

1.  Al principio, la matriz de origen (\[‘hello’, ‘world’\]) se ordena de acuerdo con el resultado de la función lambda aplicada a los elementos de las matrices. Los elementos que se pasan en la segunda matriz (\[2, 1\]), definen las claves de ordenación para los elementos correspondientes de la matriz de origen. El resultado es una matriz \[‘world’, ‘hello’\].
2.  Matriz que se ordenó en el paso anterior, se invierte. Entonces, el resultado final es \[‘hello’, ‘world’\].

Otros ejemplos se muestran a continuación.

``` sql
SELECT arrayReverseSort((x, y) -> y, [4, 3, 5], ['a', 'b', 'c']) AS res;
```

``` text
┌─res─────┐
│ [5,3,4] │
└─────────┘
```

``` sql
SELECT arrayReverseSort((x, y) -> -y, [4, 3, 5], [1, 2, 3]) AS res;
```

``` text
┌─res─────┐
│ [4,3,5] │
└─────────┘
```

## arrayUniq(arr, …) {#arrayuniqarr}

Si se pasa un argumento, cuenta el número de elementos diferentes en la matriz.
Si se pasan varios argumentos, cuenta el número de tuplas diferentes de elementos en las posiciones correspondientes en múltiples matrices.

Si desea obtener una lista de elementos únicos en una matriz, puede usar arrayReduce(‘groupUniqArray’ arr).

## Información adicional) {#array-functions-join}

Una función especial. Vea la sección [“ArrayJoin function”](array-join.md#functions_arrayjoin).

## arrayDifference {#arraydifference}

Calcula la diferencia entre los elementos de matriz adyacentes. Devuelve una matriz donde el primer elemento será 0, el segundo es la diferencia entre `a[1] - a[0]`, etc. The type of elements in the resulting array is determined by the type inference rules for subtraction (e.g. `UInt8` - `UInt8` = `Int16`).

**Sintaxis**

``` sql
arrayDifference(array)
```

**Parámetros**

-   `array` – [Matriz](https://clickhouse.tech/docs/en/data_types/array/).

**Valores devueltos**

Devuelve una matriz de diferencias entre los elementos adyacentes.

Tipo: [UInt\*](https://clickhouse.tech/docs/en/data_types/int_uint/#uint-ranges), [En\*](https://clickhouse.tech/docs/en/data_types/int_uint/#int-ranges), [Flotante\*](https://clickhouse.tech/docs/en/data_types/float/).

**Ejemplo**

Consulta:

``` sql
SELECT arrayDifference([1, 2, 3, 4])
```

Resultado:

``` text
┌─arrayDifference([1, 2, 3, 4])─┐
│ [0,1,1,1]                     │
└───────────────────────────────┘
```

Ejemplo del desbordamiento debido al tipo de resultado Int64:

Consulta:

``` sql
SELECT arrayDifference([0, 10000000000000000000])
```

Resultado:

``` text
┌─arrayDifference([0, 10000000000000000000])─┐
│ [0,-8446744073709551616]                   │
└────────────────────────────────────────────┘
```

## arrayDistinct {#arraydistinct}

Toma una matriz, devuelve una matriz que contiene solo los elementos distintos.

**Sintaxis**

``` sql
arrayDistinct(array)
```

**Parámetros**

-   `array` – [Matriz](https://clickhouse.tech/docs/en/data_types/array/).

**Valores devueltos**

Devuelve una matriz que contiene los elementos distintos.

**Ejemplo**

Consulta:

``` sql
SELECT arrayDistinct([1, 2, 2, 3, 1])
```

Resultado:

``` text
┌─arrayDistinct([1, 2, 2, 3, 1])─┐
│ [1,2,3]                        │
└────────────────────────────────┘
```

## Aquí hay algunas opciones) {#array_functions-arrayenumeratedense}

Devuelve una matriz del mismo tamaño que la matriz de origen, lo que indica dónde aparece cada elemento por primera vez en la matriz de origen.

Ejemplo:

``` sql
SELECT arrayEnumerateDense([10, 20, 10, 30])
```

``` text
┌─arrayEnumerateDense([10, 20, 10, 30])─┐
│ [1,2,1,3]                             │
└───────────────────────────────────────┘
```

## Información detallada) {#array-functions-arrayintersect}

Toma varias matrices, devuelve una matriz con elementos que están presentes en todas las matrices de origen. El orden de los elementos en la matriz resultante es el mismo que en la primera matriz.

Ejemplo:

``` sql
SELECT
    arrayIntersect([1, 2], [1, 3], [2, 3]) AS no_intersect,
    arrayIntersect([1, 2], [1, 3], [1, 4]) AS intersect
```

``` text
┌─no_intersect─┬─intersect─┐
│ []           │ [1]       │
└──────────────┴───────────┘
```

## arrayReduce {#arrayreduce}

Aplica una función de agregado a los elementos de la matriz y devuelve su resultado. El nombre de la función de agregación se pasa como una cadena entre comillas simples `'max'`, `'sum'`. Cuando se utilizan funciones de agregado paramétrico, el parámetro se indica después del nombre de la función entre paréntesis `'uniqUpTo(6)'`.

**Sintaxis**

``` sql
arrayReduce(agg_func, arr1, arr2, ..., arrN)
```

**Parámetros**

-   `agg_func` — The name of an aggregate function which should be a constant [cadena](../../sql-reference/data-types/string.md).
-   `arr` — Any number of [matriz](../../sql-reference/data-types/array.md) escriba columnas como los parámetros de la función de agregación.

**Valor devuelto**

**Ejemplo**

``` sql
SELECT arrayReduce('max', [1, 2, 3])
```

``` text
┌─arrayReduce('max', [1, 2, 3])─┐
│                             3 │
└───────────────────────────────┘
```

Si una función agregada toma varios argumentos, esta función debe aplicarse a varias matrices del mismo tamaño.

``` sql
SELECT arrayReduce('maxIf', [3, 5], [1, 0])
```

``` text
┌─arrayReduce('maxIf', [3, 5], [1, 0])─┐
│                                    3 │
└──────────────────────────────────────┘
```

Ejemplo con una función de agregado paramétrico:

``` sql
SELECT arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
```

``` text
┌─arrayReduce('uniqUpTo(3)', [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])─┐
│                                                           4 │
└─────────────────────────────────────────────────────────────┘
```

## arrayReduceInRanges {#arrayreduceinranges}

Aplica una función de agregado a los elementos de matriz en rangos dados y devuelve una matriz que contiene el resultado correspondiente a cada rango. La función devolverá el mismo resultado que múltiples `arrayReduce(agg_func, arraySlice(arr1, index, length), ...)`.

**Sintaxis**

``` sql
arrayReduceInRanges(agg_func, ranges, arr1, arr2, ..., arrN)
```

**Parámetros**

-   `agg_func` — The name of an aggregate function which should be a constant [cadena](../../sql-reference/data-types/string.md).
-   `ranges` — The ranges to aggretate which should be an [matriz](../../sql-reference/data-types/array.md) de [tuplas](../../sql-reference/data-types/tuple.md) que contiene el índice y la longitud de cada rango.
-   `arr` — Any number of [matriz](../../sql-reference/data-types/array.md) escriba columnas como los parámetros de la función de agregación.

**Valor devuelto**

**Ejemplo**

``` sql
SELECT arrayReduceInRanges(
    'sum',
    [(1, 5), (2, 3), (3, 4), (4, 4)],
    [1000000, 200000, 30000, 4000, 500, 60, 7]
) AS res
```

``` text
┌─res─────────────────────────┐
│ [1234500,234000,34560,4567] │
└─────────────────────────────┘
```

## arrayReverse (arr) {#arrayreverse}

Devuelve una matriz del mismo tamaño que la matriz original que contiene los elementos en orden inverso.

Ejemplo:

``` sql
SELECT arrayReverse([1, 2, 3])
```

``` text
┌─arrayReverse([1, 2, 3])─┐
│ [3,2,1]                 │
└─────────────────────────┘
```

## inversa(arr) {#array-functions-reverse}

Sinónimo de [“arrayReverse”](#arrayreverse)

## arrayFlatten {#arrayflatten}

Convierte una matriz de matrices en una matriz plana.

Función:

-   Se aplica a cualquier profundidad de matrices anidadas.
-   No cambia las matrices que ya son planas.

La matriz aplanada contiene todos los elementos de todas las matrices de origen.

**Sintaxis**

``` sql
flatten(array_of_arrays)
```

Apodo: `flatten`.

**Parámetros**

-   `array_of_arrays` — [Matriz](../../sql-reference/data-types/array.md) de matrices. Por ejemplo, `[[1,2,3], [4,5]]`.

**Ejemplos**

``` sql
SELECT flatten([[[1]], [[2], [3]]])
```

``` text
┌─flatten(array(array([1]), array([2], [3])))─┐
│ [1,2,3]                                     │
└─────────────────────────────────────────────┘
```

## arrayCompact {#arraycompact}

Elimina elementos duplicados consecutivos de una matriz. El orden de los valores de resultado está determinado por el orden de la matriz de origen.

**Sintaxis**

``` sql
arrayCompact(arr)
```

**Parámetros**

`arr` — The [matriz](../../sql-reference/data-types/array.md) inspeccionar.

**Valor devuelto**

La matriz sin duplicado.

Tipo: `Array`.

**Ejemplo**

Consulta:

``` sql
SELECT arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])
```

Resultado:

``` text
┌─arrayCompact([1, 1, nan, nan, 2, 3, 3, 3])─┐
│ [1,nan,nan,2,3]                            │
└────────────────────────────────────────────┘
```

## arrayZip {#arrayzip}

Combina varias matrices en una sola matriz. La matriz resultante contiene los elementos correspondientes de las matrices de origen agrupadas en tuplas en el orden de argumentos enumerado.

**Sintaxis**

``` sql
arrayZip(arr1, arr2, ..., arrN)
```

**Parámetros**

-   `arrN` — [Matriz](../data-types/array.md).

La función puede tomar cualquier cantidad de matrices de diferentes tipos. Todas las matrices de entrada deben ser del mismo tamaño.

**Valor devuelto**

-   Matriz con elementos de las matrices de origen agrupadas en [tuplas](../data-types/tuple.md). Los tipos de datos en la tupla son los mismos que los tipos de las matrices de entrada y en el mismo orden en que se pasan las matrices.

Tipo: [Matriz](../data-types/array.md).

**Ejemplo**

Consulta:

``` sql
SELECT arrayZip(['a', 'b', 'c'], [5, 2, 1])
```

Resultado:

``` text
┌─arrayZip(['a', 'b', 'c'], [5, 2, 1])─┐
│ [('a',5),('b',2),('c',1)]            │
└──────────────────────────────────────┘
```

## arrayAUC {#arrayauc}

Calcule AUC (Área bajo la curva, que es un concepto en el aprendizaje automático, vea más detalles: https://en.wikipedia.org/wiki/Receiver\_operating\_characteristic\#Area\_under\_the\_curve ).

**Sintaxis**

``` sql
arrayAUC(arr_scores, arr_labels)
```

**Parámetros**
- `arr_scores` — scores prediction model gives.
- `arr_labels` — labels of samples, usually 1 for positive sample and 0 for negtive sample.

**Valor devuelto**
Devuelve el valor AUC con el tipo Float64.

**Ejemplo**
Consulta:

``` sql
select arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])
```

Resultado:

``` text
┌─arrayAUC([0.1, 0.4, 0.35, 0.8], [0, 0, 1, 1])─┐
│                                          0.75 │
└────────────────────────────────────────---──┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/array_functions/) <!--hide-->
