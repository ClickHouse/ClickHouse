---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: Referencia
---

# Referencia de función agregada {#aggregate-functions-reference}

## contar {#agg_function-count}

Cuenta el número de filas o valores no NULL.

ClickHouse admite las siguientes sintaxis para `count`:
- `count(expr)` o `COUNT(DISTINCT expr)`.
- `count()` o `COUNT(*)`. El `count()` la sintaxis es específica de ClickHouse.

**Parámetros**

La función puede tomar:

-   Cero parámetros.
-   Una [expresion](../syntax.md#syntax-expressions).

**Valor devuelto**

-   Si se llama a la función sin parámetros, cuenta el número de filas.
-   Si el [expresion](../syntax.md#syntax-expressions) se pasa, entonces la función cuenta cuántas veces esta expresión devuelve no nula. Si la expresión devuelve un [NULL](../../sql-reference/data-types/nullable.md)-type valor, entonces el resultado de `count` no se queda `Nullable`. La función devuelve 0 si la expresión devuelta `NULL` para todas las filas.

En ambos casos el tipo del valor devuelto es [UInt64](../../sql-reference/data-types/int-uint.md).

**Detalles**

ClickHouse soporta el `COUNT(DISTINCT ...)` sintaxis. El comportamiento de esta construcción depende del [count_distinct_implementation](../../operations/settings/settings.md#settings-count_distinct_implementation) configuración. Define cuál de las [uniq\*](#agg_function-uniq) se utiliza para realizar la operación. El valor predeterminado es el [uniqExact](#agg_function-uniqexact) función.

El `SELECT count() FROM table` consulta no está optimizado, porque el número de entradas en la tabla no se almacena por separado. Elige una pequeña columna de la tabla y cuenta el número de valores en ella.

**Ejemplos**

Ejemplo 1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

Ejemplo 2:

``` sql
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation'
```

``` text
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘
```

``` sql
SELECT count(DISTINCT num) FROM t
```

``` text
┌─uniqExact(num)─┐
│              3 │
└────────────────┘
```

Este ejemplo muestra que `count(DISTINCT num)` se realiza por el `uniqExact` función según el `count_distinct_implementation` valor de ajuste.

## cualquiera (x) {#agg_function-any}

Selecciona el primer valor encontrado.
La consulta se puede ejecutar en cualquier orden e incluso en un orden diferente cada vez, por lo que el resultado de esta función es indeterminado.
Para obtener un resultado determinado, puede usar el ‘min’ o ‘max’ función en lugar de ‘any’.

En algunos casos, puede confiar en el orden de ejecución. Esto se aplica a los casos en que SELECT proviene de una subconsulta que usa ORDER BY.

Cuando un `SELECT` consulta tiene el `GROUP BY` cláusula o al menos una función agregada, ClickHouse (en contraste con MySQL) requiere que todas las expresiones `SELECT`, `HAVING`, y `ORDER BY` las cláusulas pueden calcularse a partir de claves o de funciones agregadas. En otras palabras, cada columna seleccionada de la tabla debe usarse en claves o dentro de funciones agregadas. Para obtener un comportamiento como en MySQL, puede colocar las otras columnas en el `any` función de agregado.

## Cualquier pesado (x) {#anyheavyx}

Selecciona un valor que ocurre con frecuencia [pesos pesados](http://www.cs.umd.edu/~samir/498/karp.pdf) algoritmo. Si hay un valor que se produce más de la mitad de los casos en cada uno de los subprocesos de ejecución de la consulta, se devuelve este valor. Normalmente, el resultado es no determinista.

``` sql
anyHeavy(column)
```

**Argumento**

-   `column` – The column name.

**Ejemplo**

Tome el [A tiempo](../../getting-started/example-datasets/ontime.md) conjunto de datos y seleccione cualquier valor que ocurra con frecuencia `AirlineID` columna.

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```

## Cualquier último (x) {#anylastx}

Selecciona el último valor encontrado.
El resultado es tan indeterminado como para el `any` función.

## Método de codificación de datos: {#groupbitand}

Se aplica bit a bit `AND` para la serie de números.

``` sql
groupBitAnd(expr)
```

**Parámetros**

`expr` – An expression that results in `UInt*` tipo.

**Valor de retorno**

Valor de la `UInt*` tipo.

**Ejemplo**

Datos de prueba:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Consulta:

``` sql
SELECT groupBitAnd(num) FROM t
```

Donde `num` es la columna con los datos de prueba.

Resultado:

``` text
binary     decimal
00000100 = 4
```

## GrupoBitO {#groupbitor}

Se aplica bit a bit `OR` para la serie de números.

``` sql
groupBitOr(expr)
```

**Parámetros**

`expr` – An expression that results in `UInt*` tipo.

**Valor de retorno**

Valor de la `UInt*` tipo.

**Ejemplo**

Datos de prueba:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Consulta:

``` sql
SELECT groupBitOr(num) FROM t
```

Donde `num` es la columna con los datos de prueba.

Resultado:

``` text
binary     decimal
01111101 = 125
```

## GrupoBitXor {#groupbitxor}

Se aplica bit a bit `XOR` para la serie de números.

``` sql
groupBitXor(expr)
```

**Parámetros**

`expr` – An expression that results in `UInt*` tipo.

**Valor de retorno**

Valor de la `UInt*` tipo.

**Ejemplo**

Datos de prueba:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

Consulta:

``` sql
SELECT groupBitXor(num) FROM t
```

Donde `num` es la columna con los datos de prueba.

Resultado:

``` text
binary     decimal
01101000 = 104
```

## Método de codificación de datos: {#groupbitmap}

Mapa de bits o cálculos agregados de una columna entera sin signo, devuelve cardinalidad de tipo UInt64, si agrega el sufijo -State, luego devuelve [objeto de mapa de bits](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**Parámetros**

`expr` – An expression that results in `UInt*` tipo.

**Valor de retorno**

Valor de la `UInt64` tipo.

**Ejemplo**

Datos de prueba:

``` text
UserID
1
1
2
3
```

Consulta:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

Resultado:

``` text
num
3
```

## Mínimo (x) {#agg_function-min}

Calcula el mínimo.

## máximo (x) {#agg_function-max}

Calcula el máximo.

## ¿Cómo puedo hacerlo?) {#agg-function-argmin}

Calcula el ‘arg’ para un valor mínimo ‘val’ valor. Si hay varios valores diferentes de ‘arg’ para valores mínimos de ‘val’, el primero de estos valores encontrados es la salida.

**Ejemplo:**

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

``` sql
SELECT argMin(user, salary) FROM salary
```

``` text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```

## Descripción) {#agg-function-argmax}

Calcula el ‘arg’ para un valor máximo ‘val’ valor. Si hay varios valores diferentes de ‘arg’ para valores máximos de ‘val’, el primero de estos valores encontrados es la salida.

## suma (x) {#agg_function-sum}

Calcula la suma.
Solo funciona para números.

## ¿Cómo puedo obtener más información?) {#sumwithoverflowx}

Calcula la suma de los números, utilizando el mismo tipo de datos para el resultado que para los parámetros de entrada. Si la suma supera el valor máximo para este tipo de datos, la función devuelve un error.

Solo funciona para números.

## Por ejemplo, el valor es el siguiente:)) {#agg_functions-summap}

Totals el ‘value’ matriz de acuerdo con las claves especificadas en el ‘key’ matriz.
Pasar una tupla de matrices de claves y valores es sinónimo de pasar dos matrices de claves y valores.
El número de elementos en ‘key’ y ‘value’ debe ser el mismo para cada fila que se sume.
Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

Ejemplo:

``` sql
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    ),
    statusMapTuple Tuple(Array(Int32), Array(Int32))
) ENGINE = Log;
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests),
    sumMap(statusMapTuple)
FROM sum_map
GROUP BY timeslot
```

``` text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┬─sumMap(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │ ([1,2,3,4,5],[10,10,20,10,10]) │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │ ([4,5,6,7,8],[10,10,20,10,10]) │
└─────────────────────┴──────────────────────────────────────────────┴────────────────────────────────┘
```

## SkewPop {#skewpop}

Calcula el [la asimetría](https://en.wikipedia.org/wiki/Skewness) de una secuencia.

``` sql
skewPop(expr)
```

**Parámetros**

`expr` — [Expresion](../syntax.md#syntax-expressions) devolviendo un número.

**Valor devuelto**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**Ejemplo**

``` sql
SELECT skewPop(value) FROM series_with_value_column
```

## Sistema abierto {#skewsamp}

Calcula el [asimetría de la muestra](https://en.wikipedia.org/wiki/Skewness) de una secuencia.

Representa una estimación imparcial de la asimetría de una variable aleatoria si los valores pasados forman su muestra.

``` sql
skewSamp(expr)
```

**Parámetros**

`expr` — [Expresion](../syntax.md#syntax-expressions) devolviendo un número.

**Valor devuelto**

The skewness of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). Si `n <= 1` (`n` es el tamaño de la muestra), luego la función devuelve `nan`.

**Ejemplo**

``` sql
SELECT skewSamp(value) FROM series_with_value_column
```

## KurtPop {#kurtpop}

Calcula el [curtosis](https://en.wikipedia.org/wiki/Kurtosis) de una secuencia.

``` sql
kurtPop(expr)
```

**Parámetros**

`expr` — [Expresion](../syntax.md#syntax-expressions) devolviendo un número.

**Valor devuelto**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md)

**Ejemplo**

``` sql
SELECT kurtPop(value) FROM series_with_value_column
```

## KurtSamp {#kurtsamp}

Calcula el [curtosis muestra](https://en.wikipedia.org/wiki/Kurtosis) de una secuencia.

Representa una estimación imparcial de la curtosis de una variable aleatoria si los valores pasados forman su muestra.

``` sql
kurtSamp(expr)
```

**Parámetros**

`expr` — [Expresion](../syntax.md#syntax-expressions) devolviendo un número.

**Valor devuelto**

The kurtosis of the given distribution. Type — [Float64](../../sql-reference/data-types/float.md). Si `n <= 1` (`n` es un tamaño de la muestra), luego la función devuelve `nan`.

**Ejemplo**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column
```

## Acerca de) {#agg_function-avg}

Calcula el promedio.
Solo funciona para números.
El resultado es siempre Float64.

## avgPonderado {#avgweighted}

Calcula el [media aritmética ponderada](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean).

**Sintaxis**

``` sql
avgWeighted(x, weight)
```

**Parámetros**

-   `x` — Values. [Entero](../data-types/int-uint.md) o [punto flotante](../data-types/float.md).
-   `weight` — Weights of the values. [Entero](../data-types/int-uint.md) o [punto flotante](../data-types/float.md).

Tipo de `x` y `weight` debe ser el mismo.

**Valor devuelto**

-   Media ponderada.
-   `NaN`. Si todos los pesos son iguales a 0.

Tipo: [Float64](../data-types/float.md).

**Ejemplo**

Consulta:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

Resultado:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

## uniq {#agg_function-uniq}

Calcula el número aproximado de diferentes valores del argumento.

``` sql
uniq(x[, ...])
```

**Parámetros**

La función toma un número variable de parámetros. Los parámetros pueden ser `Tuple`, `Array`, `Date`, `DateTime`, `String`, o tipos numéricos.

**Valor devuelto**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-tipo número.

**Detalles de implementación**

Función:

-   Calcula un hash para todos los parámetros en el agregado, luego lo usa en los cálculos.

-   Utiliza un algoritmo de muestreo adaptativo. Para el estado de cálculo, la función utiliza una muestra de valores hash de elemento de hasta 65536.

        This algorithm is very accurate and very efficient on the CPU. When the query contains several of these functions, using `uniq` is almost as fast as using other aggregate functions.

-   Proporciona el resultado de forma determinista (no depende del orden de procesamiento de la consulta).

Recomendamos usar esta función en casi todos los escenarios.

**Ver también**

-   [uniqCombined](#agg_function-uniqcombined)
-   [UniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## uniqCombined {#agg_function-uniqcombined}

Calcula el número aproximado de diferentes valores de argumento.

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

El `uniqCombined` es una buena opción para calcular el número de valores diferentes.

**Parámetros**

La función toma un número variable de parámetros. Los parámetros pueden ser `Tuple`, `Array`, `Date`, `DateTime`, `String`, o tipos numéricos.

`HLL_precision` es el logaritmo base-2 del número de células en [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog). Opcional, puede utilizar la función como `uniqCombined(x[, ...])`. El valor predeterminado para `HLL_precision` es 17, que es efectivamente 96 KiB de espacio (2 ^ 17 celdas, 6 bits cada una).

**Valor devuelto**

-   Numero [UInt64](../../sql-reference/data-types/int-uint.md)-tipo número.

**Detalles de implementación**

Función:

-   Calcula un hash (hash de 64 bits para `String` y 32 bits de lo contrario) para todos los parámetros en el agregado, luego lo usa en los cálculos.

-   Utiliza una combinación de tres algoritmos: matriz, tabla hash e HyperLogLog con una tabla de corrección de errores.

        For a small number of distinct elements, an array is used. When the set size is larger, a hash table is used. For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.

-   Proporciona el resultado de forma determinista (no depende del orden de procesamiento de la consulta).

!!! note "Nota"
    Dado que usa hash de 32 bits para no-`String` tipo, el resultado tendrá un error muy alto para cardinalidades significativamente mayores que `UINT_MAX` (el error aumentará rápidamente después de unas pocas decenas de miles de millones de valores distintos), por lo tanto, en este caso debe usar [UniqCombined64](#agg_function-uniqcombined64)

En comparación con el [uniq](#agg_function-uniq) función, el `uniqCombined`:

-   Consume varias veces menos memoria.
-   Calcula con una precisión varias veces mayor.
-   Por lo general, tiene un rendimiento ligeramente menor. En algunos escenarios, `uniqCombined` puede funcionar mejor que `uniq`, por ejemplo, con consultas distribuidas que transmiten un gran número de estados de agregación a través de la red.

**Ver también**

-   [uniq](#agg_function-uniq)
-   [UniqCombined64](#agg_function-uniqcombined64)
-   [uniqHLL12](#agg_function-uniqhll12)
-   [uniqExact](#agg_function-uniqexact)

## UniqCombined64 {#agg_function-uniqcombined64}

Lo mismo que [uniqCombined](#agg_function-uniqcombined), pero utiliza hash de 64 bits para todos los tipos de datos.

## uniqHLL12 {#agg_function-uniqhll12}

Calcula el número aproximado de diferentes valores de argumento [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) algoritmo.

``` sql
uniqHLL12(x[, ...])
```

**Parámetros**

La función toma un número variable de parámetros. Los parámetros pueden ser `Tuple`, `Array`, `Date`, `DateTime`, `String`, o tipos numéricos.

**Valor devuelto**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)-tipo número.

**Detalles de implementación**

Función:

-   Calcula un hash para todos los parámetros en el agregado, luego lo usa en los cálculos.

-   Utiliza el algoritmo HyperLogLog para aproximar el número de valores de argumento diferentes.

        212 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

-   Proporciona el resultado determinado (no depende del orden de procesamiento de la consulta).

No recomendamos usar esta función. En la mayoría de los casos, use el [uniq](#agg_function-uniq) o [uniqCombined](#agg_function-uniqcombined) función.

**Ver también**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqExact](#agg_function-uniqexact)

## uniqExact {#agg_function-uniqexact}

Calcula el número exacto de diferentes valores de argumento.

``` sql
uniqExact(x[, ...])
```

Utilice el `uniqExact` función si necesita absolutamente un resultado exacto. De lo contrario, use el [uniq](#agg_function-uniq) función.

El `uniqExact` función utiliza más memoria que `uniq`, porque el tamaño del estado tiene un crecimiento ilimitado a medida que aumenta el número de valores diferentes.

**Parámetros**

La función toma un número variable de parámetros. Los parámetros pueden ser `Tuple`, `Array`, `Date`, `DateTime`, `String`, o tipos numéricos.

**Ver también**

-   [uniq](#agg_function-uniq)
-   [uniqCombined](#agg_function-uniqcombined)
-   [uniqHLL12](#agg_function-uniqhll12)

## ¿Cómo puedo hacerlo?) {#agg_function-grouparray}

Crea una matriz de valores de argumento.
Los valores se pueden agregar a la matriz en cualquier orden (indeterminado).

La segunda versión (con el `max_size` parámetro) limita el tamaño de la matriz resultante a `max_size` elemento.
Por ejemplo, `groupArray (1) (x)` es equivalente a `[any (x)]`.

En algunos casos, aún puede confiar en el orden de ejecución. Esto se aplica a los casos en que `SELECT` procede de una subconsulta que utiliza `ORDER BY`.

## GrupoArrayInsertAt {#grouparrayinsertat}

Inserta un valor en la matriz en la posición especificada.

**Sintaxis**

``` sql
groupArrayInsertAt(default_x, size)(x, pos);
```

Si en una consulta se insertan varios valores en la misma posición, la función se comporta de las siguientes maneras:

-   Si se ejecuta una consulta en un solo subproceso, se utiliza el primero de los valores insertados.
-   Si una consulta se ejecuta en varios subprocesos, el valor resultante es uno indeterminado de los valores insertados.

**Parámetros**

-   `x` — Value to be inserted. [Expresion](../syntax.md#syntax-expressions) lo que resulta en uno de los [tipos de datos compatibles](../../sql-reference/data-types/index.md).
-   `pos` — Position at which the specified element `x` se va a insertar. La numeración de índices en la matriz comienza desde cero. [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).
-   `default_x`— Default value for substituting in empty positions. Optional parameter. [Expresion](../syntax.md#syntax-expressions) dando como resultado el tipo de datos configurado para `x` parámetro. Si `default_x` no está definido, el [valores predeterminados](../../sql-reference/statements/create.md#create-default-values) se utilizan.
-   `size`— Length of the resulting array. Optional parameter. When using this parameter, the default value `default_x` debe ser especificado. [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Valor devuelto**

-   Matriz con valores insertados.

Tipo: [Matriz](../../sql-reference/data-types/array.md#data-type-array).

**Ejemplo**

Consulta:

``` sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

Resultado:

``` text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐
│ ['0','','1','','2','','3','','4']                         │
└───────────────────────────────────────────────────────────┘
```

Consulta:

``` sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

Resultado:

``` text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

Consulta:

``` sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

Resultado:

``` text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

Inserción multihilo de elementos en una posición.

Consulta:

``` sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

Como resultado de esta consulta, obtiene un entero aleatorio en el `[0,9]` gama. Por ejemplo:

``` text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
```

## groupArrayMovingSum {#agg_function-grouparraymovingsum}

Calcula la suma móvil de los valores de entrada.

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

La función puede tomar el tamaño de la ventana como un parámetro. Si no se especifica, la función toma el tamaño de ventana igual al número de filas de la columna.

**Parámetros**

-   `numbers_for_summing` — [Expresion](../syntax.md#syntax-expressions) dando como resultado un valor de tipo de datos numérico.
-   `window_size` — Size of the calculation window.

**Valores devueltos**

-   Matriz del mismo tamaño y tipo que los datos de entrada.

**Ejemplo**

La tabla de ejemplo:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

Consulta:

``` sql
SELECT
    groupArrayMovingSum(int) AS I,
    groupArrayMovingSum(float) AS F,
    groupArrayMovingSum(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,7,14] │ [1.1,3.3000002,7.7000003,15.47] │ [1.10,3.30,7.70,15.47] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

``` sql
SELECT
    groupArrayMovingSum(2)(int) AS I,
    groupArrayMovingSum(2)(float) AS F,
    groupArrayMovingSum(2)(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,6,11] │ [1.1,3.3000002,6.6000004,12.17] │ [1.10,3.30,6.60,12.17] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

## Método de codificación de datos: {#agg_function-grouparraymovingavg}

Calcula la media móvil de los valores de entrada.

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

La función puede tomar el tamaño de la ventana como un parámetro. Si no se especifica, la función toma el tamaño de ventana igual al número de filas de la columna.

**Parámetros**

-   `numbers_for_summing` — [Expresion](../syntax.md#syntax-expressions) dando como resultado un valor de tipo de datos numérico.
-   `window_size` — Size of the calculation window.

**Valores devueltos**

-   Matriz del mismo tamaño y tipo que los datos de entrada.

La función utiliza [redondeando hacia cero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). Trunca los decimales insignificantes para el tipo de datos resultante.

**Ejemplo**

La tabla de ejemplo `b`:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

Consulta:

``` sql
SELECT
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F───────────────────────────────────┬─D─────────────────────┐
│ [0,0,1,3] │ [0.275,0.82500005,1.9250001,3.8675] │ [0.27,0.82,1.92,3.86] │
└───────────┴─────────────────────────────────────┴───────────────────────┘
```

``` sql
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F────────────────────────────────┬─D─────────────────────┐
│ [0,1,3,5] │ [0.55,1.6500001,3.3000002,6.085] │ [0.55,1.65,3.30,6.08] │
└───────────┴──────────────────────────────────┴───────────────────────┘
```

## ¿Cómo puedo obtener más información?) {#groupuniqarrayx-groupuniqarraymax-sizex}

Crea una matriz a partir de diferentes valores de argumento. El consumo de memoria es el mismo que para el `uniqExact` función.

La segunda versión (con el `max_size` parámetro) limita el tamaño de la matriz resultante a `max_size` elemento.
Por ejemplo, `groupUniqArray(1)(x)` es equivalente a `[any(x)]`.

## cuantil {#quantile}

Calcula un aproximado [cuantil](https://en.wikipedia.org/wiki/Quantile) de una secuencia de datos numéricos.

Esta función se aplica [muestreo de embalses](https://en.wikipedia.org/wiki/Reservoir_sampling) con un tamaño de depósito de hasta 8192 y un generador de números aleatorios para el muestreo. El resultado es no determinista. Para obtener un cuantil exacto, use el [quantileExact](#quantileexact) función.

Cuando se utilizan múltiples `quantile*` funciones con diferentes niveles en una consulta, los estados internos no se combinan (es decir, la consulta funciona de manera menos eficiente de lo que podría). En este caso, use el [cantiles](#quantiles) función.

**Sintaxis**

``` sql
quantile(level)(expr)
```

Apodo: `median`.

**Parámetros**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` valor en el rango de `[0.01, 0.99]`. Valor predeterminado: 0.5. En `level=0.5` la función calcula [mediana](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [tipos de datos](../../sql-reference/data-types/index.md#data_types), [Fecha](../../sql-reference/data-types/date.md) o [FechaHora](../../sql-reference/data-types/datetime.md).

**Valor devuelto**

-   Cuantil aproximado del nivel especificado.

Tipo:

-   [Float64](../../sql-reference/data-types/float.md) para la entrada de tipo de datos numéricos.
-   [Fecha](../../sql-reference/data-types/date.md) si los valores de entrada tienen `Date` tipo.
-   [FechaHora](../../sql-reference/data-types/datetime.md) si los valores de entrada tienen `DateTime` tipo.

**Ejemplo**

Tabla de entrada:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Consulta:

``` sql
SELECT quantile(val) FROM t
```

Resultado:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**Ver también**

-   [mediana](#median)
-   [cantiles](#quantiles)

## quantileDeterminista {#quantiledeterministic}

Calcula un aproximado [cuantil](https://en.wikipedia.org/wiki/Quantile) de una secuencia de datos numéricos.

Esta función se aplica [muestreo de embalses](https://en.wikipedia.org/wiki/Reservoir_sampling) con un tamaño de depósito de hasta 8192 y algoritmo determinista de muestreo. El resultado es determinista. Para obtener un cuantil exacto, use el [quantileExact](#quantileexact) función.

Cuando se utilizan múltiples `quantile*` funciones con diferentes niveles en una consulta, los estados internos no se combinan (es decir, la consulta funciona de manera menos eficiente de lo que podría). En este caso, use el [cantiles](#quantiles) función.

**Sintaxis**

``` sql
quantileDeterministic(level)(expr, determinator)
```

Apodo: `medianDeterministic`.

**Parámetros**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` valor en el rango de `[0.01, 0.99]`. Valor predeterminado: 0.5. En `level=0.5` la función calcula [mediana](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [tipos de datos](../../sql-reference/data-types/index.md#data_types), [Fecha](../../sql-reference/data-types/date.md) o [FechaHora](../../sql-reference/data-types/datetime.md).
-   `determinator` — Number whose hash is used instead of a random number generator in the reservoir sampling algorithm to make the result of sampling deterministic. As a determinator you can use any deterministic positive number, for example, a user id or an event id. If the same determinator value occures too often, the function works incorrectly.

**Valor devuelto**

-   Cuantil aproximado del nivel especificado.

Tipo:

-   [Float64](../../sql-reference/data-types/float.md) para la entrada de tipo de datos numéricos.
-   [Fecha](../../sql-reference/data-types/date.md) si los valores de entrada tienen `Date` tipo.
-   [FechaHora](../../sql-reference/data-types/datetime.md) si los valores de entrada tienen `DateTime` tipo.

**Ejemplo**

Tabla de entrada:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Consulta:

``` sql
SELECT quantileDeterministic(val, 1) FROM t
```

Resultado:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**Ver también**

-   [mediana](#median)
-   [cantiles](#quantiles)

## quantileExact {#quantileexact}

Calcula exactamente el [cuantil](https://en.wikipedia.org/wiki/Quantile) de una secuencia de datos numéricos.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` memoria, donde `n` es un número de valores que se pasaron. Sin embargo, para un pequeño número de valores, la función es muy efectiva.

Cuando se utilizan múltiples `quantile*` funciones con diferentes niveles en una consulta, los estados internos no se combinan (es decir, la consulta funciona de manera menos eficiente de lo que podría). En este caso, use el [cantiles](#quantiles) función.

**Sintaxis**

``` sql
quantileExact(level)(expr)
```

Apodo: `medianExact`.

**Parámetros**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` valor en el rango de `[0.01, 0.99]`. Valor predeterminado: 0.5. En `level=0.5` la función calcula [mediana](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [tipos de datos](../../sql-reference/data-types/index.md#data_types), [Fecha](../../sql-reference/data-types/date.md) o [FechaHora](../../sql-reference/data-types/datetime.md).

**Valor devuelto**

-   Cuantil del nivel especificado.

Tipo:

-   [Float64](../../sql-reference/data-types/float.md) para la entrada de tipo de datos numéricos.
-   [Fecha](../../sql-reference/data-types/date.md) si los valores de entrada tienen `Date` tipo.
-   [FechaHora](../../sql-reference/data-types/datetime.md) si los valores de entrada tienen `DateTime` tipo.

**Ejemplo**

Consulta:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

Resultado:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

**Ver también**

-   [mediana](#median)
-   [cantiles](#quantiles)

## quantileExactWeighted {#quantileexactweighted}

Calcula exactamente el [cuantil](https://en.wikipedia.org/wiki/Quantile) de una secuencia de datos numéricos, teniendo en cuenta el peso de cada elemento.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Each value is counted with its weight, as if it is present `weight` times. A hash table is used in the algorithm. Because of this, if the passed values ​​are frequently repeated, the function consumes less RAM than [quantileExact](#quantileexact). Puede usar esta función en lugar de `quantileExact` y especifique el peso 1.

Cuando se utilizan múltiples `quantile*` funciones con diferentes niveles en una consulta, los estados internos no se combinan (es decir, la consulta funciona de manera menos eficiente de lo que podría). En este caso, use el [cantiles](#quantiles) función.

**Sintaxis**

``` sql
quantileExactWeighted(level)(expr, weight)
```

Apodo: `medianExactWeighted`.

**Parámetros**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` valor en el rango de `[0.01, 0.99]`. Valor predeterminado: 0.5. En `level=0.5` la función calcula [mediana](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [tipos de datos](../../sql-reference/data-types/index.md#data_types), [Fecha](../../sql-reference/data-types/date.md) o [FechaHora](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence members. Weight is a number of value occurrences.

**Valor devuelto**

-   Cuantil del nivel especificado.

Tipo:

-   [Float64](../../sql-reference/data-types/float.md) para la entrada de tipo de datos numéricos.
-   [Fecha](../../sql-reference/data-types/date.md) si los valores de entrada tienen `Date` tipo.
-   [FechaHora](../../sql-reference/data-types/datetime.md) si los valores de entrada tienen `DateTime` tipo.

**Ejemplo**

Tabla de entrada:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

Consulta:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

Resultado:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**Ver también**

-   [mediana](#median)
-   [cantiles](#quantiles)

## quantileTiming {#quantiletiming}

Con la precisión determinada calcula el [cuantil](https://en.wikipedia.org/wiki/Quantile) de una secuencia de datos numéricos.

El resultado es determinista (no depende del orden de procesamiento de la consulta). La función está optimizada para trabajar con secuencias que describen distribuciones como tiempos de carga de páginas web o tiempos de respuesta de back-end.

Cuando se utilizan múltiples `quantile*` funciones con diferentes niveles en una consulta, los estados internos no se combinan (es decir, la consulta funciona de manera menos eficiente de lo que podría). En este caso, use el [cantiles](#quantiles) función.

**Sintaxis**

``` sql
quantileTiming(level)(expr)
```

Apodo: `medianTiming`.

**Parámetros**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` valor en el rango de `[0.01, 0.99]`. Valor predeterminado: 0.5. En `level=0.5` la función calcula [mediana](https://en.wikipedia.org/wiki/Median).

-   `expr` — [Expresion](../syntax.md#syntax-expressions) sobre una columna valores que devuelven un [Flotante\*](../../sql-reference/data-types/float.md)-tipo número.

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

**Exactitud**

El cálculo es preciso si:

-   El número total de valores no supera los 5670.
-   El número total de valores supera los 5670, pero el tiempo de carga de la página es inferior a 1024 ms.

De lo contrario, el resultado del cálculo se redondea al múltiplo más cercano de 16 ms.

!!! note "Nota"
    Para calcular los cuantiles de tiempo de carga de la página, esta función es más efectiva y precisa que [cuantil](#quantile).

**Valor devuelto**

-   Cuantil del nivel especificado.

Tipo: `Float32`.

!!! note "Nota"
    Si no se pasan valores a la función (cuando se `quantileTimingIf`), [NaN](../../sql-reference/data-types/float.md#data_type-float-nan-inf) se devuelve. El propósito de esto es diferenciar estos casos de los casos que resultan en cero. Ver [ORDER BY cláusula](../statements/select/order-by.md#select-order-by) para notas sobre la clasificación `NaN` valor.

**Ejemplo**

Tabla de entrada:

``` text
┌─response_time─┐
│            72 │
│           112 │
│           126 │
│           145 │
│           104 │
│           242 │
│           313 │
│           168 │
│           108 │
└───────────────┘
```

Consulta:

``` sql
SELECT quantileTiming(response_time) FROM t
```

Resultado:

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**Ver también**

-   [mediana](#median)
-   [cantiles](#quantiles)

## quantileTimingWeighted {#quantiletimingweighted}

Con la precisión determinada calcula el [cuantil](https://en.wikipedia.org/wiki/Quantile) de una secuencia de datos numéricos según el peso de cada miembro de secuencia.

El resultado es determinista (no depende del orden de procesamiento de la consulta). La función está optimizada para trabajar con secuencias que describen distribuciones como tiempos de carga de páginas web o tiempos de respuesta de back-end.

Cuando se utilizan múltiples `quantile*` funciones con diferentes niveles en una consulta, los estados internos no se combinan (es decir, la consulta funciona de manera menos eficiente de lo que podría). En este caso, use el [cantiles](#quantiles) función.

**Sintaxis**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

Apodo: `medianTimingWeighted`.

**Parámetros**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` valor en el rango de `[0.01, 0.99]`. Valor predeterminado: 0.5. En `level=0.5` la función calcula [mediana](https://en.wikipedia.org/wiki/Median).

-   `expr` — [Expresion](../syntax.md#syntax-expressions) sobre una columna valores que devuelven un [Flotante\*](../../sql-reference/data-types/float.md)-tipo número.

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**Exactitud**

El cálculo es preciso si:

-   El número total de valores no supera los 5670.
-   El número total de valores supera los 5670, pero el tiempo de carga de la página es inferior a 1024 ms.

De lo contrario, el resultado del cálculo se redondea al múltiplo más cercano de 16 ms.

!!! note "Nota"
    Para calcular los cuantiles de tiempo de carga de la página, esta función es más efectiva y precisa que [cuantil](#quantile).

**Valor devuelto**

-   Cuantil del nivel especificado.

Tipo: `Float32`.

!!! note "Nota"
    Si no se pasan valores a la función (cuando se `quantileTimingIf`), [NaN](../../sql-reference/data-types/float.md#data_type-float-nan-inf) se devuelve. El propósito de esto es diferenciar estos casos de los casos que resultan en cero. Ver [ORDER BY cláusula](../statements/select/order-by.md#select-order-by) para notas sobre la clasificación `NaN` valor.

**Ejemplo**

Tabla de entrada:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

Consulta:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

Resultado:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

**Ver también**

-   [mediana](#median)
-   [cantiles](#quantiles)

## quantileTDigest {#quantiletdigest}

Calcula un aproximado [cuantil](https://en.wikipedia.org/wiki/Quantile) de una secuencia de datos numéricos usando el [T-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algoritmo.

El error máximo es 1%. El consumo de memoria es `log(n)`, donde `n` es un número de valores. El resultado depende del orden de ejecución de la consulta y no es determinista.

El rendimiento de la función es menor que el rendimiento de [cuantil](#quantile) o [quantileTiming](#quantiletiming). En términos de la relación entre el tamaño del estado y la precisión, esta función es mucho mejor que `quantile`.

Cuando se utilizan múltiples `quantile*` funciones con diferentes niveles en una consulta, los estados internos no se combinan (es decir, la consulta funciona de manera menos eficiente de lo que podría). En este caso, use el [cantiles](#quantiles) función.

**Sintaxis**

``` sql
quantileTDigest(level)(expr)
```

Apodo: `medianTDigest`.

**Parámetros**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` valor en el rango de `[0.01, 0.99]`. Valor predeterminado: 0.5. En `level=0.5` la función calcula [mediana](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [tipos de datos](../../sql-reference/data-types/index.md#data_types), [Fecha](../../sql-reference/data-types/date.md) o [FechaHora](../../sql-reference/data-types/datetime.md).

**Valor devuelto**

-   Cuantil aproximado del nivel especificado.

Tipo:

-   [Float64](../../sql-reference/data-types/float.md) para la entrada de tipo de datos numéricos.
-   [Fecha](../../sql-reference/data-types/date.md) si los valores de entrada tienen `Date` tipo.
-   [FechaHora](../../sql-reference/data-types/datetime.md) si los valores de entrada tienen `DateTime` tipo.

**Ejemplo**

Consulta:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

Resultado:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**Ver también**

-   [mediana](#median)
-   [cantiles](#quantiles)

## quantileTDigestWeighted {#quantiletdigestweighted}

Calcula un aproximado [cuantil](https://en.wikipedia.org/wiki/Quantile) de una secuencia de datos numéricos usando el [T-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) algoritmo. La función tiene en cuenta el peso de cada miembro de secuencia. El error máximo es 1%. El consumo de memoria es `log(n)`, donde `n` es un número de valores.

El rendimiento de la función es menor que el rendimiento de [cuantil](#quantile) o [quantileTiming](#quantiletiming). En términos de la relación entre el tamaño del estado y la precisión, esta función es mucho mejor que `quantile`.

El resultado depende del orden de ejecución de la consulta y no es determinista.

Cuando se utilizan múltiples `quantile*` funciones con diferentes niveles en una consulta, los estados internos no se combinan (es decir, la consulta funciona de manera menos eficiente de lo que podría). En este caso, use el [cantiles](#quantiles) función.

**Sintaxis**

``` sql
quantileTDigest(level)(expr)
```

Apodo: `medianTDigest`.

**Parámetros**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` valor en el rango de `[0.01, 0.99]`. Valor predeterminado: 0.5. En `level=0.5` la función calcula [mediana](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [tipos de datos](../../sql-reference/data-types/index.md#data_types), [Fecha](../../sql-reference/data-types/date.md) o [FechaHora](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**Valor devuelto**

-   Cuantil aproximado del nivel especificado.

Tipo:

-   [Float64](../../sql-reference/data-types/float.md) para la entrada de tipo de datos numéricos.
-   [Fecha](../../sql-reference/data-types/date.md) si los valores de entrada tienen `Date` tipo.
-   [FechaHora](../../sql-reference/data-types/datetime.md) si los valores de entrada tienen `DateTime` tipo.

**Ejemplo**

Consulta:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

Resultado:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**Ver también**

-   [mediana](#median)
-   [cantiles](#quantiles)

## mediana {#median}

El `median*` funciones son los alias para el `quantile*` función. Calculan la mediana de una muestra de datos numéricos.

Función:

-   `median` — Alias for [cuantil](#quantile).
-   `medianDeterministic` — Alias for [quantileDeterminista](#quantiledeterministic).
-   `medianExact` — Alias for [quantileExact](#quantileexact).
-   `medianExactWeighted` — Alias for [quantileExactWeighted](#quantileexactweighted).
-   `medianTiming` — Alias for [quantileTiming](#quantiletiming).
-   `medianTimingWeighted` — Alias for [quantileTimingWeighted](#quantiletimingweighted).
-   `medianTDigest` — Alias for [quantileTDigest](#quantiletdigest).
-   `medianTDigestWeighted` — Alias for [quantileTDigestWeighted](#quantiletdigestweighted).

**Ejemplo**

Tabla de entrada:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

Consulta:

``` sql
SELECT medianDeterministic(val, 1) FROM t
```

Resultado:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```

## quantiles(level1, level2, …)(x) {#quantiles}

Todas las funciones de cuantiles también tienen funciones de cuantiles correspondientes: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`. Estas funciones calculan todos los cuantiles de los niveles enumerados en una sola pasada y devuelven una matriz de los valores resultantes.

## Acerca de Nosotros) {#varsampx}

Calcula la cantidad `Σ((x - x̅)^2) / (n - 1)`, donde `n` es el tamaño de la muestra y `x̅`es el valor promedio de `x`.

Representa una estimación imparcial de la varianza de una variable aleatoria si los valores pasados forman su muestra.

Devoluciones `Float64`. Cuando `n <= 1`, devoluciones `+∞`.

!!! note "Nota"
    Esta función utiliza un algoritmo numéricamente inestable. Si necesita [estabilidad numérica](https://en.wikipedia.org/wiki/Numerical_stability) en los cálculos, utilice el `varSampStable` función. Funciona más lento, pero proporciona un menor error computacional.

## Nombre de la red inalámbrica (SSID):) {#varpopx}

Calcula la cantidad `Σ((x - x̅)^2) / n`, donde `n` es el tamaño de la muestra y `x̅`es el valor promedio de `x`.

En otras palabras, dispersión para un conjunto de valores. Devoluciones `Float64`.

!!! note "Nota"
    Esta función utiliza un algoritmo numéricamente inestable. Si necesita [estabilidad numérica](https://en.wikipedia.org/wiki/Numerical_stability) en los cálculos, utilice el `varPopStable` función. Funciona más lento, pero proporciona un menor error computacional.

## Soporte técnico) {#stddevsampx}

El resultado es igual a la raíz cuadrada de `varSamp(x)`.

!!! note "Nota"
    Esta función utiliza un algoritmo numéricamente inestable. Si necesita [estabilidad numérica](https://en.wikipedia.org/wiki/Numerical_stability) en los cálculos, utilice el `stddevSampStable` función. Funciona más lento, pero proporciona un menor error computacional.

## stddevPop(x) {#stddevpopx}

El resultado es igual a la raíz cuadrada de `varPop(x)`.

!!! note "Nota"
    Esta función utiliza un algoritmo numéricamente inestable. Si necesita [estabilidad numérica](https://en.wikipedia.org/wiki/Numerical_stability) en los cálculos, utilice el `stddevPopStable` función. Funciona más lento, pero proporciona un menor error computacional.

## topK(N)(x) {#topknx}

Devuelve una matriz de los valores aproximadamente más frecuentes de la columna especificada. La matriz resultante se ordena en orden descendente de frecuencia aproximada de valores (no por los valores mismos).

Implementa el [Ahorro de espacio filtrado](http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf) algoritmo para analizar TopK, basado en el algoritmo de reducción y combinación de [Ahorro de espacio paralelo](https://arxiv.org/pdf/1401.0702.pdf).

``` sql
topK(N)(column)
```

Esta función no proporciona un resultado garantizado. En ciertas situaciones, pueden producirse errores y pueden devolver valores frecuentes que no son los valores más frecuentes.

Recomendamos usar el `N < 10` valor; el rendimiento se reduce con grandes `N` valor. Valor máximo de `N = 65536`.

**Parámetros**

-   ‘N’ es el número de elementos a devolver.

Si se omite el parámetro, se utiliza el valor predeterminado 10.

**Argumento**

-   ' x ' – The value to calculate frequency.

**Ejemplo**

Tome el [A tiempo](../../getting-started/example-datasets/ontime.md) conjunto de datos y seleccione los tres valores más frecuentes `AirlineID` columna.

``` sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```

``` text
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```

## topKPeso {#topkweighted}

Similar a `topK` pero toma un argumento adicional de tipo entero - `weight`. Cada valor se contabiliza `weight` veces para el cálculo de la frecuencia.

**Sintaxis**

``` sql
topKWeighted(N)(x, weight)
```

**Parámetros**

-   `N` — The number of elements to return.

**Argumento**

-   `x` – The value.
-   `weight` — The weight. [UInt8](../../sql-reference/data-types/int-uint.md).

**Valor devuelto**

Devuelve una matriz de los valores con la suma aproximada máxima de pesos.

**Ejemplo**

Consulta:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

Resultado:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```

## covarSamp(x, y) {#covarsampx-y}

Calcula el valor de `Σ((x - x̅)(y - y̅)) / (n - 1)`.

Devuelve Float64. Cuando `n <= 1`, returns +∞.

!!! note "Nota"
    Esta función utiliza un algoritmo numéricamente inestable. Si necesita [estabilidad numérica](https://en.wikipedia.org/wiki/Numerical_stability) en los cálculos, utilice el `covarSampStable` función. Funciona más lento, pero proporciona un menor error computacional.

## covarPop(x, y) {#covarpopx-y}

Calcula el valor de `Σ((x - x̅)(y - y̅)) / n`.

!!! note "Nota"
    Esta función utiliza un algoritmo numéricamente inestable. Si necesita [estabilidad numérica](https://en.wikipedia.org/wiki/Numerical_stability) en los cálculos, utilice el `covarPopStable` función. Funciona más lento pero proporciona un menor error computacional.

## corr(x, y) {#corrx-y}

Calcula el coeficiente de correlación de Pearson: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`.

!!! note "Nota"
    Esta función utiliza un algoritmo numéricamente inestable. Si necesita [estabilidad numérica](https://en.wikipedia.org/wiki/Numerical_stability) en los cálculos, utilice el `corrStable` función. Funciona más lento, pero proporciona un menor error computacional.

## categoricalInformationValue {#categoricalinformationvalue}

Calcula el valor de `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` para cada categoría.

``` sql
categoricalInformationValue(category1, category2, ..., tag)
```

El resultado indica cómo una característica discreta (categórica `[category1, category2, ...]` contribuir a un modelo de aprendizaje que predice el valor de `tag`.

## SimpleLinearRegression {#simplelinearregression}

Realiza una regresión lineal simple (unidimensional).

``` sql
simpleLinearRegression(x, y)
```

Parámetros:

-   `x` — Column with dependent variable values.
-   `y` — Column with explanatory variable values.

Valores devueltos:

Constante `(a, b)` de la línea resultante `y = a*x + b`.

**Ejemplos**

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])─┐
│ (1,0)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])─┐
│ (1,3)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

## stochasticLinearRegression {#agg_functions-stochasticlinearregression}

Esta función implementa la regresión lineal estocástica. Admite parámetros personalizados para la tasa de aprendizaje, el coeficiente de regularización L2, el tamaño de mini lote y tiene pocos métodos para actualizar los pesos ([Adán](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) (utilizado por defecto), [SGD simple](https://en.wikipedia.org/wiki/Stochastic_gradient_descent), [Impulso](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum), [Nesterov](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)).

### Parámetros {#agg_functions-stochasticlinearregression-parameters}

Hay 4 parámetros personalizables. Se pasan a la función secuencialmente, pero no es necesario pasar los cuatro; se usarán valores predeterminados, sin embargo, un buen modelo requirió algún ajuste de parámetros.

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` es el coeficiente en la longitud del paso, cuando se realiza el paso de descenso de gradiente. Una tasa de aprendizaje demasiado grande puede causar pesos infinitos del modelo. El valor predeterminado es `0.00001`.
2.  `l2 regularization coefficient` que puede ayudar a prevenir el sobreajuste. El valor predeterminado es `0.1`.
3.  `mini-batch size` establece el número de elementos, cuyos gradientes se calcularán y sumarán para realizar un paso de descenso de gradiente. El descenso estocástico puro usa un elemento, sin embargo, tener lotes pequeños (aproximadamente 10 elementos) hace que los pasos de gradiente sean más estables. El valor predeterminado es `15`.
4.  `method for updating weights`, son: `Adam` (predeterminada), `SGD`, `Momentum`, `Nesterov`. `Momentum` y `Nesterov` requieren un poco más de cálculos y memoria, sin embargo, resultan útiles en términos de velocidad de convergencia y estabilidad de los métodos de gradiente estocásticos.

### Uso {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` se utiliza en dos pasos: ajustar el modelo y predecir nuevos datos. Para ajustar el modelo y guardar su estado para su uso posterior, utilizamos `-State` combinador, que básicamente guarda el estado (pesos del modelo, etc.).
Para predecir usamos la función [evalMLMethod](../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod), que toma un estado como argumento, así como características para predecir.

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** Accesorio

Dicha consulta puede ser utilizada.

``` sql
CREATE TABLE IF NOT EXISTS train_data
(
    param1 Float64,
    param2 Float64,
    target Float64
) ENGINE = Memory;

CREATE TABLE your_model ENGINE = Memory AS SELECT
stochasticLinearRegressionState(0.1, 0.0, 5, 'SGD')(target, param1, param2)
AS state FROM train_data;
```

Aquí también tenemos que insertar datos en `train_data` tabla. El número de parámetros no es fijo, depende solo del número de argumentos, pasados a `linearRegressionState`. Todos deben ser valores numéricos.
Tenga en cuenta que la columna con valor objetivo (que nos gustaría aprender a predecir) se inserta como primer argumento.

**2.** Predecir

Después de guardar un estado en la tabla, podemos usarlo varias veces para la predicción, o incluso fusionarlo con otros estados y crear nuevos modelos aún mejores.

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

La consulta devolverá una columna de valores predichos. Tenga en cuenta que el primer argumento de `evalMLMethod` ser `AggregateFunctionState` objeto, siguiente son columnas de características.

`test_data` es una mesa como `train_data` pero puede no contener el valor objetivo.

### Nota {#agg_functions-stochasticlinearregression-notes}

1.  Para fusionar dos modelos, el usuario puede crear dicha consulta:
    `sql  SELECT state1 + state2 FROM your_models`
    donde `your_models` la tabla contiene ambos modelos. Esta consulta devolverá un nuevo `AggregateFunctionState` objeto.

2.  El usuario puede obtener pesos del modelo creado para sus propios fines sin guardar el modelo si no `-State` combinador se utiliza.
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    Dicha consulta se ajustará al modelo y devolverá sus pesos: primero son los pesos, que corresponden a los parámetros del modelo, el último es el sesgo. Entonces, en el ejemplo anterior, la consulta devolverá una columna con 3 valores.

**Ver también**

-   [stochasticLogisticRegression](#agg_functions-stochasticlogisticregression)
-   [Diferencia entre regresiones lineales y logísticas](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## stochasticLogisticRegression {#agg_functions-stochasticlogisticregression}

Esta función implementa la regresión logística estocástica. Se puede usar para problemas de clasificación binaria, admite los mismos parámetros personalizados que stochasticLinearRegression y funciona de la misma manera.

### Parámetros {#agg_functions-stochasticlogisticregression-parameters}

Los parámetros son exactamente los mismos que en stochasticLinearRegression:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
Para obtener más información, consulte [parámetros](#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

1.  Accesorio

<!-- -->

    See the `Fitting` section in the [stochasticLinearRegression](#stochasticlinearregression-usage-fitting) description.

    Predicted labels have to be in \[-1, 1\].

1.  Predecir

<!-- -->

    Using saved state we can predict probability of object having label `1`.

    ``` sql
    WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) FROM test_data
    ```

    The query will return a column of probabilities. Note that first argument of `evalMLMethod` is `AggregateFunctionState` object, next are columns of features.

    We can also set a bound of probability, which assigns elements to different labels.

    ``` sql
    SELECT ans < 1.1 AND ans > 0.5 FROM
    (WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) AS ans FROM test_data)
    ```

    Then the result will be labels.

    `test_data` is a table like `train_data` but may not contain target value.

**Ver también**

-   [stochasticLinearRegression](#agg_functions-stochasticlinearregression)
-   [Diferencia entre regresiones lineales y logísticas.](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## Método de codificación de datos: {#groupbitmapand}

Calcula el AND de una columna de mapa de bits, devuelve la cardinalidad del tipo UInt64, si agrega el sufijo -State, luego devuelve [objeto de mapa de bits](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapAnd(expr)
```

**Parámetros**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` tipo.

**Valor de retorno**

Valor de la `UInt64` tipo.

**Ejemplo**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapAnd(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapAnd(z)─┐
│               3   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapAndState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapAndState(z)))─┐
│ [6,8,10]                                         │
└──────────────────────────────────────────────────┘
```

## Método de codificación de datos: {#groupbitmapor}

Calcula el OR de una columna de mapa de bits, devuelve la cardinalidad del tipo UInt64, si agrega el sufijo -State, luego devuelve [objeto de mapa de bits](../../sql-reference/functions/bitmap-functions.md). Esto es equivalente a `groupBitmapMerge`.

``` sql
groupBitmapOr(expr)
```

**Parámetros**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` tipo.

**Valor de retorno**

Valor de la `UInt64` tipo.

**Ejemplo**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapOr(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapOr(z)─┐
│             15   │
└──────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapOrState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapOrState(z)))─┐
│ [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]           │
└─────────────────────────────────────────────────┘
```

## Método de codificación de datos: {#groupbitmapxor}

Calcula el XOR de una columna de mapa de bits, devuelve la cardinalidad del tipo UInt64, si agrega el sufijo -State, luego devuelve [objeto de mapa de bits](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapOr(expr)
```

**Parámetros**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` tipo.

**Valor de retorno**

Valor de la `UInt64` tipo.

**Ejemplo**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapXor(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapXor(z)─┐
│              10   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapXorState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapXorState(z)))─┐
│ [1,3,5,6,8,10,11,13,14,15]                       │
└──────────────────────────────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/agg_functions/reference/) <!--hide-->
