---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "Param\xE9trico"
---

# Funciones agregadas paramétricas {#aggregate_functions_parametric}

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## histograma {#histogram}

Calcula un histograma adaptativo. No garantiza resultados precisos.

``` sql
histogram(number_of_bins)(values)
```

Las funciones utiliza [Un algoritmo de árbol de decisión paralelo de transmisión](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). Los bordes de los contenedores de histograma se ajustan a medida que los nuevos datos entran en una función. En caso común, los anchos de los contenedores no son iguales.

**Parámetros**

`number_of_bins` — Upper limit for the number of bins in the histogram. The function automatically calculates the number of bins. It tries to reach the specified number of bins, but if it fails, it uses fewer bins.
`values` — [Expresion](../syntax.md#syntax-expressions) resultando en valores de entrada.

**Valores devueltos**

-   [Matriz](../../sql-reference/data-types/array.md) de [Tuples](../../sql-reference/data-types/tuple.md) del siguiente formato:

        ```
        [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
        ```

        - `lower` — Lower bound of the bin.
        - `upper` — Upper bound of the bin.
        - `height` — Calculated height of the bin.

**Ejemplo**

``` sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

Puede visualizar un histograma con el [Bar](../../sql-reference/functions/other-functions.md#function-bar) función, por ejemplo:

``` sql
WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height,
    bar(height, 0, 6, 5) AS bar
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

En este caso, debe recordar que no conoce los bordes del contenedor del histograma.

## sequenceMatch(pattern)(timestamp, cond1, cond2, …) {#function-sequencematch}

Comprueba si la secuencia contiene una cadena de eventos que coincida con el patrón.

``` sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

!!! warning "Advertencia"
    Los eventos que ocurren en el mismo segundo pueden estar en la secuencia en un orden indefinido que afecta el resultado.

**Parámetros**

-   `pattern` — Pattern string. See [Sintaxis de patrón](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` y `DateTime`. También puede utilizar cualquiera de los [UInt](../../sql-reference/data-types/int-uint.md) tipos de datos.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. Puede pasar hasta 32 argumentos de condición. La función sólo tiene en cuenta los eventos descritos en estas condiciones. Si la secuencia contiene datos que no se describen en una condición, la función los omite.

**Valores devueltos**

-   1, si el patrón coincide.
-   0, si el patrón no coincide.

Tipo: `UInt8`.

<a name="sequence-function-pattern-syntax"></a>
**Sintaxis de patrón**

-   `(?N)` — Matches the condition argument at position `N`. Las condiciones están numeradas en el `[1, 32]` gama. Por ejemplo, `(?1)` coincide con el argumento pasado al `cond1` parámetro.

-   `.*` — Matches any number of events. You don't need conditional arguments to match this element of the pattern.

-   `(?t operator value)` — Sets the time in seconds that should separate two events. For example, pattern `(?1)(?t>1800)(?2)` coincide con los eventos que ocurren a más de 1800 segundos el uno del otro. Un número arbitrario de cualquier evento puede estar entre estos eventos. Puede usar el `>=`, `>`, `<`, `<=` operador.

**Ejemplos**

Considere los datos en el `t` tabla:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

Realizar la consulta:

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

La función encontró la cadena de eventos donde el número 2 sigue al número 1. Se saltó el número 3 entre ellos, porque el número no se describe como un evento. Si queremos tener en cuenta este número al buscar la cadena de eventos dada en el ejemplo, debemos establecer una condición para ello.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

En este caso, la función no pudo encontrar la cadena de eventos que coincida con el patrón, porque el evento para el número 3 ocurrió entre 1 y 2. Si en el mismo caso comprobamos la condición para el número 4, la secuencia coincidiría con el patrón.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**Ver también**

-   [sequenceCount](#function-sequencecount)

## sequenceCount(pattern)(time, cond1, cond2, …) {#function-sequencecount}

Cuenta el número de cadenas de eventos que coinciden con el patrón. La función busca cadenas de eventos que no se superponen. Comienza a buscar la siguiente cadena después de que se haga coincidir la cadena actual.

!!! warning "Advertencia"
    Los eventos que ocurren en el mismo segundo pueden estar en la secuencia en un orden indefinido que afecta el resultado.

``` sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**Parámetros**

-   `pattern` — Pattern string. See [Sintaxis de patrón](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` y `DateTime`. También puede utilizar cualquiera de los [UInt](../../sql-reference/data-types/int-uint.md) tipos de datos.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. Puede pasar hasta 32 argumentos de condición. La función sólo tiene en cuenta los eventos descritos en estas condiciones. Si la secuencia contiene datos que no se describen en una condición, la función los omite.

**Valores devueltos**

-   Número de cadenas de eventos no superpuestas que coinciden.

Tipo: `UInt64`.

**Ejemplo**

Considere los datos en el `t` tabla:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

Cuente cuántas veces ocurre el número 2 después del número 1 con cualquier cantidad de otros números entre ellos:

``` sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

**Ver también**

-   [sequenceMatch](#function-sequencematch)

## ventanaEmbudo {#windowfunnel}

Busca cadenas de eventos en una ventana de tiempo deslizante y calcula el número máximo de eventos que ocurrieron desde la cadena.

La función funciona de acuerdo con el algoritmo:

-   La función busca datos que desencadenan la primera condición en la cadena y establece el contador de eventos en 1. Este es el momento en que comienza la ventana deslizante.

-   Si los eventos de la cadena ocurren secuencialmente dentro de la ventana, el contador se incrementa. Si se interrumpe la secuencia de eventos, el contador no se incrementa.

-   Si los datos tienen varias cadenas de eventos en diferentes puntos de finalización, la función solo generará el tamaño de la cadena más larga.

**Sintaxis**

``` sql
windowFunnel(window, [mode])(timestamp, cond1, cond2, ..., condN)
```

**Parámetros**

-   `window` — Length of the sliding window in seconds.
-   `mode` - Es un argumento opcional.
    -   `'strict'` - Cuando el `'strict'` se establece, windowFunnel() aplica condiciones solo para los valores únicos.
-   `timestamp` — Name of the column containing the timestamp. Data types supported: [Fecha](../../sql-reference/data-types/date.md), [FechaHora](../../sql-reference/data-types/datetime.md#data_type-datetime) y otros tipos de enteros sin signo (tenga en cuenta que aunque timestamp admite el `UInt64` tipo, su valor no puede exceder el máximo de Int64, que es 2 ^ 63 - 1).
-   `cond` — Conditions or data describing the chain of events. [UInt8](../../sql-reference/data-types/int-uint.md).

**Valor devuelto**

El número máximo de condiciones desencadenadas consecutivas de la cadena dentro de la ventana de tiempo deslizante.
Se analizan todas las cadenas en la selección.

Tipo: `Integer`.

**Ejemplo**

Determine si un período de tiempo establecido es suficiente para que el usuario seleccione un teléfono y lo compre dos veces en la tienda en línea.

Establezca la siguiente cadena de eventos:

1.  El usuario inició sesión en su cuenta en la tienda (`eventID = 1003`).
2.  El usuario busca un teléfono (`eventID = 1007, product = 'phone'`).
3.  El usuario realizó un pedido (`eventID = 1009`).
4.  El usuario volvió a realizar el pedido (`eventID = 1010`).

Tabla de entrada:

``` text
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-28 │       1 │ 2019-01-29 10:00:00 │    1003 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-31 │       1 │ 2019-01-31 09:00:00 │    1007 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-30 │       1 │ 2019-01-30 08:00:00 │    1009 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-02-01 │       1 │ 2019-02-01 08:00:00 │    1010 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
```

Averigüe hasta qué punto el usuario `user_id` podría atravesar la cadena en un período de enero a febrero de 2019.

Consulta:

``` sql
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM trend
    WHERE (event_date >= '2019-01-01') AND (event_date <= '2019-02-02')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC
```

Resultado:

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

## retención {#retention}

La función toma como argumentos un conjunto de condiciones de 1 a 32 argumentos de tipo `UInt8` que indican si se cumplió una determinada condición para el evento.
Cualquier condición se puede especificar como un argumento (como en [WHERE](../../sql-reference/statements/select/where.md#select-where)).

Las condiciones, excepto la primera, se aplican en pares: el resultado del segundo será verdadero si el primero y el segundo son verdaderos, del tercero si el primero y el fird son verdaderos, etc.

**Sintaxis**

``` sql
retention(cond1, cond2, ..., cond32);
```

**Parámetros**

-   `cond` — an expression that returns a `UInt8` resultado (1 o 0).

**Valor devuelto**

La matriz de 1 o 0.

-   1 — condition was met for the event.
-   0 — condition wasn't met for the event.

Tipo: `UInt8`.

**Ejemplo**

Consideremos un ejemplo de cálculo del `retention` función para determinar el tráfico del sitio.

**1.** Сreate a table to illustrate an example.

``` sql
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

Tabla de entrada:

Consulta:

``` sql
SELECT * FROM retention_test
```

Resultado:

``` text
┌───────date─┬─uid─┐
│ 2020-01-01 │   0 │
│ 2020-01-01 │   1 │
│ 2020-01-01 │   2 │
│ 2020-01-01 │   3 │
│ 2020-01-01 │   4 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-02 │   0 │
│ 2020-01-02 │   1 │
│ 2020-01-02 │   2 │
│ 2020-01-02 │   3 │
│ 2020-01-02 │   4 │
│ 2020-01-02 │   5 │
│ 2020-01-02 │   6 │
│ 2020-01-02 │   7 │
│ 2020-01-02 │   8 │
│ 2020-01-02 │   9 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-03 │   0 │
│ 2020-01-03 │   1 │
│ 2020-01-03 │   2 │
│ 2020-01-03 │   3 │
│ 2020-01-03 │   4 │
│ 2020-01-03 │   5 │
│ 2020-01-03 │   6 │
│ 2020-01-03 │   7 │
│ 2020-01-03 │   8 │
│ 2020-01-03 │   9 │
│ 2020-01-03 │  10 │
│ 2020-01-03 │  11 │
│ 2020-01-03 │  12 │
│ 2020-01-03 │  13 │
│ 2020-01-03 │  14 │
└────────────┴─────┘
```

**2.** Agrupar usuarios por ID único `uid` utilizando el `retention` función.

Consulta:

``` sql
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

Resultado:

``` text
┌─uid─┬─r───────┐
│   0 │ [1,1,1] │
│   1 │ [1,1,1] │
│   2 │ [1,1,1] │
│   3 │ [1,1,1] │
│   4 │ [1,1,1] │
│   5 │ [0,0,0] │
│   6 │ [0,0,0] │
│   7 │ [0,0,0] │
│   8 │ [0,0,0] │
│   9 │ [0,0,0] │
│  10 │ [0,0,0] │
│  11 │ [0,0,0] │
│  12 │ [0,0,0] │
│  13 │ [0,0,0] │
│  14 │ [0,0,0] │
└─────┴─────────┘
```

**3.** Calcule el número total de visitas al sitio por día.

Consulta:

``` sql
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
```

Resultado:

``` text
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

Donde:

-   `r1`- el número de visitantes únicos que visitaron el sitio durante 2020-01-01 (la `cond1` condición).
-   `r2`- el número de visitantes únicos que visitaron el sitio durante un período de tiempo específico entre 2020-01-01 y 2020-01-02 (`cond1` y `cond2` condición).
-   `r3`- el número de visitantes únicos que visitaron el sitio durante un período de tiempo específico entre 2020-01-01 y 2020-01-03 (`cond1` y `cond3` condición).

## UniqUpTo(N)(x) {#uniquptonx}

Calculates the number of different argument values ​​if it is less than or equal to N. If the number of different argument values is greater than N, it returns N + 1.

Recomendado para usar con Ns pequeños, hasta 10. El valor máximo de N es 100.

Para el estado de una función agregada, utiliza la cantidad de memoria igual a 1 + N \* el tamaño de un valor de bytes.
Para las cadenas, almacena un hash no criptográfico de 8 bytes. Es decir, el cálculo se aproxima a las cadenas.

La función también funciona para varios argumentos.

Funciona lo más rápido posible, excepto en los casos en que se usa un valor N grande y el número de valores únicos es ligeramente menor que N.

Ejemplo de uso:

``` text
Problem: Generate a report that shows only keywords that produced at least 5 unique users.
Solution: Write in the GROUP BY query SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/agg_functions/parametric_functions/) <!--hide-->

## sumMapFiltered(keys\_to\_keep)(claves, valores) {#summapfilteredkeys-to-keepkeys-values}

El mismo comportamiento que [sumMap](reference.md#agg_functions-summap) excepto que una matriz de claves se pasa como un parámetro. Esto puede ser especialmente útil cuando se trabaja con una alta cardinalidad de claves.
