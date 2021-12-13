---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 66
toc_title: Otro
---

# Otras funciones {#other-functions}

## nombre de host() {#hostname}

Devuelve una cadena con el nombre del host en el que se realizó esta función. Para el procesamiento distribuido, este es el nombre del host del servidor remoto, si la función se realiza en un servidor remoto.

## getMacro {#getmacro}

Obtiene un valor con nombre del [macro](../../operations/server-configuration-parameters/settings.md#macros) sección de la configuración del servidor.

**Sintaxis**

``` sql
getMacro(name);
```

**Parámetros**

-   `name` — Name to retrieve from the `macros` apartado. [Cadena](../../sql-reference/data-types/string.md#string).

**Valor devuelto**

-   Valor de la macro especificada.

Tipo: [Cadena](../../sql-reference/data-types/string.md).

**Ejemplo**

Ejemplo `macros` sección en el archivo de configuración del servidor:

``` xml
<macros>
    <test>Value</test>
</macros>
```

Consulta:

``` sql
SELECT getMacro('test');
```

Resultado:

``` text
┌─getMacro('test')─┐
│ Value            │
└──────────────────┘
```

Una forma alternativa de obtener el mismo valor:

``` sql
SELECT * FROM system.macros
WHERE macro = 'test';
```

``` text
┌─macro─┬─substitution─┐
│ test  │ Value        │
└───────┴──────────────┘
```

## FQDN {#fqdn}

Devuelve el nombre de dominio completo.

**Sintaxis**

``` sql
fqdn();
```

Esta función no distingue entre mayúsculas y minúsculas.

**Valor devuelto**

-   Cadena con el nombre de dominio completo.

Tipo: `String`.

**Ejemplo**

Consulta:

``` sql
SELECT FQDN();
```

Resultado:

``` text
┌─FQDN()──────────────────────────┐
│ clickhouse.ru-central1.internal │
└─────────────────────────────────┘
```

## Nombre básico {#basename}

Extrae la parte final de una cadena después de la última barra o barra invertida. Esta función se utiliza a menudo para extraer el nombre de archivo de una ruta.

``` sql
basename( expr )
```

**Parámetros**

-   `expr` — Expression resulting in a [Cadena](../../sql-reference/data-types/string.md) valor de tipo. Todas las barras diagonales inversas deben escaparse en el valor resultante.

**Valor devuelto**

Una cadena que contiene:

-   La parte final de una cadena después de la última barra o barra invertida.

        If the input string contains a path ending with slash or backslash, for example, `/` or `c:\`, the function returns an empty string.

-   La cadena original si no hay barras diagonales o barras diagonales inversas.

**Ejemplo**

``` sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some-file-name' AS a, basename(a)
```

``` text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## Ancho visible (x) {#visiblewidthx}

Calcula el ancho aproximado al enviar valores a la consola en formato de texto (separado por tabuladores).
Esta función es utilizada por el sistema para implementar formatos Pretty.

`NULL` se representa como una cadena correspondiente a `NULL` en `Pretty` formato.

``` sql
SELECT visibleWidth(NULL)
```

``` text
┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## ¿Cómo puedo hacerlo?) {#totypenamex}

Devuelve una cadena que contiene el nombre de tipo del argumento pasado.

Si `NULL` se pasa a la función como entrada, luego devuelve el `Nullable(Nothing)` tipo, que corresponde a un tipo interno `NULL` representación en ClickHouse.

## BlockSize() {#function-blocksize}

Obtiene el tamaño del bloque.
En ClickHouse, las consultas siempre se ejecutan en bloques (conjuntos de partes de columna). Esta función permite obtener el tamaño del bloque al que lo llamó.

## materializar (x) {#materializex}

Convierte una constante en una columna completa que contiene solo un valor.
En ClickHouse, las columnas completas y las constantes se representan de manera diferente en la memoria. Las funciones funcionan de manera diferente para argumentos constantes y argumentos normales (se ejecuta un código diferente), aunque el resultado es casi siempre el mismo. Esta función es para depurar este comportamiento.

## ignore(…) {#ignore}

Acepta cualquier argumento, incluyendo `NULL`. Siempre devuelve 0.
Sin embargo, el argumento aún se evalúa. Esto se puede usar para puntos de referencia.

## sueño (segundos) {#sleepseconds}

Dormir ‘seconds’ segundos en cada bloque de datos. Puede especificar un número entero o un número de punto flotante.

## sleepEachRow(segundos) {#sleepeachrowseconds}

Dormir ‘seconds’ segundos en cada fila. Puede especificar un número entero o un número de punto flotante.

## currentDatabase() {#currentdatabase}

Devuelve el nombre de la base de datos actual.
Puede utilizar esta función en los parámetros del motor de tablas en una consulta CREATE TABLE donde debe especificar la base de datos.

## currentUser() {#other-function-currentuser}

Devuelve el inicio de sesión del usuario actual. El inicio de sesión del usuario, que inició la consulta, se devolverá en caso de consulta distibuted.

``` sql
SELECT currentUser();
```

Apodo: `user()`, `USER()`.

**Valores devueltos**

-   Inicio de sesión del usuario actual.
-   Inicio de sesión del usuario que inició la consulta en caso de consulta distribuida.

Tipo: `String`.

**Ejemplo**

Consulta:

``` sql
SELECT currentUser();
```

Resultado:

``` text
┌─currentUser()─┐
│ default       │
└───────────────┘
```

## isConstant {#is-constant}

Comprueba si el argumento es una expresión constante.

A constant expression means an expression whose resulting value is known at the query analysis (i.e. before execution). For example, expressions over [literal](../syntax.md#literals) son expresiones constantes.

La función está destinada al desarrollo, depuración y demostración.

**Sintaxis**

``` sql
isConstant(x)
```

**Parámetros**

-   `x` — Expression to check.

**Valores devueltos**

-   `1` — `x` es constante.
-   `0` — `x` no es constante.

Tipo: [UInt8](../data-types/int-uint.md).

**Ejemplos**

Consulta:

``` sql
SELECT isConstant(x + 1) FROM (SELECT 43 AS x)
```

Resultado:

``` text
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
```

Consulta:

``` sql
WITH 3.14 AS pi SELECT isConstant(cos(pi))
```

Resultado:

``` text
┌─isConstant(cos(pi))─┐
│                   1 │
└─────────────────────┘
```

Consulta:

``` sql
SELECT isConstant(number) FROM numbers(1)
```

Resultado:

``` text
┌─isConstant(number)─┐
│                  0 │
└────────────────────┘
```

## isFinite(x) {#isfinitex}

Acepta Float32 y Float64 y devuelve UInt8 igual a 1 si el argumento no es infinito y no es un NaN, de lo contrario 0.

## IsInfinite(x) {#isinfinitex}

Acepta Float32 y Float64 y devuelve UInt8 igual a 1 si el argumento es infinito, de lo contrario 0. Tenga en cuenta que se devuelve 0 para un NaN.

## ifNotFinite {#ifnotfinite}

Comprueba si el valor de punto flotante es finito.

**Sintaxis**

    ifNotFinite(x,y)

**Parámetros**

-   `x` — Value to be checked for infinity. Type: [Flotante\*](../../sql-reference/data-types/float.md).
-   `y` — Fallback value. Type: [Flotante\*](../../sql-reference/data-types/float.md).

**Valor devuelto**

-   `x` si `x` es finito.
-   `y` si `x` no es finito.

**Ejemplo**

Consulta:

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

Resultado:

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

Puede obtener un resultado similar usando [operador ternario](conditional-functions.md#ternary-operator): `isFinite(x) ? x : y`.

## isNaN(x) {#isnanx}

Acepta Float32 y Float64 y devuelve UInt8 igual a 1 si el argumento es un NaN, de lo contrario 0.

## hasColumnInTable(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’) {#hascolumnintablehostname-username-password-database-table-column}

Acepta cadenas constantes: nombre de base de datos, nombre de tabla y nombre de columna. Devuelve una expresión constante UInt8 igual a 1 si hay una columna; de lo contrario, 0. Si se establece el parámetro hostname, la prueba se ejecutará en un servidor remoto.
La función produce una excepción si la tabla no existe.
Para los elementos de una estructura de datos anidada, la función comprueba la existencia de una columna. Para la propia estructura de datos anidados, la función devuelve 0.

## Bar {#function-bar}

Permite construir un diagrama unicode-art.

`bar(x, min, max, width)` dibuja una banda con un ancho proporcional a `(x - min)` e igual a `width` caracteres cuando `x = max`.

Parámetros:

-   `x` — Size to display.
-   `min, max` — Integer constants. The value must fit in `Int64`.
-   `width` — Constant, positive integer, can be fractional.

La banda se dibuja con precisión a un octavo de un símbolo.

Ejemplo:

``` sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

``` text
┌──h─┬──────c─┬─bar────────────────┐
│  0 │ 292907 │ █████████▋         │
│  1 │ 180563 │ ██████             │
│  2 │ 114861 │ ███▋               │
│  3 │  85069 │ ██▋                │
│  4 │  68543 │ ██▎                │
│  5 │  78116 │ ██▌                │
│  6 │ 113474 │ ███▋               │
│  7 │ 170678 │ █████▋             │
│  8 │ 278380 │ █████████▎         │
│  9 │ 391053 │ █████████████      │
│ 10 │ 457681 │ ███████████████▎   │
│ 11 │ 493667 │ ████████████████▍  │
│ 12 │ 509641 │ ████████████████▊  │
│ 13 │ 522947 │ █████████████████▍ │
│ 14 │ 539954 │ █████████████████▊ │
│ 15 │ 528460 │ █████████████████▌ │
│ 16 │ 539201 │ █████████████████▊ │
│ 17 │ 523539 │ █████████████████▍ │
│ 18 │ 506467 │ ████████████████▊  │
│ 19 │ 520915 │ █████████████████▎ │
│ 20 │ 521665 │ █████████████████▍ │
│ 21 │ 542078 │ ██████████████████ │
│ 22 │ 493642 │ ████████████████▍  │
│ 23 │ 400397 │ █████████████▎     │
└────┴────────┴────────────────────┘
```

## transformar {#transform}

Transforma un valor de acuerdo con la asignación explícitamente definida de algunos elementos a otros.
Hay dos variaciones de esta función:

### ¿Cómo puedo hacerlo?) {#transformx-array-from-array-to-default}

`x` – What to transform.

`array_from` – Constant array of values for converting.

`array_to` – Constant array of values to convert the values in ‘from’ a.

`default` – Which value to use if ‘x’ no es igual a ninguno de los valores en ‘from’.

`array_from` y `array_to` – Arrays of the same size.

Tipo:

`transform(T, Array(T), Array(U), U) -> U`

`T` y `U` pueden ser tipos numéricos, de cadena o de fecha o de fecha y hora.
Cuando se indica la misma letra (T o U), para los tipos numéricos pueden no ser tipos coincidentes, sino tipos que tienen un tipo común.
Por ejemplo, el primer argumento puede tener el tipo Int64, mientras que el segundo tiene el tipo Array(UInt16).

Si el ‘x’ valor es igual a uno de los elementos en el ‘array_from’ matriz, devuelve el elemento existente (que está numerado igual) de la ‘array_to’ matriz. De lo contrario, devuelve ‘default’. Si hay varios elementos coincidentes en ‘array_from’, devuelve una de las coincidencias.

Ejemplo:

``` sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

``` text
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

### ¿Cómo puedo hacerlo?) {#transformx-array-from-array-to}

Difiere de la primera variación en que el ‘default’ se omite el argumento.
Si el ‘x’ valor es igual a uno de los elementos en el ‘array_from’ matriz, devuelve el elemento coincidente (que está numerado igual) de la ‘array_to’ matriz. De lo contrario, devuelve ‘x’.

Tipo:

`transform(T, Array(T), Array(T)) -> T`

Ejemplo:

``` sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vk.com'], ['www.yandex', 'example.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

``` text
┌─s──────────────┬───────c─┐
│                │ 2906259 │
│ www.yandex     │  867767 │
│ ███████.ru     │  313599 │
│ mail.yandex.ru │  107147 │
│ ██████.ru      │  100355 │
│ █████████.ru   │   65040 │
│ news.yandex.ru │   64515 │
│ ██████.net     │   59141 │
│ example.com    │   57316 │
└────────────────┴─────────┘
```

## Tamaño de formatoReadable (x) {#formatreadablesizex}

Acepta el tamaño (número de bytes). Devuelve un tamaño redondeado con un sufijo (KiB, MiB, etc.) como una cadena.

Ejemplo:

``` sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

``` text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## menos (a, b) {#leasta-b}

Devuelve el valor más pequeño de a y b.

## mayor(a, b) {#greatesta-b}

Devuelve el valor más grande de a y b.

## operatividad() {#uptime}

Devuelve el tiempo de actividad del servidor en segundos.

## versión() {#version}

Devuelve la versión del servidor como una cadena.

## Zona horaria() {#timezone}

Devuelve la zona horaria del servidor.

## blockNumber {#blocknumber}

Devuelve el número de secuencia del bloque de datos donde se encuentra la fila.

## rowNumberInBlock {#function-rownumberinblock}

Devuelve el número ordinal de la fila en el bloque de datos. Los diferentes bloques de datos siempre se recalculan.

## rowNumberInAllBlocks() {#rownumberinallblocks}

Devuelve el número ordinal de la fila en el bloque de datos. Esta función solo considera los bloques de datos afectados.

## vecino {#neighbor}

La función de ventana que proporciona acceso a una fila en un desplazamiento especificado que viene antes o después de la fila actual de una columna determinada.

**Sintaxis**

``` sql
neighbor(column, offset[, default_value])
```

El resultado de la función depende de los bloques de datos afectados y del orden de los datos en el bloque.
Si realiza una subconsulta con ORDER BY y llama a la función desde fuera de la subconsulta, puede obtener el resultado esperado.

**Parámetros**

-   `column` — A column name or scalar expression.
-   `offset` — The number of rows forwards or backwards from the current row of `column`. [Int64](../../sql-reference/data-types/int-uint.md).
-   `default_value` — Optional. The value to be returned if offset goes beyond the scope of the block. Type of data blocks affected.

**Valores devueltos**

-   Valor para `column` en `offset` distancia de la fila actual si `offset` valor no está fuera de los límites del bloque.
-   Valor predeterminado para `column` si `offset` valor está fuera de los límites del bloque. Si `default_value` se da, entonces será utilizado.

Tipo: tipo de bloques de datos afectados o tipo de valor predeterminado.

**Ejemplo**

Consulta:

``` sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

Resultado:

``` text
┌─number─┬─neighbor(number, 2)─┐
│      0 │                   2 │
│      1 │                   3 │
│      2 │                   4 │
│      3 │                   5 │
│      4 │                   6 │
│      5 │                   7 │
│      6 │                   8 │
│      7 │                   9 │
│      8 │                   0 │
│      9 │                   0 │
└────────┴─────────────────────┘
```

Consulta:

``` sql
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
```

Resultado:

``` text
┌─number─┬─neighbor(number, 2, 999)─┐
│      0 │                        2 │
│      1 │                        3 │
│      2 │                        4 │
│      3 │                        5 │
│      4 │                        6 │
│      5 │                        7 │
│      6 │                        8 │
│      7 │                        9 │
│      8 │                      999 │
│      9 │                      999 │
└────────┴──────────────────────────┘
```

Esta función se puede utilizar para calcular el valor métrico interanual:

Consulta:

``` sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

Resultado:

``` text
┌──────month─┬─money─┬─prev_year─┬─year_over_year─┐
│ 2018-01-01 │    32 │         0 │              0 │
│ 2018-02-01 │    63 │         0 │              0 │
│ 2018-03-01 │    91 │         0 │              0 │
│ 2018-04-01 │    22 │         0 │              0 │
│ 2018-05-01 │    52 │         0 │              0 │
│ 2018-06-01 │    83 │         0 │              0 │
│ 2018-07-01 │    13 │         0 │              0 │
│ 2018-08-01 │    44 │         0 │              0 │
│ 2018-09-01 │    75 │         0 │              0 │
│ 2018-10-01 │     5 │         0 │              0 │
│ 2018-11-01 │    36 │         0 │              0 │
│ 2018-12-01 │    66 │         0 │              0 │
│ 2019-01-01 │    97 │        32 │           0.33 │
│ 2019-02-01 │    28 │        63 │           2.25 │
│ 2019-03-01 │    56 │        91 │           1.62 │
│ 2019-04-01 │    87 │        22 │           0.25 │
└────────────┴───────┴───────────┴────────────────┘
```

## EjecuciónDiferencia (x) {#other_functions-runningdifference}

Calculates the difference between successive row values ​​in the data block.
Devuelve 0 para la primera fila y la diferencia con respecto a la fila anterior para cada fila subsiguiente.

El resultado de la función depende de los bloques de datos afectados y del orden de los datos en el bloque.
Si realiza una subconsulta con ORDER BY y llama a la función desde fuera de la subconsulta, puede obtener el resultado esperado.

Ejemplo:

``` sql
SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2016-11-24'
    ORDER BY EventTime ASC
    LIMIT 5
)
```

``` text
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2016-11-24 00:00:04 │     0 │
│    1107 │ 2016-11-24 00:00:05 │     1 │
│    1108 │ 2016-11-24 00:00:05 │     0 │
│    1109 │ 2016-11-24 00:00:09 │     4 │
│    1110 │ 2016-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
```

Tenga en cuenta que el tamaño del bloque afecta el resultado. Con cada nuevo bloque, el `runningDifference` estado de reset.

``` sql
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
```

``` sql
set max_block_size=100000 -- default value is 65536!

SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## runningDifferenceStartingWithFirstvalue {#runningdifferencestartingwithfirstvalue}

Lo mismo que para [runningDifference](./other-functions.md#other_functions-runningdifference), la diferencia es el valor de la primera fila, devolvió el valor de la primera fila, y cada fila subsiguiente devuelve la diferencia de la fila anterior.

## ¿Cómo puedo hacerlo?) {#macnumtostringnum}

Acepta un número UInt64. Lo interpreta como una dirección MAC en big endian. Devuelve una cadena que contiene la dirección MAC correspondiente con el formato AA:BB:CC:DD:EE:FF (números separados por dos puntos en forma hexadecimal).

## Sistema abierto.) {#macstringtonums}

La función inversa de MACNumToString. Si la dirección MAC tiene un formato no válido, devuelve 0.

## Sistema abierto.) {#macstringtoouis}

Acepta una dirección MAC con el formato AA:BB:CC:DD:EE:FF (números separados por dos puntos en forma hexadecimal). Devuelve los primeros tres octetos como un número UInt64. Si la dirección MAC tiene un formato no válido, devuelve 0.

## getSizeOfEnumType {#getsizeofenumtype}

Devuelve el número de campos en [Enum](../../sql-reference/data-types/enum.md).

``` sql
getSizeOfEnumType(value)
```

**Parámetros:**

-   `value` — Value of type `Enum`.

**Valores devueltos**

-   El número de campos con `Enum` valores de entrada.
-   Se produce una excepción si el tipo no es `Enum`.

**Ejemplo**

``` sql
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## BlockSerializedSize {#blockserializedsize}

Devuelve el tamaño en el disco (sin tener en cuenta la compresión).

``` sql
blockSerializedSize(value[, value[, ...]])
```

**Parámetros:**

-   `value` — Any value.

**Valores devueltos**

-   El número de bytes que se escribirán en el disco para el bloque de valores (sin compresión).

**Ejemplo**

``` sql
SELECT blockSerializedSize(maxState(1)) as x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## ToColumnTypeName {#tocolumntypename}

Devuelve el nombre de la clase que representa el tipo de datos de la columna en la RAM.

``` sql
toColumnTypeName(value)
```

**Parámetros:**

-   `value` — Any type of value.

**Valores devueltos**

-   Una cadena con el nombre de la clase que se utiliza para representar la `value` tipo de datos en la memoria RAM.

**Ejemplo de la diferencia entre`toTypeName ' and ' toColumnTypeName`**

``` sql
SELECT toTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

``` sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

El ejemplo muestra que el `DateTime` tipo de datos se almacena en la memoria como `Const(UInt32)`.

## dumpColumnStructure {#dumpcolumnstructure}

Produce una descripción detallada de las estructuras de datos en la memoria RAM

``` sql
dumpColumnStructure(value)
```

**Parámetros:**

-   `value` — Any type of value.

**Valores devueltos**

-   Una cadena que describe la estructura que se utiliza para representar el `value` tipo de datos en la memoria RAM.

**Ejemplo**

``` sql
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))
```

``` text
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## defaultValueOfArgumentType {#defaultvalueofargumenttype}

Genera el valor predeterminado para el tipo de datos.

No incluye valores predeterminados para columnas personalizadas establecidas por el usuario.

``` sql
defaultValueOfArgumentType(expression)
```

**Parámetros:**

-   `expression` — Arbitrary type of value or an expression that results in a value of an arbitrary type.

**Valores devueltos**

-   `0` para los números.
-   Cadena vacía para cadenas.
-   `ᴺᵁᴸᴸ` para [NULL](../../sql-reference/data-types/nullable.md).

**Ejemplo**

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## replicar {#other-functions-replicate}

Crea una matriz con un solo valor.

Utilizado para la implementación interna de [arrayJoin](array-join.md#functions_arrayjoin).

``` sql
SELECT replicate(x, arr);
```

**Parámetros:**

-   `arr` — Original array. ClickHouse creates a new array of the same length as the original and fills it with the value `x`.
-   `x` — The value that the resulting array will be filled with.

**Valor devuelto**

Una matriz llena con el valor `x`.

Tipo: `Array`.

**Ejemplo**

Consulta:

``` sql
SELECT replicate(1, ['a', 'b', 'c'])
```

Resultado:

``` text
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## Sistema de archivosDisponible {#filesystemavailable}

Devuelve la cantidad de espacio restante en el sistema de archivos donde se encuentran los archivos de las bases de datos. Siempre es más pequeño que el espacio libre total ([Sistema de archivosLibre](#filesystemfree)) porque algo de espacio está reservado para el sistema operativo.

**Sintaxis**

``` sql
filesystemAvailable()
```

**Valor devuelto**

-   La cantidad de espacio restante disponible en bytes.

Tipo: [UInt64](../../sql-reference/data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT formatReadableSize(filesystemAvailable()) AS "Available space", toTypeName(filesystemAvailable()) AS "Type";
```

Resultado:

``` text
┌─Available space─┬─Type───┐
│ 30.75 GiB       │ UInt64 │
└─────────────────┴────────┘
```

## Sistema de archivosLibre {#filesystemfree}

Devuelve la cantidad total del espacio libre en el sistema de archivos donde se encuentran los archivos de las bases de datos. Ver también `filesystemAvailable`

**Sintaxis**

``` sql
filesystemFree()
```

**Valor devuelto**

-   Cantidad de espacio libre en bytes.

Tipo: [UInt64](../../sql-reference/data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT formatReadableSize(filesystemFree()) AS "Free space", toTypeName(filesystemFree()) AS "Type";
```

Resultado:

``` text
┌─Free space─┬─Type───┐
│ 32.39 GiB  │ UInt64 │
└────────────┴────────┘
```

## sistema de archivosCapacidad {#filesystemcapacity}

Devuelve la capacidad del sistema de archivos en bytes. Para la evaluación, el [camino](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) al directorio de datos debe estar configurado.

**Sintaxis**

``` sql
filesystemCapacity()
```

**Valor devuelto**

-   Información de capacidad del sistema de archivos en bytes.

Tipo: [UInt64](../../sql-reference/data-types/int-uint.md).

**Ejemplo**

Consulta:

``` sql
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity", toTypeName(filesystemCapacity()) AS "Type"
```

Resultado:

``` text
┌─Capacity──┬─Type───┐
│ 39.32 GiB │ UInt64 │
└───────────┴────────┘
```

## finalizeAggregation {#function-finalizeaggregation}

Toma el estado de la función agregada. Devuelve el resultado de la agregación (estado finalizado).

## runningAccumulate {#function-runningaccumulate}

Toma los estados de la función agregada y devuelve una columna con valores, son el resultado de la acumulación de estos estados para un conjunto de líneas de bloque, desde la primera hasta la línea actual.
Por ejemplo, toma el estado de la función agregada (ejemplo runningAccumulate(uniqState(UserID)) ), y para cada fila de bloque, devuelve el resultado de la función agregada en la fusión de estados de todas las filas anteriores y la fila actual.
Por lo tanto, el resultado de la función depende de la partición de los datos en los bloques y del orden de los datos en el bloque.

## joinGet {#joinget}

La función le permite extraer datos de la tabla de la misma manera que [diccionario](../../sql-reference/dictionaries/index.md).

Obtiene datos de [Unir](../../engines/table-engines/special/join.md#creating-a-table) usando la clave de unión especificada.

Solo admite tablas creadas con `ENGINE = Join(ANY, LEFT, <join_keys>)` instrucción.

**Sintaxis**

``` sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**Parámetros**

-   `join_storage_table_name` — an [identificador](../syntax.md#syntax-identifiers) indica dónde se realiza la búsqueda. El identificador se busca en la base de datos predeterminada (ver parámetro `default_database` en el archivo de configuración). Para reemplazar la base de datos predeterminada, utilice `USE db_name` o especifique la base de datos y la tabla a través del separador `db_name.db_table`, ver el ejemplo.
-   `value_column` — name of the column of the table that contains required data.
-   `join_keys` — list of keys.

**Valor devuelto**

Devuelve la lista de valores correspondientes a la lista de claves.

Si cierto no existe en la tabla fuente, entonces `0` o `null` será devuelto basado en [Sistema abierto.](../../operations/settings/settings.md#join_use_nulls) configuración.

Más información sobre `join_use_nulls` en [Únase a la operación](../../engines/table-engines/special/join.md).

**Ejemplo**

Tabla de entrada:

``` sql
CREATE DATABASE db_test
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 1
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13)
```

``` text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

Consulta:

``` sql
SELECT joinGet(db_test.id_val,'val',toUInt32(number)) from numbers(4) SETTINGS join_use_nulls = 1
```

Resultado:

``` text
┌─joinGet(db_test.id_val, 'val', toUInt32(number))─┐
│                                                0 │
│                                               11 │
│                                               12 │
│                                                0 │
└──────────────────────────────────────────────────┘
```

## modelEvaluate(model_name, …) {#function-modelevaluate}

Evaluar modelo externo.
Acepta un nombre de modelo y argumentos de modelo. Devuelve Float64.

## ¿Cómo puedo hacerlo?\]) {#throwifx-custom-message}

Lance una excepción si el argumento no es cero.
custom_message - es un parámetro opcional: una cadena constante, proporciona un mensaje de error

``` sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

``` text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## identidad {#identity}

Devuelve el mismo valor que se usó como argumento. Se utiliza para la depuración y pruebas, permite cancelar el uso de índice, y obtener el rendimiento de la consulta de un análisis completo. Cuando se analiza la consulta para el posible uso del índice, el analizador no mira dentro `identity` función.

**Sintaxis**

``` sql
identity(x)
```

**Ejemplo**

Consulta:

``` sql
SELECT identity(42)
```

Resultado:

``` text
┌─identity(42)─┐
│           42 │
└──────────────┘
```

## randomPrintableASCII {#randomascii}

Genera una cadena con un conjunto aleatorio de [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) caracteres imprimibles.

**Sintaxis**

``` sql
randomPrintableASCII(length)
```

**Parámetros**

-   `length` — Resulting string length. Positive integer.

        If you pass `length < 0`, behavior of the function is undefined.

**Valor devuelto**

-   Cadena con un conjunto aleatorio de [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) caracteres imprimibles.

Tipo: [Cadena](../../sql-reference/data-types/string.md)

**Ejemplo**

``` sql
SELECT number, randomPrintableASCII(30) as str, length(str) FROM system.numbers LIMIT 3
```

``` text
┌─number─┬─str────────────────────────────┬─length(randomPrintableASCII(30))─┐
│      0 │ SuiCOSTvC0csfABSw=UcSzp2.`rv8x │                               30 │
│      1 │ 1Ag NlJ &RCN:*>HVPG;PE-nO"SUFD │                               30 │
│      2 │ /"+<"wUTh:=LjJ Vm!c&hI*m#XTfzz │                               30 │
└────────┴────────────────────────────────┴──────────────────────────────────┘
```

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/other_functions/) <!--hide-->
