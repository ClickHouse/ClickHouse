# Funciones de conversión de tipos {#type-conversion-functions}

## Problemas comunes de conversiones numéricas {#numeric-conversion-issues}

Cuando convierte un valor de uno a otro tipo de datos, debe recordar que, en un caso común, es una operación insegura que puede provocar una pérdida de datos. Puede producirse una pérdida de datos si intenta ajustar el valor de un tipo de datos más grande a un tipo de datos más pequeño, o si convierte valores entre diferentes tipos de datos.

ClickHouse tiene el [mismo comportamiento que los programas de C++](https://en.cppreference.com/w/cpp/language/implicit_conversion).

## ¿Cómo puedo obtener más información?) {#toint8163264}

Convierte un valor de entrada en el [En](../../data_types/int_uint.md) tipo de datos. Esta familia de funciones incluye:

-   `toInt8(expr)` — Resultados en el `Int8` tipo de datos.
-   `toInt16(expr)` — Resultados en el `Int16` tipo de datos.
-   `toInt32(expr)` — Resultados en el `Int32` tipo de datos.
-   `toInt64(expr)` — Resultados en el `Int64` tipo de datos.

**Parámetros**

-   `expr` — [Expresion](../syntax.md#syntax-expressions) devolviendo un número o una cadena con la representación decimal de un número. No se admiten representaciones binarias, octales y hexadecimales de números. Los ceros principales son despojados.

**Valor devuelto**

Valor entero en el `Int8`, `Int16`, `Int32`, o `Int64` tipo de datos.

Funciones de uso [redondeando hacia cero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), lo que significa que truncan dígitos fraccionarios de números.

El comportamiento de las funciones [NaN y Inf](../../data_types/float.md#data_type-float-nan-inf) los argumentos no están definidos. Recuerde acerca de [problemas de conversión numérica](#numeric-conversion-issues), al usar las funciones.

**Ejemplo**

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8)
```

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## ¿Cómo puedo obtener más información? {#toint8163264orzero}

Toma un argumento de tipo String e intenta analizarlo en Int (8 \| 16 \| 32 \| 64). Si falla, devuelve 0.

**Ejemplo**

``` sql
select toInt64OrZero('123123'), toInt8OrZero('123qwe123')
```

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## ¿Cómo puedo hacerlo? {#toint8163264ornull}

Toma un argumento de tipo String e intenta analizarlo en Int (8 \| 16 \| 32 \| 64). Si falla, devuelve NULL.

**Ejemplo**

``` sql
select toInt64OrNull('123123'), toInt8OrNull('123qwe123')
```

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## ¿Cómo puedo obtener más información?) {#touint8163264}

Convierte un valor de entrada en el [UInt](../../data_types/int_uint.md) tipo de datos. Esta familia de funciones incluye:

-   `toUInt8(expr)` — Resultados en el `UInt8` tipo de datos.
-   `toUInt16(expr)` — Resultados en el `UInt16` tipo de datos.
-   `toUInt32(expr)` — Resultados en el `UInt32` tipo de datos.
-   `toUInt64(expr)` — Resultados en el `UInt64` tipo de datos.

**Parámetros**

-   `expr` — [Expresion](../syntax.md#syntax-expressions) devolviendo un número o una cadena con la representación decimal de un número. No se admiten representaciones binarias, octales y hexadecimales de números. Los ceros principales son despojados.

**Valor devuelto**

Valor entero en el `UInt8`, `UInt16`, `UInt32`, o `UInt64` tipo de datos.

Funciones de uso [redondeando hacia cero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), lo que significa que truncan dígitos fraccionarios de números.

El comportamiento de las funciones para los instrumentos negativos y para [NaN y Inf](../../data_types/float.md#data_type-float-nan-inf) los argumentos no están definidos. Si pasa una cadena con un número negativo, por ejemplo `'-32'`, ClickHouse genera una excepción. Recuerde acerca de [problemas de conversión numérica](#numeric-conversion-issues), al usar las funciones.

**Ejemplo**

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8)
```

``` text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## ¿Cómo puedo obtener más información? {#touint8163264orzero}

## ¿Cómo puedo hacerlo? {#touint8163264ornull}

## ¿Cómo puedo obtener más información?) {#tofloat3264}

## ¿Cómo puedo hacerlo? {#tofloat3264orzero}

## ¿Cómo puedo hacerlo? {#tofloat3264ornull}

## Fecha {#todate}

## Todos los derechos reservados {#todateorzero}

## ToDateOrNull {#todateornull}

## toDateTime {#todatetime}

## ToDateTimeOrZero {#todatetimeorzero}

## ToDateTimeOrNull {#todatetimeornull}

## toDecimal(32/64/128) {#todecimal3264128}

Convertir `value` a la [Decimal](../../data_types/decimal.md) tipo de datos con precisión de `S`. El `value` puede ser un número o una cadena. El `S` (escala) parámetro especifica el número de decimales.

-   `toDecimal32(value, S)`
-   `toDecimal64(value, S)`
-   `toDecimal128(value, S)`

## ¿Cómo puedo hacer esto? {#todecimal3264128ornull}

Convierte una cadena de entrada en un [Información detallada))](../../data_types/decimal.md) valor de tipo de datos. Esta familia de funciones incluye:

-   `toDecimal32OrNull(expr, S)` — Resultados en `Nullable(Decimal32(S))` tipo de datos.
-   `toDecimal64OrNull(expr, S)` — Resultados en `Nullable(Decimal64(S))` tipo de datos.
-   `toDecimal128OrNull(expr, S)` — Resultados en `Nullable(Decimal128(S))` tipo de datos.

Estas funciones deben usarse en lugar de `toDecimal*()` funciones, si usted prefiere conseguir un `NULL` valor de entrada en lugar de una excepción en el caso de un error de análisis de valor de entrada.

**Parámetros**

-   `expr` — [Expresion](../syntax.md#syntax-expressions), devuelve un valor en el [Cadena](../../data_types/string.md) tipo de datos. ClickHouse espera la representación textual del número decimal. Por ejemplo, `'1.111'`.
-   `S` — Escala, el número de decimales en el valor resultante.

**Valor devuelto**

Un valor en el `Nullable(Decimal(P,S))` tipo de datos. El valor contiene:

-   Número con `S` lugares decimales, si ClickHouse interpreta la cadena de entrada como un número.
-   `NULL`, si ClickHouse no puede interpretar la cadena de entrada como un número o si el número de entrada contiene más de `S` lugares decimales.

**Ejemplos**

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌──────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.11100 │ Nullable(Decimal(9, 5))                            │
└──────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```

## Por ejemplo: {#todecimal3264128orzero}

Convierte un valor de entrada en el [Decimal (P, S)](../../data_types/decimal.md) tipo de datos. Esta familia de funciones incluye:

-   `toDecimal32OrZero( expr, S)` — Resultados en `Decimal32(S)` tipo de datos.
-   `toDecimal64OrZero( expr, S)` — Resultados en `Decimal64(S)` tipo de datos.
-   `toDecimal128OrZero( expr, S)` — Resultados en `Decimal128(S)` tipo de datos.

Estas funciones deben usarse en lugar de `toDecimal*()` funciones, si usted prefiere conseguir un `0` valor de entrada en lugar de una excepción en el caso de un error de análisis de valor de entrada.

**Parámetros**

-   `expr` — [Expresion](../syntax.md#syntax-expressions), devuelve un valor en el [Cadena](../../data_types/string.md) tipo de datos. ClickHouse espera la representación textual del número decimal. Por ejemplo, `'1.111'`.
-   `S` — Escala, el número de decimales en el valor resultante.

**Valor devuelto**

Un valor en el `Nullable(Decimal(P,S))` tipo de datos. El valor contiene:

-   Número con `S` lugares decimales, si ClickHouse interpreta la cadena de entrada como un número.
-   0 con `S` decimales, si ClickHouse no puede interpretar la cadena de entrada como un número o si el número de entrada contiene más de `S` lugares decimales.

**Ejemplo**

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌──────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.11100 │ Decimal(9, 5)                                      │
└──────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```

## ToString {#tostring}

Funciones para convertir entre números, cadenas (pero no cadenas fijas), fechas y fechas con horas.
Todas estas funciones aceptan un argumento.

Al convertir a o desde una cadena, el valor se formatea o se analiza utilizando las mismas reglas que para el formato TabSeparated (y casi todos los demás formatos de texto). Si no se puede analizar la cadena, se produce una excepción y se cancela la solicitud.

Al convertir fechas a números o viceversa, la fecha corresponde al número de días desde el comienzo de la época Unix.
Al convertir fechas con horas a números o viceversa, la fecha con hora corresponde al número de segundos desde el comienzo de la época Unix.

Los formatos de fecha y fecha con hora para las funciones toDate/toDateTime se definen de la siguiente manera:

``` text
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

Como excepción, si convierte de tipos numéricos UInt32, Int32, UInt64 o Int64 a Date, y si el número es mayor o igual que 65536, el número se interpreta como una marca de tiempo Unix (y no como el número de días) y se redondea a la fecha. Esto permite soporte para la ocurrencia común de la escritura ‘toDate(unix\_timestamp)’, que de otra manera sería un error y requeriría escribir el más engorroso ‘toDate(toDateTime(unix\_timestamp))’.

La conversión entre una fecha y una fecha con la hora se realiza de la manera natural: agregando un tiempo nulo o eliminando el tiempo.

La conversión entre tipos numéricos utiliza las mismas reglas que las asignaciones entre diferentes tipos numéricos en C++.

Además, la función toString del argumento DateTime puede tomar un segundo argumento String que contiene el nombre de la zona horaria. Ejemplo: `Asia/Yekaterinburg` En este caso, la hora se formatea de acuerdo con la zona horaria especificada.

``` sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat
```

``` text
┌───────────now_local─┬─now_yekat───────────┐
│ 2016-06-15 00:11:21 │ 2016-06-15 02:11:21 │
└─────────────────────┴─────────────────────┘
```

Ver también el `toUnixTimestamp` función.

## ¿Qué puedes encontrar en Neodigit) {#tofixedstrings-n}

Convierte un argumento de tipo String en un tipo FixedString(N) (una cadena con longitud fija N). N debe ser una constante.
Si la cadena tiene menos bytes que N, se pasa con bytes nulos a la derecha. Si la cadena tiene más bytes que N, se produce una excepción.

## Todos los derechos reservados.) {#tostringcuttozeros}

Acepta un argumento String o FixedString. Devuelve la cadena con el contenido truncado en el primer byte cero encontrado.

Ejemplo:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut
```

``` text
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut
```

``` text
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## ¿Cómo puedo obtener más información?) {#reinterpretasuint8163264}

## ¿Cómo puedo obtener más información?) {#reinterpretasint8163264}

## ¿Cómo puedo obtener más información?) {#reinterpretasfloat3264}

## reinterpretAsDate {#reinterpretasdate}

## reinterpretAsDateTime {#reinterpretasdatetime}

Estas funciones aceptan una cadena e interpretan los bytes colocados al principio de la cadena como un número en orden de host (little endian). Si la cadena no es lo suficientemente larga, las funciones funcionan como si la cadena estuviera rellenada con el número necesario de bytes nulos. Si la cadena es más larga de lo necesario, se ignoran los bytes adicionales. Una fecha se interpreta como el número de días desde el comienzo de la época Unix, y una fecha con hora se interpreta como el número de segundos desde el comienzo de la época Unix.

## reinterpretAsString {#type-conversion-functions-reinterpretasstring}

Esta función acepta un número o fecha o fecha con hora, y devuelve una cadena que contiene bytes que representan el valor correspondiente en orden de host (little endian). Los bytes nulos se eliminan desde el final. Por ejemplo, un valor de tipo UInt32 de 255 es una cadena que tiene un byte de longitud.

## reinterpretAsFixedString {#reinterpretasfixedstring}

Esta función acepta un número o fecha o fecha con hora, y devuelve un FixedString que contiene bytes que representan el valor correspondiente en orden de host (little endian). Los bytes nulos se eliminan desde el final. Por ejemplo, un valor de tipo UInt32 de 255 es un FixedString que tiene un byte de longitud.

## CAST(x, t) {#type-conversion-function-cast}

Convertir ‘x’ a la ‘t’ tipo de datos. La sintaxis CAST(x AS t) también es compatible.

Ejemplo:

``` sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string
```

``` text
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

La conversión a FixedString(N) solo funciona para argumentos de tipo String o FixedString(N).

Conversión de tipo a [NULL](../../data_types/nullable.md) y la espalda es compatible. Ejemplo:

``` sql
SELECT toTypeName(x) FROM t_null
```

``` text
┌─toTypeName(x)─┐
│ Int8          │
│ Int8          │
└───────────────┘
```

``` sql
SELECT toTypeName(CAST(x, 'Nullable(UInt16)')) FROM t_null
```

``` text
┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

## toInterval(Year\|Quarter\|Month\|Week\|Day\|Hour\|Minute\|Second) {#function-tointerval}

Convierte un argumento de tipo Number en un [Intervalo](../../data_types/special_data_types/interval.md) tipo de datos.

**Sintaxis**

``` sql
toIntervalSecond(number)
toIntervalMinute(number)
toIntervalHour(number)
toIntervalDay(number)
toIntervalWeek(number)
toIntervalMonth(number)
toIntervalQuarter(number)
toIntervalYear(number)
```

**Parámetros**

-   `number` — Duración del intervalo. Número entero positivo.

**Valores devueltos**

-   El valor en `Interval` tipo de datos.

**Ejemplo**

``` sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week
```

``` text
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│                2019-01-08 │                   2019-01-08 │
└───────────────────────────┴──────────────────────────────┘
```

## parseDateTimeBestEffort {#type-conversion-functions-parsedatetimebesteffort}

Analice un argumento de tipo numérico a un tipo Date o DateTime.
diferente de toDate y toDateTime, parseDateTimeBestEffort puede progresar en un formato de fecha más complejo.
Para obtener más información, consulte el enlace: [Formato de fecha compleja](https://xkcd.com/1179/)

## parseDateTimeBestEffortOrNull {#parsedatetimebesteffortornull}

Lo mismo que para [parseDateTimeBestEffort](#type_conversion_functions-parsedatetimebesteffort) excepto que devuelve null cuando encuentra un formato de fecha que no se puede procesar.

## parseDateTimeBestEffortOrZero {#parsedatetimebesteffortorzero}

Lo mismo que para [parseDateTimeBestEffort](#type_conversion_functions-parsedatetimebesteffort) excepto que devuelve una fecha cero o una fecha cero cuando encuentra un formato de fecha que no se puede procesar.

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/type_conversion_functions/) <!--hide-->
