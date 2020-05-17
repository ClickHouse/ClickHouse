---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "Codificaci\xF3n"
---

# Funciones de codificación {#encoding-functions}

## char {#char}

Devuelve la cadena con la longitud como el número de argumentos pasados y cada byte tiene el valor del argumento correspondiente. Acepta varios argumentos de tipos numéricos. Si el valor del argumento está fuera del rango del tipo de datos UInt8, se convierte a UInt8 con posible redondeo y desbordamiento.

**Sintaxis**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**Parámetros**

-   `number_1, number_2, ..., number_n` — Numerical arguments interpreted as integers. Types: [En](../../sql-reference/data-types/int-uint.md), [Flotante](../../sql-reference/data-types/float.md).

**Valor devuelto**

-   una cadena de bytes.

Tipo: `String`.

**Ejemplo**

Consulta:

``` sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello
```

Resultado:

``` text
┌─hello─┐
│ hello │
└───────┘
```

Puede construir una cadena de codificación arbitraria pasando los bytes correspondientes. Aquí hay un ejemplo para UTF-8:

Consulta:

``` sql
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
```

Resultado:

``` text
┌─hello──┐
│ привет │
└────────┘
```

Consulta:

``` sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

Resultado:

``` text
┌─hello─┐
│ 你好  │
└───────┘
```

## hexagonal {#hex}

Devuelve una cadena que contiene la representación hexadecimal del argumento.

**Sintaxis**

``` sql
hex(arg)
```

La función está usando letras mayúsculas `A-F` y no usar ningún prefijo (como `0x`) o sufijos (como `h`).

Para argumentos enteros, imprime dígitos hexadecimales (“nibbles”) del más significativo al menos significativo (big endian o “human readable” orden). Comienza con el byte distinto de cero más significativo (se omiten los cero bytes principales) pero siempre imprime ambos dígitos de cada byte incluso si el dígito inicial es cero.

Ejemplo:

**Ejemplo**

Consulta:

``` sql
SELECT hex(1);
```

Resultado:

``` text
01
```

Valores de tipo `Date` y `DateTime` están formateados como enteros correspondientes (el número de días desde Epoch para Date y el valor de Unix Timestamp para DateTime).

Para `String` y `FixedString`, todos los bytes son simplemente codificados como dos números hexadecimales. No se omiten cero bytes.

Los valores de los tipos de coma flotante y Decimal se codifican como su representación en la memoria. Como apoyamos la pequeña arquitectura endian, están codificados en little endian. No se omiten cero bytes iniciales / finales.

**Parámetros**

-   `arg` — A value to convert to hexadecimal. Types: [Cadena](../../sql-reference/data-types/string.md), [UInt](../../sql-reference/data-types/int-uint.md), [Flotante](../../sql-reference/data-types/float.md), [Decimal](../../sql-reference/data-types/decimal.md), [Fecha](../../sql-reference/data-types/date.md) o [FechaHora](../../sql-reference/data-types/datetime.md).

**Valor devuelto**

-   Una cadena con la representación hexadecimal del argumento.

Tipo: `String`.

**Ejemplo**

Consulta:

``` sql
SELECT hex(toFloat32(number)) as hex_presentation FROM numbers(15, 2);
```

Resultado:

``` text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

Consulta:

``` sql
SELECT hex(toFloat64(number)) as hex_presentation FROM numbers(15, 2);
```

Resultado:

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

## unhex(str) {#unhexstr}

Acepta una cadena que contiene cualquier número de dígitos hexadecimales y devuelve una cadena que contiene los bytes correspondientes. Admite letras mayúsculas y minúsculas A-F. El número de dígitos hexadecimales no tiene que ser par. Si es impar, el último dígito se interpreta como la mitad menos significativa del byte 00-0F. Si la cadena de argumento contiene algo que no sean dígitos hexadecimales, se devuelve algún resultado definido por la implementación (no se produce una excepción).
Si desea convertir el resultado en un número, puede usar el ‘reverse’ y ‘reinterpretAsType’ función.

## UUIDStringToNum (str) {#uuidstringtonumstr}

Acepta una cadena que contiene 36 caracteres en el formato `123e4567-e89b-12d3-a456-426655440000`, y lo devuelve como un conjunto de bytes en un FixedString(16).

## UUIDNumToString (str) {#uuidnumtostringstr}

Acepta un valor de FixedString(16). Devuelve una cadena que contiene 36 caracteres en formato de texto.

## ¿Cómo puedo hacerlo?) {#bitmasktolistnum}

Acepta un entero. Devuelve una cadena que contiene la lista de potencias de dos que suman el número de origen cuando se suma. Están separados por comas sin espacios en formato de texto, en orden ascendente.

## ¿Qué puedes encontrar en Neodigit) {#bitmasktoarraynum}

Acepta un entero. Devuelve una matriz de números UInt64 que contiene la lista de potencias de dos que suman el número de origen cuando se suma. Los números en la matriz están en orden ascendente.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/encoding_functions/) <!--hide-->
