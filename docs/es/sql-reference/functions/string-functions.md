---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: Trabajar con cadenas
---

# Funciones para trabajar con cadenas {#functions-for-working-with-strings}

## vaciar {#empty}

Devuelve 1 para una cadena vacía o 0 para una cadena no vacía.
El tipo de resultado es UInt8.
Una cadena se considera no vacía si contiene al menos un byte, incluso si se trata de un espacio o un byte nulo.
La función también funciona para matrices.

## notEmpty {#notempty}

Devuelve 0 para una cadena vacía o 1 para una cadena no vacía.
El tipo de resultado es UInt8.
La función también funciona para matrices.

## longitud {#length}

Devuelve la longitud de una cadena en bytes (no en caracteres y no en puntos de código).
El tipo de resultado es UInt64.
La función también funciona para matrices.

## longitudUTF8 {#lengthutf8}

Devuelve la longitud de una cadena en puntos de código Unicode (no en caracteres), suponiendo que la cadena contiene un conjunto de bytes que componen texto codificado en UTF-8. Si no se cumple esta suposición, devuelve algún resultado (no arroja una excepción).
El tipo de resultado es UInt64.

## char\_length, CHAR\_LENGTH {#char-length}

Devuelve la longitud de una cadena en puntos de código Unicode (no en caracteres), suponiendo que la cadena contiene un conjunto de bytes que componen texto codificado en UTF-8. Si no se cumple esta suposición, devuelve algún resultado (no arroja una excepción).
El tipo de resultado es UInt64.

## character\_length, CHARACTER\_LENGTH {#character-length}

Devuelve la longitud de una cadena en puntos de código Unicode (no en caracteres), suponiendo que la cadena contiene un conjunto de bytes que componen texto codificado en UTF-8. Si no se cumple esta suposición, devuelve algún resultado (no arroja una excepción).
El tipo de resultado es UInt64.

## inferior, lcase {#lower}

Convierte símbolos latinos ASCII en una cadena a minúsculas.

## superior, ucase {#upper}

Convierte los símbolos latinos ASCII en una cadena a mayúsculas.

## Método de codificación de datos: {#lowerutf8}

Convierte una cadena en minúsculas, suponiendo que la cadena contiene un conjunto de bytes que componen un texto codificado en UTF-8.
No detecta el idioma. Entonces, para el turco, el resultado podría no ser exactamente correcto.
Si la longitud de la secuencia de bytes UTF-8 es diferente para mayúsculas y minúsculas de un punto de código, el resultado puede ser incorrecto para este punto de código.
Si la cadena contiene un conjunto de bytes que no es UTF-8, entonces el comportamiento no está definido.

## superiorUTF8 {#upperutf8}

Convierte una cadena en mayúsculas, suponiendo que la cadena contiene un conjunto de bytes que componen un texto codificado en UTF-8.
No detecta el idioma. Entonces, para el turco, el resultado podría no ser exactamente correcto.
Si la longitud de la secuencia de bytes UTF-8 es diferente para mayúsculas y minúsculas de un punto de código, el resultado puede ser incorrecto para este punto de código.
Si la cadena contiene un conjunto de bytes que no es UTF-8, entonces el comportamiento no está definido.

## Sistema abierto {#isvalidutf8}

Devuelve 1, si el conjunto de bytes es válido codificado en UTF-8, de lo contrario 0.

## Acerca de Nosotros {#tovalidutf8}

Reemplaza los caracteres UTF-8 no válidos por `�` (U+FFFD) carácter. Todos los caracteres no válidos que se ejecutan en una fila se contraen en el único carácter de reemplazo.

``` sql
toValidUTF8( input_string )
```

Parámetros:

-   input\_string — Any set of bytes represented as the [Cadena](../../sql-reference/data-types/string.md) objeto de tipo de datos.

Valor devuelto: cadena UTF-8 válida.

**Ejemplo**

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b')
```

``` text
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## repetir {#repeat}

Repite una cadena tantas veces como se especifique y concatena los valores replicados como una única cadena.

**Sintaxis**

``` sql
repeat(s, n)
```

**Parámetros**

-   `s` — The string to repeat. [Cadena](../../sql-reference/data-types/string.md).
-   `n` — The number of times to repeat the string. [UInt](../../sql-reference/data-types/int-uint.md).

**Valor devuelto**

La cadena única, que contiene la cadena `s` repetir `n` tiempo. Si `n` \< 1, la función devuelve cadena vacía.

Tipo: `String`.

**Ejemplo**

Consulta:

``` sql
SELECT repeat('abc', 10)
```

Resultado:

``` text
┌─repeat('abc', 10)──────────────┐
│ abcabcabcabcabcabcabcabcabcabc │
└────────────────────────────────┘
```

## inverso {#reverse}

Invierte la cadena (como una secuencia de bytes).

## reverseUTF8 {#reverseutf8}

Invierte una secuencia de puntos de código Unicode, suponiendo que la cadena contiene un conjunto de bytes que representan un texto UTF-8. De lo contrario, hace otra cosa (no arroja una excepción).

## format(pattern, s0, s1, …) {#format}

Formatear el patrón constante con la cadena enumerada en los argumentos. `pattern` es un patrón de formato de Python simplificado. La cadena de formato contiene “replacement fields” rodeado de llaves `{}`. Cualquier cosa que no esté contenida entre llaves se considera texto literal, que se copia sin cambios en la salida. Si necesita incluir un carácter de llave en el texto literal, se puede escapar duplicando: `{{ '{{' }}` y `{{ '}}' }}`. Los nombres de campo pueden ser números (comenzando desde cero) o vacíos (luego se tratan como números de consecuencia).

``` sql
SELECT format('{1} {0} {1}', 'World', 'Hello')
```

``` text
┌─format('{1} {0} {1}', 'World', 'Hello')─┐
│ Hello World Hello                       │
└─────────────────────────────────────────┘
```

``` sql
SELECT format('{} {}', 'Hello', 'World')
```

``` text
┌─format('{} {}', 'Hello', 'World')─┐
│ Hello World                       │
└───────────────────────────────────┘
```

## concat {#concat}

Concatena las cadenas enumeradas en los argumentos, sin un separador.

**Sintaxis**

``` sql
concat(s1, s2, ...)
```

**Parámetros**

Valores de tipo String o FixedString.

**Valores devueltos**

Devuelve la cadena que resulta de concatenar los argumentos.

Si alguno de los valores de argumento `NULL`, `concat` devoluciones `NULL`.

**Ejemplo**

Consulta:

``` sql
SELECT concat('Hello, ', 'World!')
```

Resultado:

``` text
┌─concat('Hello, ', 'World!')─┐
│ Hello, World!               │
└─────────────────────────────┘
```

## ConcatAssumeInjective {#concatassumeinjective}

Lo mismo que [concat](#concat), la diferencia es que usted necesita asegurar eso `concat(s1, s2, ...) → sn` es inyectivo, se utilizará para la optimización de GROUP BY.

La función se llama “injective” si siempre devuelve un resultado diferente para diferentes valores de argumentos. En otras palabras: diferentes argumentos nunca arrojan un resultado idéntico.

**Sintaxis**

``` sql
concatAssumeInjective(s1, s2, ...)
```

**Parámetros**

Valores de tipo String o FixedString.

**Valores devueltos**

Devuelve la cadena que resulta de concatenar los argumentos.

Si alguno de los valores de argumento `NULL`, `concatAssumeInjective` devoluciones `NULL`.

**Ejemplo**

Tabla de entrada:

``` sql
CREATE TABLE key_val(`key1` String, `key2` String, `value` UInt32) ENGINE = TinyLog;
INSERT INTO key_val VALUES ('Hello, ','World',1), ('Hello, ','World',2), ('Hello, ','World!',3), ('Hello',', World!',2);
SELECT * from key_val;
```

``` text
┌─key1────┬─key2─────┬─value─┐
│ Hello,  │ World    │     1 │
│ Hello,  │ World    │     2 │
│ Hello,  │ World!   │     3 │
│ Hello   │ , World! │     2 │
└─────────┴──────────┴───────┘
```

Consulta:

``` sql
SELECT concat(key1, key2), sum(value) FROM key_val GROUP BY concatAssumeInjective(key1, key2)
```

Resultado:

``` text
┌─concat(key1, key2)─┬─sum(value)─┐
│ Hello, World!      │          3 │
│ Hello, World!      │          2 │
│ Hello, World       │          3 │
└────────────────────┴────────────┘
```

## substring(s, desplazamiento, longitud), mid(s, desplazamiento, longitud), substr(s, desplazamiento, longitud) {#substring}

Devuelve una subcadena que comienza con el byte ‘offset’ índice que es ‘length’ bytes de largo. La indexación de caracteres comienza desde uno (como en SQL estándar). El ‘offset’ y ‘length’ los argumentos deben ser constantes.

## substringUTF8(s, desplazamiento, longitud) {#substringutf8}

Lo mismo que ‘substring’, pero para puntos de código Unicode. Funciona bajo el supuesto de que la cadena contiene un conjunto de bytes que representan un texto codificado en UTF-8. Si no se cumple esta suposición, devuelve algún resultado (no arroja una excepción).

## Aquí hay algunas opciones) {#appendtrailingcharifabsent}

Si el ‘s’ cadena no está vacía y no contiene el ‘c’ carácter al final, se añade el ‘c’ carácter hasta el final.

## convertirCharset(s), de, a) {#convertcharset}

Devuelve la cadena ‘s’ que se convirtió de la codificación en ‘from’ a la codificación en ‘to’.

## Sistema abierto.) {#base64encode}

Codificar ‘s’ cadena en base64

## base64Decode(s)) {#base64decode}

Decodificar cadena codificada en base64 ‘s’ en la cadena original. En caso de fallo plantea una excepción.

## tryBase64Decode(s) {#trybase64decode}

Similar a base64Decode, pero en caso de error se devolverá una cadena vacía.

## endsWith(s, sufijo) {#endswith}

Devuelve si se debe terminar con el sufijo especificado. Devuelve 1 si la cadena termina con el sufijo especificado, de lo contrario devuelve 0.

## startsWith(str, prefijo) {#startswith}

Devuelve 1 si la cadena comienza con el prefijo especificado, de lo contrario devuelve 0.

``` sql
SELECT startsWith('Spider-Man', 'Spi');
```

**Valores devueltos**

-   1, si la cadena comienza con el prefijo especificado.
-   0, si la cadena no comienza con el prefijo especificado.

**Ejemplo**

Consulta:

``` sql
SELECT startsWith('Hello, world!', 'He');
```

Resultado:

``` text
┌─startsWith('Hello, world!', 'He')─┐
│                                 1 │
└───────────────────────────────────┘
```

## recortar {#trim}

Quita todos los caracteres especificados del inicio o el final de una cadena.
De forma predeterminada, elimina todas las apariciones consecutivas de espacios en blanco comunes (carácter ASCII 32) de ambos extremos de una cadena.

**Sintaxis**

``` sql
trim([[LEADING|TRAILING|BOTH] trim_character FROM] input_string)
```

**Parámetros**

-   `trim_character` — specified characters for trim. [Cadena](../../sql-reference/data-types/string.md).
-   `input_string` — string for trim. [Cadena](../../sql-reference/data-types/string.md).

**Valor devuelto**

Una cadena sin caracteres especificados iniciales y (o) finales.

Tipo: `String`.

**Ejemplo**

Consulta:

``` sql
SELECT trim(BOTH ' ()' FROM '(   Hello, world!   )')
```

Resultado:

``` text
┌─trim(BOTH ' ()' FROM '(   Hello, world!   )')─┐
│ Hello, world!                                 │
└───────────────────────────────────────────────┘
```

## trimLeft {#trimleft}

Quita todas las apariciones consecutivas de espacios en blanco comunes (carácter ASCII 32) desde el principio de una cadena. No elimina otros tipos de caracteres de espacios en blanco (tab, espacio sin interrupción, etc.).

**Sintaxis**

``` sql
trimLeft(input_string)
```

Apodo: `ltrim(input_string)`.

**Parámetros**

-   `input_string` — string to trim. [Cadena](../../sql-reference/data-types/string.md).

**Valor devuelto**

Una cadena sin espacios en blanco comunes iniciales.

Tipo: `String`.

**Ejemplo**

Consulta:

``` sql
SELECT trimLeft('     Hello, world!     ')
```

Resultado:

``` text
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## trimRight {#trimright}

Quita todas las apariciones consecutivas de espacios en blanco comunes (carácter ASCII 32) del final de una cadena. No elimina otros tipos de caracteres de espacios en blanco (tab, espacio sin interrupción, etc.).

**Sintaxis**

``` sql
trimRight(input_string)
```

Apodo: `rtrim(input_string)`.

**Parámetros**

-   `input_string` — string to trim. [Cadena](../../sql-reference/data-types/string.md).

**Valor devuelto**

Una cadena sin espacios en blanco comunes finales.

Tipo: `String`.

**Ejemplo**

Consulta:

``` sql
SELECT trimRight('     Hello, world!     ')
```

Resultado:

``` text
┌─trimRight('     Hello, world!     ')─┐
│      Hello, world!                   │
└──────────────────────────────────────┘
```

## AjusteTanto {#trimboth}

Quita todas las apariciones consecutivas de espacios en blanco comunes (carácter ASCII 32) de ambos extremos de una cadena. No elimina otros tipos de caracteres de espacios en blanco (tab, espacio sin interrupción, etc.).

**Sintaxis**

``` sql
trimBoth(input_string)
```

Apodo: `trim(input_string)`.

**Parámetros**

-   `input_string` — string to trim. [Cadena](../../sql-reference/data-types/string.md).

**Valor devuelto**

Una cadena sin espacios en blanco comunes iniciales y finales.

Tipo: `String`.

**Ejemplo**

Consulta:

``` sql
SELECT trimBoth('     Hello, world!     ')
```

Resultado:

``` text
┌─trimBoth('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## CRC32(s)) {#crc32}

Devuelve la suma de comprobación CRC32 de una cadena, utilizando el polinomio CRC-32-IEEE 802.3 y el valor inicial `0xffffffff` (implementación zlib).

El tipo de resultado es UInt32.

## CRC32IEEE(s) {#crc32ieee}

Devuelve la suma de comprobación CRC32 de una cadena, utilizando el polinomio CRC-32-IEEE 802.3.

El tipo de resultado es UInt32.

## CRC64(s)) {#crc64}

Devuelve la suma de comprobación CRC64 de una cadena, utilizando el polinomio CRC-64-ECMA.

El tipo de resultado es UInt64.

[Artículo Original](https://clickhouse.tech/docs/en/query_language/functions/string_functions/) <!--hide-->
