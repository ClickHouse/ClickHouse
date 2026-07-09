---
alias: []
description: 'Documentación sobre el formato RowBinary'
input_format: true
keywords: ['RowBinary']
output_format: true
slug: /interfaces/formats/RowBinary
title: 'RowBinary'
doc_type: 'reference'
---

import RowBinaryFormatSettings from './_snippets/common-row-binary-format-settings.md'

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

El formato `RowBinary` analiza los datos fila por fila en formato binario.
Las filas y los valores aparecen de forma consecutiva, sin separadores.
Como los datos están en formato binario, el delimitador después de `FORMAT RowBinary` se especifica estrictamente de la siguiente manera:

* Cualquier cantidad de espacios en blanco:
  * `' '` (espacio - código `0x20`)
  * `'\t'` (tabulación - código `0x09`)
  * `'\f'` (salto de página - código `0x0C`)
* Seguido de exactamente una secuencia de nueva línea:
  * estilo Windows `"\r\n"`
  * o estilo Unix `'\n'`
* Seguido inmediatamente de datos binarios.

:::note
Este formato es menos eficiente que el formato [Native](../Native.md), ya que se basa en filas.
:::

<div id="data-types-wire-format">
  ## Formato wire de los tipos de datos
</div>

:::tip
La mayoría de las consultas de los ejemplos pueden ejecutarse con `curl` y guardar la salida en un archivo.

```bash
curl -XPOST "http://localhost:8123?default_format=RowBinary" \
  --data-binary "SELECT 42 :: UInt32"  > out.bin
```

:::

Luego, los datos se pueden examinar con un editor hexadecimal.

<div id="unsigned-leb128">
  ### LEB128 sin signo (Base 128 little-endian)
</div>

Una codificación de enteros sin signo de ancho variable en formato **little-endian**, utilizada para codificar la longitud de tipos de datos de tamaño variable como `String`, `Array` y `Map`. Puede encontrarse una implementación de ejemplo en la [página de Wikipedia de LEB128](https://en.wikipedia.org/wiki/LEB128#Decode_unsigned_integer).

<div id="integer-types">
  ### (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
</div>

Todos los tipos enteros se codifican con un número adecuado de bytes en formato **little-endian**. Los tipos con signo (`Int8` a `Int256`) usan la representación en **complemento a dos**. La mayoría de los lenguajes permiten extraer estos enteros de arrays de bytes, ya sea con herramientas integradas o bibliotecas conocidas. Para `Int128`/`Int256` y `UInt128`/`UInt256`, que superan el tamaño de entero nativo de la mayoría de los lenguajes, puede ser necesaria una deserialización personalizada.

<div id="bool">
  ### Bool
</div>

Los valores booleanos se codifican en un solo byte y pueden deserializarse de forma similar a `UInt8`.

* `0` es `false`
* `1` es `true`

<div id="float32-float64">
  ### Float32, Float64
</div>

Números de coma flotante **little-endian** codificados en 4 bytes para `Float32` y en 8 bytes para `Float64`. Al igual que con los enteros, la mayoría de los lenguajes ofrecen herramientas adecuadas para deserializar estos valores.

<div id="bfloat16">
  ### BFloat16
</div>

[BFloat16](https://clickhouse.com/docs/sql-reference/data-types/float#bfloat16) (Brain Floating Point) es un formato de coma flotante de 16 bits con el rango de Float32, pero con menor precisión, lo que lo hace útil para cargas de trabajo de aprendizaje automático. El formato wire es, esencialmente, los 16 bits más significativos de un valor Float32. Si su lenguaje no lo admite de forma nativa, la forma más sencilla de manejarlo es leerlo y escribirlo como UInt16, convirtiéndolo desde y hacia Float32:

Para convertir BFloat16 a Float32 (pseudocódigo):

```text
// Read 2 bytes as little-endian UInt16
// Left-shift by 16 bits to get Float32 bits
bfloat16Bits = readUInt16()
float32Bits = bfloat16Bits << 16
floatValue = reinterpretAsFloat32(float32Bits)
```

Para convertir Float32 a BFloat16 (pseudocódigo):

```text
// Right-shift Float32 bits by 16 to truncate to BFloat16
float32Bits = reinterpretAsUInt32(floatValue)
bfloat16Bits = float32Bits >> 16
writeUInt16(bfloat16Bits)
```

Ejemplos de valores subyacentes para `BFloat16`:

```sql
SELECT CAST(1.25, 'BFloat16')
```

```text
0xA0, 0x3F, // 1.25 as BFloat16
```

<div id="decimal">
  ### Decimal32, Decimal64, Decimal128, Decimal256
</div>

Los tipos Decimal se representan como enteros **little-endian** con el ancho de bits correspondiente.

* `Decimal32` - 4 bytes, o `Int32`.
* `Decimal64` - 8 bytes, o `Int64`.
* `Decimal128` - 16 bytes, o `Int128`.
* `Decimal256` - 32 bytes, o `Int256`.

Al deserializar un valor Decimal, las partes entera y fraccionaria se pueden obtener mediante el siguiente pseudocódigo:

```text
let scale_multiplier = 10 ** scale
let whole_part = trunc(value / scale_multiplier)  // truncate toward zero
let fractional_part = value % scale_multiplier
let result = Decimal(whole_part, fractional_part)
```

Donde `trunc` realiza el truncamiento hacia cero (no la división redondeada hacia abajo, que difiere para los valores negativos), y `scale` es el número de dígitos después del punto decimal. Por ejemplo, para `Decimal(10, 2)` (equivalente a `Decimal32(2)`), la escala es `2` y el valor `12345` se representará como `(123, 45)`.

La serialización requiere la operación inversa:

```text
let scale_multiplier = 10 ** scale
let result = whole_part * scale_multiplier + fractional_part
```

Consulta más detalles en la [documentación de ClickHouse sobre tipos Decimal](https://clickhouse.com/docs/sql-reference/data-types/decimal).

<div id="string">
  ### String
</div>

Las cadenas de ClickHouse son **secuencias arbitrarias de bytes**. No es necesario que sean UTF-8 válidas. El prefijo de longitud es la **longitud en bytes**, no el número de caracteres.

Se codifican en dos partes:

1. Un entero de longitud variable (LEB128) que indica la longitud de la cadena en bytes.
2. Los bytes sin procesar de la cadena.

Por ejemplo, una cadena `foobar` se codificará usando *siete* bytes de la siguiente manera:

```text
0x06, // LEB128 length of the string (6)
0x66, // 'f'
0x6f, // 'o'
0x6f, // 'o'
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

<div id="fixedstring">
  ### FixedString
</div>

A diferencia de `String`, `FixedString` tiene una longitud fija, definida en el esquema. Se codifica como una secuencia de bytes, rellenada con bytes cero al final si el valor es más corto que `N`.

:::note
Al leer un `FixedString`, los bytes cero finales pueden ser relleno o caracteres `\0` reales de los datos; son indistinguibles on the wire. El propio ClickHouse conserva los `N` bytes tal cual.
:::

Un `FixedString(3)` vacío contiene únicamente ceros de relleno:

```text
0x00, 0x00, 0x00
```

Un `FixedString(3)` no vacío que contiene la cadena `hi`:

```text
0x68, // 'h'
0x69, // 'i'
0x00, // padding zero
```

`FixedString(3)` no vacía que contiene la cadena `bar`:

```text
0x62, // 'b'
0x61, // 'a'
0x72, // 'r'
```

No se requiere relleno en el último ejemplo, ya que se utilizan los *tres* bytes.

<div id="date">
  ### Date
</div>

Se almacena como `UInt16` (dos bytes) y representa el número de días transcurridos ***desde*** `1970-01-01`.

Rango de valores admitidos: `[1970-01-01, 2149-06-06]`.

Valores subyacentes de ejemplo para `Date`:

```sql
SELECT CAST('2024-01-15', 'Date') AS d
```

```text
0x19, 0x4D, // 19737 as UInt16 (little-endian) = 19737 days since 1970-01-01
```

<div id="date32">
  ### Date32
</div>

Se almacena como `Int32` (cuatro bytes), que representa el número de días ***antes o después*** de `1970-01-01`.

Rango de valores admitido: `[1900-01-01, 2299-12-31]`.

Ejemplos de valores subyacentes para `Date32`:

```sql
SELECT CAST('2024-01-15', 'Date32') AS d
```

```text
0x19, 0x4D, 0x00, 0x00, // 19737 as Int32 (little-endian) = 19737 days since 1970-01-01
```

Una fecha anterior al epoch:

```sql
SELECT CAST('1900-01-01', 'Date32') AS d
```

```text
0x21, 0x9C, 0xFF, 0xFF, // -25567 as Int32 (little-endian) = 25567 days before 1970-01-01
```

<div id="datetime">
  ### DateTime
</div>

Se almacena como `UInt32` (cuatro bytes), que representa el número de segundos transcurridos ***desde*** `1970-01-01 00:00:00 UTC`.

Sintaxis:

```text
DateTime([timezone])
```

Por ejemplo, `DateTime` o `DateTime('UTC')`.

:::note
El valor binario siempre es un desplazamiento respecto del epoch en UTC. La zona horaria no cambia la codificación. Sin embargo, la zona horaria **sí** afecta a cómo se interpretan los valores de cadena al insertar: insertar `'2024-01-15 10:30:00'` en una columna `DateTime('America/New_York')` almacena un valor de epoch distinto de insertar la misma cadena en una columna `DateTime('UTC')`, porque la cadena se interpreta como hora local en la zona horaria de la columna. A nivel de transmisión, ambos son simplemente segundos de epoch `UInt32`.
:::

Rango de valores admitido: `[1970-01-01 00:00:00, 2106-02-07 06:28:15]`.

Valores subyacentes de ejemplo para `DateTime`:

```sql
SELECT CAST('2024-01-15 10:30:00', 'DateTime(\'UTC\')') AS d
```

```text
0x28, 0x09, 0xA5, 0x65, // 1705314600 as UInt32 (little-endian)
```

<div id="datetime64">
  ### DateTime64
</div>

Se almacena como `Int64` (ocho bytes) y representa el número de **ticks** ***anteriores o posteriores*** a `1970-01-01 00:00:00 UTC`. La resolución del tick se define mediante el parámetro `precision`; consulte la sintaxis a continuación:

```text
DateTime64(precision, [timezone])
```

Donde `precision` es un entero entre `0` y `9`. Normalmente, solo se usan estos valores: `3` (milisegundos), `6` (microsegundos),
`9` (nanosegundos).

Ejemplos de definiciones válidas de DateTime64: `DateTime64(0)`, `DateTime64(3)`, `DateTime64(6, 'UTC')` o `DateTime64(9, 'Europe/Amsterdam')`.

:::note
Al igual que con `DateTime`, el valor binario siempre es un desplazamiento respecto de la época UTC. La zona horaria afecta a cómo se interpretan los valores de cadena al insertarlos (consulta la nota de [DateTime](#datetime)), pero la codificación en sí siempre son ticks `Int64` desde la época UTC.
:::

El valor subyacente `Int64` del tipo `DateTime64` puede interpretarse como la cantidad de las siguientes unidades antes o después de la época Unix:

* `DateTime64(0)` - segundos.
* `DateTime64(3)` - milisegundos.
* `DateTime64(6)` - microsegundos.
* `DateTime64(9)` - nanosegundos.

Rango de valores admitido: `[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999]`.

Valores subyacentes de ejemplo para `DateTime64`:

* `DateTime64(3)`: el valor `1546300800000` representa `2019-01-01 00:00:00 UTC`.
* `DateTime64(6)`: el valor `1705314600123456` representa `2024-01-15 10:30:00.123456 UTC`.
* `DateTime64(9)`: el valor `1705314600123456789` representa `2024-01-15 10:30:00.123456789 UTC`.

:::note
La precisión del valor máximo es 8. Si se usa la precisión máxima de 9 dígitos (nanosegundos), el valor máximo admitido es 2262-04-11 23:47:16 en UTC.
:::

<div id="time">
  ### Time
</div>

Se almacena como `Int32`, que representa un valor temporal en segundos. Los valores negativos son válidos.

Rango de valores admitidos: `[-999:59:59, 999:59:59]` (es decir, `[-3599999, 3599999]` segundos).

:::note
Por el momento, el ajuste `enable_time_time64_type` debe establecerse en `1` para usar `Time` o `Time64`.
:::

Ejemplos de valores subyacentes para `Time`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16', 'Time') AS t
```

```text
0x80, 0xDA, 0x00, 0x00, // 55936 seconds = 15:32:16
```

<div id="time64">
  ### Time64
</div>

Se almacena internamente como un `Decimal64` (que a su vez se almacena como `Int64`) y representa un valor de tiempo con fracciones de segundo, con precisión configurable. Los valores negativos son válidos.

Sintaxis:

```text
Time64(precision)
```

Donde `precision` es un número entero de `0` a `9`. Valores habituales: `3` (milisegundos), `6` (microsegundos), `9` (nanosegundos).

Rango de valores admitido: `[-999:59:59.xxxxxxxxx, 999:59:59.xxxxxxxxx]`.

:::note
Por el momento, la configuración `enable_time_time64_type` debe establecerse en `1` para poder usar `Time` o `Time64`.
:::

El valor `Int64` subyacente representa segundos fraccionarios escalados por `10^precision`.

Valores subyacentes de ejemplo para `Time64`:

```sql
SET enable_time_time64_type = 1;
SELECT CAST('15:32:16.123456', 'Time64(6)') AS t
```

```text
0x40, 0x82, 0x0D, 0x06,
0x0D, 0x00, 0x00, 0x00, // 55936123456 as Int64
// 55936123456 / 10^6 = 55936.123456 seconds = 15:32:16.123456
```

<div id="interval-types">
  ### Tipos Interval
</div>

Todos los tipos Interval se almacenan como `Int64` (ocho bytes, little-endian). El valor representa la cantidad de la unidad de tiempo correspondiente. Los valores negativos son válidos.

Los tipos Interval son: `IntervalNanosecond`, `IntervalMicrosecond`, `IntervalMillisecond`, `IntervalSecond`, `IntervalMinute`, `IntervalHour`, `IntervalDay`, `IntervalWeek`, `IntervalMonth`, `IntervalQuarter`, `IntervalYear`.

:::note
El nombre del tipo Interval (por ejemplo, `IntervalSecond` frente a `IntervalDay`) determina la unidad del valor almacenado. La codificación wire siempre es la misma.
:::

Valores subyacentes de ejemplo:

```sql
SELECT INTERVAL 5 SECOND   AS a,
     INTERVAL 10 DAY     AS b,
     INTERVAL -7 DAY     AS c,
     INTERVAL 3 YEAR     AS d,
     INTERVAL 500 MICROSECOND AS e
```

```text
// IntervalSecond: 5
0x05, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: 10
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalDay: -7
0xF9, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
// IntervalYear: 3
0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
// IntervalMicrosecond: 500
0xF4, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

<div id="enum8-enum16">
  ### Enum8, Enum16
</div>

Se almacena como un solo byte (`Enum8` == `Int8`) o dos bytes (`Enum16` == `Int16`) que representan el índice del valor del enum en su definición. Ten en cuenta que el tipo de almacenamiento es **con signo**: los valores del enum pueden ser negativos (p. ej., `Enum8('a' = -128, 'b' = 0)`).

Un Enum puede definirse de forma sencilla, así:

```sql
SELECT 1 :: Enum8('hello' = 1, 'world' = 2) AS e;
```

```text
   ┌─e─────┐
1. │ hello │
   └───────┘
```

El Enum8 definido anteriormente tendrá la siguiente asignación de valores en el client:

```text
Map<Int8, String> {
  1: 'hello',
  2: 'world'
}
```

O, de forma más compleja, así:

```sql
SELECT 42 :: Enum16('f\'' = 1, 'x =' = 2, 'b\'\'' = 3, '\'c=4=' = 42, '4' = 1234) AS e;
```

```text
   ┌─e─────┐
1. │ 'c=4= │
   └───────┘
```

El Enum16 definido anteriormente tendrá la siguiente asignación de valores en el client:

```text
Map<Int16, String> {
  1:    'f\'',
  2:    'x =',
  3:    'b\'',
  42:   '\'c=4=',
  1234: '4'
}
```

En el analizador de tipos de datos, el principal desafío es identificar los símbolos escapados en la definición de `enum`, como `\'`, y los símbolos especiales como `=` que pueden aparecer dentro de cadenas entre comillas.

<div id="uuid">
  ### UUID
</div>

Se representa como una secuencia de 16 bytes. El UUID se almacena como **dos valores `UInt64` little-endian**: los primeros 8 bytes de la representación estándar del UUID tienen sus bytes en orden inverso, y los segundos 8 bytes también tienen sus bytes en orden inverso de forma independiente.

Por ejemplo, dado el UUID `61f0c404-5cb3-11e7-907b-a6006ad3dba0`:

* Representación estándar en bytes: `61 f0 c4 04 5c b3 11 e7` | `90 7b a6 00 6a d3 db a0`
* Primera mitad invertida (LE UInt64): `e7 11 b3 5c 04 c4 f0 61`
* Segunda mitad invertida (LE UInt64): `a0 db d3 6a 00 a6 7b 90`

Valores subyacentes de ejemplo para `UUID`:

* `61f0c404-5cb3-11e7-907b-a6006ad3dba0` se representa como:

```text
0xE7, 0x11, 0xB3, 0x5C, 0x04, 0xC4, 0xF0, 0x61,
0xA0, 0xDB, 0xD3, 0x6A, 0x00, 0xA6, 0x7B, 0x90,
```

* El UUID por defecto `00000000-0000-0000-0000-000000000000` se representa como 16 bytes con valor cero:

```text
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
```

Se puede usar cuando se ha insertado un nuevo registro, pero no se ha especificado el valor UUID.

<div id="ipv4">
  ### IPv4
</div>

Se almacena en cuatro bytes como `UInt32` en orden de bytes **little-endian**. Tenga en cuenta que esto difiere del orden de bytes de red tradicional (big-endian), usado habitualmente para las direcciones IP. Valores subyacentes de ejemplo para `IPv4`:

```sql
SELECT    
  CAST('0.0.0.0',         'IPv4') AS a,
  CAST('127.0.0.1',       'IPv4') AS b,
  CAST('192.168.0.1',     'IPv4') AS c,
  CAST('255.255.255.255', 'IPv4') AS d,
  CAST('168.212.226.204', 'IPv4') AS e
```

```text
0x00, 0x00, 0x00, 0x00, // 0.0.0.0
0x01, 0x00, 0x00, 0x7f, // 127.0.0.1
0x01, 0x00, 0xa8, 0xc0, // 192.168.0.1
0xff, 0xff, 0xff, 0xff, // 255.255.255.255
0xcc, 0xe2, 0xd4, 0xa8, // 168.212.226.204
```

<div id="ipv6">
  ### IPv6
</div>

Se almacena en 16 bytes en **orden big-endian / de bytes de red** (MSB primero). Ejemplos de valores subyacentes para `IPv6`:

```sql
SELECT
    CAST('2a02:aa08:e000:3100::2',        'IPv6') AS a,
    CAST('2001:44c8:129:2632:33:0:252:2', 'IPv6') AS b,
    CAST('2a02:e980:1e::1',               'IPv6') AS c
```

```text
// 2a02:aa08:e000:3100::2
0x2A, 0x02, 0xAA, 0x08, 0xE0, 0x00, 0x31, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
// 2001:44c8:129:2632:33:0:252:2
0x20, 0x01, 0x44, 0xC8, 0x01, 0x29, 0x26, 0x32, 
0x00, 0x33, 0x00, 0x00, 0x02, 0x52, 0x00, 0x02,
// 2a02:e980:1e::1
0x2A, 0x02, 0xE9, 0x80, 0x00, 0x1E, 0x00, 0x00, 
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
```

<div id="nullable">
  ### Nullable
</div>

Un tipo de dato Nullable se codifica de la siguiente manera:

1. Un único byte que indica si el valor es `NULL` o no:
   * `0x00` significa que el valor no es `NULL`.
   * `0x01` significa que el valor es `NULL`.
2. Si el valor no es `NULL`, el tipo de dato subyacente se codifica de forma habitual. Si el valor es `NULL`, **no se escribe ningún byte adicional** para el tipo subyacente.

Por ejemplo, un valor `Nullable(UInt32)`:

```sql
SELECT    
   CAST(42,   'Nullable(UInt32)') AS a,
   CAST(NULL, 'Nullable(UInt32)') AS b
```

```text
0x00,                   // Not NULL - the value follows
0x2A, 0x00, 0x00, 0x00, // UInt32(42)
0x01,                   // NULL - nothing follows
```

<div id="lowcardinality">
  ### LowCardinality
</div>

En el formato RowBinary, el marcador de baja cardinalidad no afecta al formato wire. Por ejemplo, `LowCardinality(String)` se codifica de la misma forma que un `String` normal.

:::warning
Esto solo se aplica a RowBinary. En el formato Native, `LowCardinality` usa una codificación diferente basada en diccionarios.
:::

:::note
Una columna puede definirse como `LowCardinality(Nullable(T))`, pero no es posible definirla como `Nullable(LowCardinality(T))`; siempre dará como resultado un error del servidor.
:::

Durante las pruebas, [allow&#95;suspicious&#95;low&#95;cardinality&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_low_cardinality_types) puede establecerse en `1` para permitir la mayoría de los tipos de datos dentro de `LowCardinality` y así lograr una mejor cobertura.

<div id="array">
  ### Array
</div>

Un array se codifica de la siguiente manera:

1. Un [entero de longitud variable (LEB128)](#unsigned-leb128) que indica el número de elementos del array.
2. Los elementos del array, codificados de la misma forma que el tipo de dato subyacente.

Por ejemplo, un array con valores `UInt32`:

```sql
SELECT CAST(array(1, 2, 3), 'Array(UInt32)') AS arr
```

```text
0x03,                   // LEB128 - the array has 3 elements
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x02, 0x00, 0x00, 0x00, // UInt32(2)
0x03, 0x00, 0x00, 0x00, // UInt32(3)
```

Un ejemplo un poco más complejo:

```sql
SELECT array('foobar', 'qaz') AS arr
```

```text
0x02,             // LEB128 - the array has 2 elements
0x06,             // LEB128 - the first string has 6 bytes
0x66, 0x6f, 0x6f, 
0x62, 0x61, 0x72, // 'foobar'
0x03,             // LEB128 - the second string has 3 bytes
0x71, 0x61, 0x7a, // 'qaz'
```

:::note
Un array puede contener valores Nullable, pero el propio array no puede ser Nullable.
:::

Lo siguiente es válido:

```sql
SELECT CAST([NULL, 'foo'], 'Array(Nullable(String))') AS arr;
```

```text
   ┌─arr──────────┐
1. │ [NULL,'foo'] │
   └──────────────┘
```

Y se codificará de la siguiente forma:

```text
0x02,             // LEB128  - the array has 2 elements
0x01,             // Is NULL - nothing follows for this element
0x00,             // Is NOT NULL - the data follows
0x03,             // LEB128  - the string has 3 bytes
0x66, 0x6f, 0x6f, // 'foo'
```

Puede encontrar un ejemplo de cómo trabajar con arrays multidimensionales en la [sección Geo](#geo-types).

<div id="tuple">
  ### Tuple
</div>

Una tupla se codifica como todos los elementos de la tupla, uno a continuación de otro, en el formato wire correspondiente, sin metainformación ni delimitadores adicionales.

```sql
CREATE OR REPLACE TABLE foo
(
    `t` Tuple(
           UInt32,
           String,
           Array(UInt8)
        )
)
ENGINE = Memory;
INSERT INTO foo VALUES ((42, 'foo', array(99, 144)));
```

```text
0x2a, 0x00, 0x00, 0x00, // 42 as UInt32
0x03,                   // LEB128 - the string has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x02,                   // LEB128 - the array has 2 elements
0x63,                   // 99 as UInt8
0x90,                   // 144 as UInt8
```

La representación en cadena del tipo de datos Tuple presenta desafíos similares a los del [tipo Enum](#enum8-enum16), como el seguimiento de los símbolos de escape y los caracteres especiales; con Tuple, además, también es necesario controlar los paréntesis de apertura y cierre. Además, tenga en cuenta que los Tuples más complejos pueden contener otros Tuples anidados, Arrays, Maps e incluso enums.

Por ejemplo, en la siguiente tabla, el Tuple contiene un enum con una comilla simple y un paréntesis en el nombre, lo que puede provocar problemas de análisis si no se maneja correctamente:

```sql
CREATE OR REPLACE TABLE foo
(
   `t` Tuple(
          Enum8('f\'()' = 0),
          Array(Nullable(Tuple(UInt32, String)))
       )
) ENGINE = Memory;
```

<div id="map">
  ### Map
</div>

Un mapa puede representarse como un `Array(Tuple(K, V))`, donde `K` es el tipo de clave y `V` es el tipo de valor. El mapa se codifica de la siguiente manera:

1. Un [entero de longitud variable (LEB128)](#unsigned-leb128) que indica el número de elementos del mapa.
2. Los elementos del mapa como pares clave-valor, codificados según sus tipos correspondientes.

Por ejemplo, un mapa con claves `String` y valores `UInt32`:

```sql
SELECT CAST(map('foo', 1, 'bar', 2), 'Map(String, UInt32)') AS m
```

```text
0x02,                   // LEB128 - the map has 2 elements
0x03,                   // LEB128 - the first key has 3 bytes
0x66, 0x6f, 0x6f,       // 'foo'
0x01, 0x00, 0x00, 0x00, // UInt32(1)
0x03,                   // LEB128 - the second key has 3 bytes
0x62, 0x61, 0x72,       // 'bar'
0x02, 0x00, 0x00, 0x00, // UInt32(2)
```

:::note
Es posible tener maps con estructuras muy anidadas, como `Map(String, Map(Int32, Array(Nullable(String))))`, que se codificarán de forma similar a lo descrito anteriormente.
:::

<div id="variant">
  ### Variant
</div>

Este tipo representa una unión de otros tipos de datos. El tipo `Variant(T1, T2, ..., TN)` significa que cada fila de este tipo tiene un valor de tipo `T1`, `T2`, …, `TN` o ninguno de ellos (valor `NULL`).

:::warning
Aunque para el usuario final `Variant(T1, T2)` significa exactamente lo mismo que `Variant(T2, T1)`, el orden de los tipos en la definición sí importa para el formato wire: en la definición, los tipos siempre se ordenan alfabéticamente, y esto es importante, ya que la variante exacta se codifica mediante un &quot;discriminante&quot;: el índice del tipo de datos en la definición.
:::

Considere el siguiente ejemplo:

```sql
SET allow_experimental_variant_type = 1,
    allow_suspicious_variant_types = 1;
CREATE OR REPLACE TABLE foo
(
  -- It does not matter what is the order of types in the user input;
  -- the types are always sorted alphabetically in the wire format.
  `var` Variant(
           Array(Int16),
           Bool,
           Date,
           FixedString(6),
           Float32, Float64,
           Int128, Int16, Int32, Int64, Int8,
           String,
           UInt128, UInt16, UInt32, UInt64, UInt8
       )
)
ENGINE = MergeTree
ORDER BY ();
INSERT INTO foo VALUES (true), ('foobar' :: FixedString(6)), (100.5 :: Float64), (100 :: Int128), ([1, 2, 3] :: Array(Int16));
SELECT * FROM foo FORMAT RowBinary;
```

```text
0x01,                               // type index -> Bool
 0x01,                               // true
 0x03,                               // type index -> FixedString(6)
 0x66, 0x6F, 0x6F, 0x62, 0x61, 0x72, // 'foobar' 
 0x05,                               // type index -> Float64
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x20, 0x59, 0x40,             // 100.5 as Float64
 0x06,                               // type index -> Int128
 0x64, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00, 
 0x00, 0x00, 0x00, 0x00,             // 100 as Int128
 0x00,                               // type index -> Array(Int16)
 0x03,                               // LEB128 - the array has 3 elements
 0x01, 0x00,                         // 1 as Int16
 0x02, 0x00,                         // 2 as Int16
 0x03, 0x00,                         // 3 as Int16
```

Un valor `NULL` se codifica con un byte discriminante de `0xFF`:

```sql
SELECT NULL :: Variant(UInt32, String)
```

```text
0xFF, // discriminant = NULL
```

La opción de configuración [allow&#95;suspicious&#95;variant&#95;types](https://clickhouse.com/docs/operations/settings/settings#allow_suspicious_variant_types) puede utilizarse para permitir pruebas más exhaustivas del tipo `Variant`.

<div id="dynamic">
  ### Dynamic
</div>

El tipo `Dynamic` puede contener valores de cualquier tipo, determinados en tiempo de ejecución. En el formato RowBinary, cada valor es autodescriptivo: la primera parte es la especificación del tipo en [este formato](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding). A continuación viene el contenido, con la codificación del valor tal como se describe en este documento. Por tanto, para analizar un valor solo necesitas usar el índice de tipo para determinar el analizador adecuado y reutilizar la lógica de análisis de RowBinary que ya tengas en otro lugar.

```text
[BinaryTypeIndex][type-specific parameters...][value]
```

Donde `BinaryTypeIndex` es un único byte que identifica el tipo. Consulta la referencia [aquí](https://clickhouse.com/docs/sql-reference/data-types/data-types-binary-encoding) para ver los índices de tipo y los parámetros.

Un valor `NULL` de Dynamic se codifica con `BinaryTypeIndex` `0x00` (el tipo `Nothing`), sin bytes adicionales:

```sql
SELECT NULL::Dynamic
```

```text
00                        # BinaryTypeIndex: Nothing (0x00), represents NULL
```

**Ejemplos:**

```sql
SELECT 42::Dynamic
```

```text
0a                        # BinaryTypeIndex: Int64 (0x0A)
2a 00 00 00 00 00 00 00   # Int64 value: 42
```

```sql
SELECT toDateTime64('2024-01-15 10:30:00', 3, 'America/New_York')::Dynamic
```

```text
14                        # BinaryTypeIndex: DateTime64WithTimezone (0x14)
03                        # UInt8: precision
10                        # VarUInt: timezone name length
41 6d 65 72 69 63 61 2f   # "America/"
4e 65 77 5f 59 6f 72 6b   # "New_York"
c0 6c be 0d 8d 01 00 00   # Int64: timestamps
```

<div id="json">
  ### JSON
</div>

El tipo JSON codifica los datos en dos categorías distintas:

1. **Rutas tipadas** - Rutas declaradas con tipos explícitos en el esquema (p. ej., `JSON(user_id UInt32, name String)`)
2. **Rutas dinámicas/rutas de desbordamiento cuando se supera el límite de rutas dinámicas** - Rutas descubiertas en tiempo de ejecución y almacenadas como tipo `Dynamic`. La codificación del valor está precedida por la definición del tipo.

El wire format y las reglas difieren para estas dos categorías.

| Categoría de path   | Incluido en la serialización | Codificación del valor              | Se permite Variant/Nullable |
| ------------------- | ---------------------------- | ----------------------------------- | --------------------------- |
| **Rutas con tipo**  | Siempre (incluso si es NULL) | Formato binario específico del tipo | Sí                          |
| **Rutas dinámicas** | Solo si no es NULL           | Dynamic                             | No                          |

Los paths se serializan en tres grupos escritos de forma secuencial: paths tipados, paths dinámicos y, por último, paths de shared data (overflow). Los paths tipados y dinámicos se escriben en un orden implementation-defined (determinado por la iteración interna del hash-map), mientras que los paths de shared data se escriben en orden alfabético. Los lectores no deben depender de ningún orden específico de paths. El deserializador despacha cada path por nombre, no por posición.

Cada fila JSON en formato RowBinary se serializa de la siguiente manera:

```text
[VarUInt: number_of_paths]
[String: path_1][value_1]
[String: path_2][value_2]
...
```

**Ejemplos:**

**1. JSON simple solo con rutas con tipo:**

Schema: `JSON(user_id UInt32, active Bool)`

Fila: `{"user_id": 42, "active": true}`

Codificación binaria (hex con anotaciones):

```text
02                              # VarUInt: 2 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)
```

**2. JSON simple con rutas tipadas y dinámicas:**

Schema: `JSON(user_id UInt32, active Bool)`

Fila: `{"user_id": 42, "active": true, "name": "Alice"}`

Codificación binaria (hex con anotaciones):

```text
03                              # VarUInt: 3 paths total

# Typed path "active"
06 61 63 74 69 76 65            # String: "active" (length 6 + bytes)
01                              # Bool/UInt8 value: true (1)

# Dynamic path "name"
04 6E 61 6D 65                  # String: "name" (length 4 + bytes)
15                              # BinaryTypeIndex: String (0x15)
05 41 6C 69 63 65               # String value: "Alice" (length 5 + bytes)

# Typed path "user_id"
07 75 73 65 72 5F 69 64         # String: "user_id" (length 7 + bytes)
2A 00 00 00                     # UInt32 value: 42 (little-endian)

```

**3. Manejo de nulos:**

Con una columna Nullable tipada se obtiene null:

Schema: `JSON(score Nullable(Int32))`

Fila: `{"score": null }`

Codificación binaria (hex con anotaciones):

```text
01                              # VarUInt: 1 path total

# Typed path "score" (Nullable)
05 73 63 6f 72 65               # String: "score" (length 5 + bytes)
01                              # Nullable flag: 1 (is NULL, no value follows)
```

Con una columna tipada no anulable, se obtiene el valor por defecto:

Schema: `JSON(name String)`

Fila: `{"name": null}`

Codificación binaria:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

04 6e 61 6d 65  # "name"
00              # String length 0 (empty string)
```

Con una ruta dinámica, se ignora:

Esquema: `JSON(id UInt64)`

Row: `{"id": 100, "metadata": null}`

Codificación binaria:

```text
01                              # VarUInt: 1 path (dynamic NULL paths are skipped!)

# Typed path "id"
02 69 64                        # String: "id" (length 2 + bytes)
64 00 00 00 00 00 00 00         # UInt64 value: 100 (little-endian)

```

Nota: La ruta `metadata` con valor NULL **no se incluye** porque las rutas dinámicas solo se serializan cuando no son nulas. Esta es una diferencia clave con respecto a las rutas tipadas.

**4. Objetos JSON anidados:**

Esquema: `JSON()`

Fila: `{"user": {"name": "Bob", "age": 30}}`

Codificación binaria (hexadecimal con anotaciones):

```text
02                              # VarUInt: 2 paths (nested objects are flattened)

# Dynamic path "user.age"
08 75 73 65 72 2E 61 67 65      # String: "user.age" (length 8 + bytes)
0A                              # BinaryTypeIndex: Int64 (0x0A)
1E 00 00 00 00 00 00 00         # Int64 value: 30 (little-endian)

# Dynamic path "user.name"
09 75 73 65 72 2E 6E 61 6D 65   # String: "user.name" (length 9 + bytes)
15                              # BinaryTypeIndex: String (0x15)
03 42 6F 62                     # String value: "Bob" (length 3 + bytes)

```

Nota: Los objetos anidados se aplanan en paths separados por puntos (p. ej., `user.name` en lugar de una estructura anidada).

**Alternativa: JSON en modo String**

Con la configuración `output_format_binary_write_json_as_string=1`, las columnas JSON se serializan como una única cadena de texto JSON en lugar de en formato binario estructurado. Existe una configuración correspondiente para escribir en columnas JSON: `input_format_binary_read_json_as_string`. La elección de esta configuración depende de si desea parsear el JSON en el client o en el server.

<div id="geo-types">
  ### Tipos Geo
</div>

Geo es una categoría de tipos de datos que representan datos geográficos. Incluye:

* `Point` - como `Tuple(Float64, Float64)`.
* `Ring` - como `Array(Point)`, o `Array(Tuple(Float64, Float64))`.
* `Polygon` - como `Array(Ring)`, o `Array(Array(Tuple(Float64, Float64)))`.
* `MultiPolygon` - como `Array(Polygon)`, o `Array(Array(Array(Tuple(Float64, Float64))))`.
* `LineString` - como `Array(Point)`, o `Array(Tuple(Float64, Float64))`.
* `MultiLineString` - como `Array(LineString)`, o `Array(Array(Tuple(Float64, Float64)))`.

El formato wire de los valores Geo es exactamente el mismo que el de Tuple y Array. Las cabeceras del formato `RowBinaryWithNamesAndTypes` contendrán los alias de estos tipos; por ejemplo, `Point`, `Ring`, `Polygon`, `MultiPolygon`, `LineString` y `MultiLineString`.

```sql
SELECT    (1.0, 2.0)                                       :: Point           AS point,
    [(3.0, 4.0), (5.0, 6.0)]                         :: Ring            AS ring,
    [[(7.0, 8.0), (9.0, 10.0)], [(11.0, 12.0)]]      :: Polygon         AS polygon,
    [[[(13.0, 14.0), (15.0, 16.0)], [(17.0, 18.0)]]] :: MultiPolygon    AS multi_polygon,
    [(19.0, 20.0), (21.0, 22.0)]                     :: LineString      AS line_string,
    [[(23.0, 24.0), (25.0, 26.0)], [(27.0, 28.0)]]   :: MultiLineString AS multi_line_string
```

```text
// Point - or Tuple(Float64, Float64)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y
// Ring - or Array(Tuple(Float64, Float64))
0x02, // LEB128 - the "ring" array has 2 points
   // Ring - Point #1
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, 
   // Ring - Point #2
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, 
   0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, 
// Polygon - or Array(Array(Tuple(Float64, Float64)))
0x02, // LEB128 - the "polygon" array has 2 rings
   0x02, // LEB128 - the first ring has 2 points
      // Polygon - Ring #1 - Point #1
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x1C, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x40,
      // Polygon - Ring #1 - Point #2
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x22, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x24, 0x40, 
  0x01, // LEB128 - the second ring has 1 point
      // Polygon - Ring #2 - Point #1 (the only one)
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x26, 0x40, 
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x28, 0x40, 
// MultiPolygon - or Array(Array(Array(Tuple(Float64, Float64))))
0x01, // LEB128 - the "multi_polygon" array has 1 polygon
   0x02, // LEB128 - the first polygon has 2 rings
      0x02, // LEB128 - the first ring has 2 points
         // MultiPolygon - Polygon #1 - Ring #1 - Point #1
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2C, 0x40,
         // MultiPolygon - Polygon #1 - Ring #1 - Point #2
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2E, 0x40, 
         0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x30, 0x40, 
      0x01, // LEB128 - the second ring has 1 point
        // MultiPolygon - Polygon #1 - Ring #2 - Point #1 (the only one)
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x31, 0x40, 
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x32, 0x40, 
 // LineString - or Array(Tuple(Float64, Float64))
 0x02, // LEB128 - the line string has 2 points
    // LineString - Point #1
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x33, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x34, 0x40,
    // LineString - Point #2
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x35, 0x40, 
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x36, 0x40, 
 // MultiLineString - or Array(Array(Tuple(Float64, Float64)))
 0x02, // LEB128 - the multi line string has 2 line strings
   0x02, // LEB128 - the first line string has 2 points
     // MultiLineString - LineString #1 - Point #1
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x38, 0x40, 
     // MultiLineString - LineString #1 - Point #2
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x39, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3A, 0x40, 
   0x01, // LEB128 - the second line string has 1 point
     // MultiLineString - LineString #2 - Point #1 (the only one)
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3B, 0x40, 
     0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x3C, 0x40,
```

<div id="geometry">
  ### Geometry
</div>

`Geometry` es un tipo `Variant` que puede contener cualquiera de los tipos Geo enumerados anteriormente. En la representación binaria, se codifica exactamente igual que un `Variant`, con un byte discriminante que indica qué tipo geo viene a continuación.

Los índices del discriminante de `Geometry` son:

| Índice | Tipo            |
| ------ | --------------- |
| 0      | LineString      |
| 1      | MultiLineString |
| 2      | MultiPolygon    |
| 3      | Point           |
| 4      | Polygon         |
| 5      | Ring            |

Estructura del formato binario:

```text
// 1 byte discriminant (0-5)
// followed by the corresponding geo type data
```

Ejemplo de codificación de un `Point` como `Geometry`:

```sql
SELECT ((1.0, 2.0)::Point)::Geometry
```

```text
0x03,                                           // discriminant = 3 (Point)
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xF0, 0x3F, // Point.X = 1.0 as Float64
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, // Point.Y = 2.0 as Float64
```

Ejemplo de codificación de un `Ring` como `Geometry`:

```text
0x05,       // discriminant = 5 (Ring)
0x02,       // LEB128 - array has 2 points
// Point #1
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, // X = 3.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x40, // Y = 4.0
// Point #2
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x40, // X = 5.0
0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x18, 0x40, // Y = 6.0
```

<div id="nested">
  ### Nested
</div>

El formato wire de `Nested` depende del ajuste `flatten_nested`.

:::warning
Todos los arrays de componentes de una misma fila **deben tener la misma longitud**. El servidor impone esta restricción. Las longitudes desiguales provocarán errores de inserción.
:::

<div id="nested-flattened">
  #### `flatten_nested = 1` (predeterminado)
</div>

Con la configuración predeterminada, `Nested` se aplana en arrays independientes. Cada subcolumna se convierte en una columna `Array` independiente con un nombre separado por puntos:

```sql
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
-- flatten_nested=1 is the default
INSERT INTO foo VALUES (['foo', 'bar'], [42, 144]);
```

`DESCRIBE TABLE foo` muestra las columnas aplanadas:

```text
   ┌─name─┬─type──────────┐
1. │ n.a  │ Array(String) │
2. │ n.b  │ Array(Int32)  │
   └──────┴───────────────┘
```

Cada array se serializa por separado, como se describe en la sección [Array](#array):

```text
0x02,                   // LEB128 - 2 String elements in the first array (n.a)
 0x03,                   // LEB128 - the first string has 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x03,                   // LEB128 - the second string has 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
0x02,                   // LEB128 - 2 Int32 elements in the second array (n.b)
 0x2A, 0x00, 0x00, 0x00, // 42 as Int32
 0x90, 0x00, 0x00, 0x00, // 144 as Int32
```

<div id="nested-unflattened">
  #### `flatten_nested = 0`
</div>

Con `flatten_nested = 0`, `Nested` se mantiene como una única columna de tipo `Array(Tuple(...))`. El nombre de la columna no se separa con puntos:

```sql
SET flatten_nested = 0;
CREATE OR REPLACE TABLE foo
(
    n Nested(a String, b Int32)
) ENGINE = MergeTree ORDER BY ();
INSERT INTO foo VALUES ([('foo', 42), ('bar', 144)]);
```

`DESCRIBE TABLE foo` muestra una única columna:

```text
   ┌─name─┬─type───────────────────────┐
1. │ n    │ Nested(a String, b Int32)  │
   └──────┴────────────────────────────┘
```

La codificación es `Array(Tuple(String, Int32))`: un prefijo con la longitud del array, seguido de los campos de la tupla de cada elemento, en este orden:

```text
0x02,                   // LEB128 - 2 elements in the array
 0x03,                   // LEB128 - first tuple, field a: 3 bytes
 0x66, 0x6F, 0x6F,       // 'foo'
 0x2A, 0x00, 0x00, 0x00, // first tuple, field b: 42 as Int32
 0x03,                   // LEB128 - second tuple, field a: 3 bytes
 0x62, 0x61, 0x72,       // 'bar'
 0x90, 0x00, 0x00, 0x00, // second tuple, field b: 144 as Int32
```

Observe cómo los campos se intercalan por elemento (a₁, b₁, a₂, b₂) en lugar de agruparse por columna (a₁, a₂, b₁, b₂), como en la representación aplanada.

<div id="simpleaggregatefunction">
  ### SimpleAggregateFunction
</div>

`SimpleAggregateFunction(func, T)` se codifica de forma idéntica que su tipo de dato subyacente `T`. El nombre de la función de agregación no afecta al formato wire.

Por ejemplo, `SimpleAggregateFunction(max, UInt32)` se codifica de la misma forma que un `UInt32` normal:

```sql
CREATE TABLE test_saf
(
    key UInt32,
    val SimpleAggregateFunction(max, UInt32)
) ENGINE = AggregatingMergeTree ORDER BY key;

INSERT INTO test_saf VALUES (1, 42);
SELECT val FROM test_saf;
```

El encabezado RowBinaryWithNamesAndTypes informa el tipo como `SimpleAggregateFunction(max, UInt32)`, pero el valor serializado es simplemente un `UInt32`:

```text
0x2A, 0x00, 0x00, 0x00, // 42 as UInt32
```

<div id="aggregatefunction">
  ### AggregateFunction
</div>

`AggregateFunction(func, T)` almacena el estado intermedio completo de una función de agregación. A diferencia de `SimpleAggregateFunction`, que también almacena un estado intermedio pero lo codifica de forma idéntica al tipo de dato subyacente, `AggregateFunction` almacena un blob binario opaco cuyo formato es específico de cada función de agregación.

:::warning
Los estados de agregación **no tienen prefijo de longitud** en RowBinary. Un analizador debe comprender el formato de serialización interno de cada función de agregación concreta para saber cuántos bytes debe consumir. En la práctica, la mayoría de los client tratan los estados de agregación como opacos y usan los combinadores `*State` / `*Merge` para que el server se encargue de la serialización.
:::

El formato interno varía según la función. Algunos ejemplos sencillos:

**`countState`** — almacena el recuento como un VarUInt (LEB128):

```sql
SELECT countState(number) FROM numbers(5)
```

```text
0x05, // VarUInt: 5
```

**`sumState`** — almacena la suma acumulada en un entero de tamaño fijo. El tamaño depende del tipo del argumento (`UInt64` para argumentos enteros):

```sql
SELECT sumState(toUInt32(number)) FROM numbers(5) -- sum = 0+1+2+3+4 = 10
```

```text
0x0A, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // 10 as UInt64
```

**`minState` / `maxState`** — almacena un byte indicador, seguido del valor en el tipo subyacente. El indicador es `0x00` para un estado vacío (no se ha visto ningún valor) o `0x01` cuando hay un valor presente:

```sql
SELECT maxState(toUInt32(number)) FROM numbers(5) -- max = 4
```

```text
0x01,                   // flag: has value
0x04, 0x00, 0x00, 0x00, // 4 as UInt32
```

Un estado vacío (sin filas agregadas):

```sql
SELECT minState(toUInt32(number)) FROM numbers(0)
```

```text
0x00, // flag: no value
```

:::note
Las funciones más complejas, como `uniq`, `quantile` o `groupArray`, usan formatos específicos de la implementación. Si necesita leer o escribir estos estados, consulte el código fuente de ClickHouse de la función correspondiente.
:::

<div id="qbit">
  ### QBit
</div>

`QBit` es un tipo de vector para búsquedas eficientes con distintos niveles de precisión. Internamente, se almacena en formato transpuesto. En la transmisión, QBit no es más que un `Array` del tipo de elemento subyacente (`Int8`, `Float32`, `Float64` o `BFloat16`). La optimización de transposición de bits para el almacenamiento se realiza en el servidor, no en el protocolo RowBinary.

Sintaxis:

```text
QBit(element_type, dimension[, stride])
```

Donde `element_type` es `Int8`, `Float32`, `Float64` o `BFloat16`, y `dimension` es la dimensión fija del vector. El `stride` opcional solo controla cómo se agrupan los planos de bits en flujos de almacenamiento en el servidor; no afecta al formato wire de RowBinary, que siempre es el array completo de `dimension` elementos.

Formato wire: idéntico a `Array(element_type)`:

```text
// LEB128 length
// followed by `length` elements of `element_type`
```

Ejemplo de codificación de `QBit(Float32, 4)` que contiene `[1.0, 2.0, 3.0, 4.0]`:

```sql
SELECT [1.0, 2.0, 3.0, 4.0]::QBit(Float32, 4)
```

```text
0x04,                   // LEB128 - array has 4 elements
0x00, 0x00, 0x80, 0x3F, // 1.0 as Float32
0x00, 0x00, 0x00, 0x40, // 2.0 as Float32
0x00, 0x00, 0x40, 0x40, // 3.0 as Float32
0x00, 0x00, 0x80, 0x40, // 4.0 as Float32
```

<div id="format-settings">
  ## Configuración del formato
</div>

<RowBinaryFormatSettings />