---
alias: []
description: 'Documentación sobre el formato Native'
input_format: true
keywords: ['Native']
output_format: true
slug: /interfaces/formats/Native
title: 'Native'
doc_type: 'reference'
---

| Entrada | Salida | Alias |
| ------- | ------ | ----- |
| ✔       | ✔      |       |

<div id="description">
  ## Descripción
</div>

La especificación oficial completa del formato `Native` está disponible [aquí](/es/interfaces/specs/NativeFormat), y la especificación complementaria del protocolo `Native` —el protocolo TCP de red que lo transporta— está disponible [aquí](/es/interfaces/specs/NativeProtocol).

:::note
Ambas especificaciones fueron generadas por LLM a partir del código fuente de ClickHouse. El código sigue siendo la referencia principal: si la especificación y el código discrepan, el código es correcto.
:::

El formato `Native` es el formato más eficiente de ClickHouse porque es realmente &quot;columnar&quot;,
ya que no convierte las columnas en filas.

En este formato, los datos se escriben y se leen por [bloques](/es/development/architecture#block) en formato binario.
Para cada bloque, se registran uno tras otro el número de filas, el número de columnas, los nombres y tipos de las columnas, y las partes de las columnas del bloque.

Este es el formato utilizado en la interfaz nativa para la interacción entre servidores, para usar el cliente de línea de comandos y para los clientes de C++.

:::tip
Puedes usar este formato para generar rápidamente volcados que solo ClickHouse puede leer.
Puede que no sea práctico trabajar directamente con este formato.
:::

<div id="data-types-wire-format">
  ## Formato wire de los tipos de datos
</div>

Los datos se envían por wire en formato columnar, lo que significa que cada columna se envía por separado
y que todos los valores de una columna se envían juntos como un único array.

Cada columna de un bloque contiene un encabezado similar a [RowBinaryWithNamesAndTypes](../formats/RowBinary/RowBinaryWithNamesAndTypes.md).

:::note
Al usar el protocolo binario native TCP (o cuando el endpoint HTTP recibe `?client_protocol_version=<n>`),
se escribe una estructura `BlockInfo` antes de los recuentos de columnas y filas. Los ejemplos de esta sección usan
la interfaz HTTP estándar sin versión del protocolo, por lo que se omite `BlockInfo`.
:::

<div id="block-structure">
  ### Estructura del bloque
</div>

La siguiente consulta devuelve dos columnas, `number` y `str`, con tres filas:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str FROM system.numbers LIMIT 3" > out.bin
```

Los datos de salida caben en un único bloque de ClickHouse y tendrán este aspecto:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x02,                   // 2 columns
  0x03,                   // 3 rows
  // -- Column 1 Header --
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6e, 0x75, 0x6d,       
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6e,
  0x74, 0x36, 0x34,       // 'UInt64'
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x01, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x02, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 2 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6e, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
  0x01,                   // LEB128 - the string has 1 byte
  0x32,                   // '2' as String
])
```

<div id="multiple-blocks">
  ### Múltiples bloques
</div>

Sin embargo, en muchos casos, los datos no cabrán en un solo bloque, y ClickHouse los enviará en varios bloques.
Considere la siguiente consulta, que recupera dos filas con un tamaño de bloque reducido para forzar que los datos se dividan en una fila por bloque:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" --data-binary "SELECT number, toString(number) AS str                FROM system.numbers LIMIT 2                 SETTINGS max_block_size=1" \  > out.bin
```

Salida:

```js
const data = new Uint8Array([
 
  // ----- Block 1 ----- 
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D, 
  0x62, 0x65, 0x72,       // column name: 'number' 
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34,       // 'UInt64' 
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00, // 0 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72, 
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x30,                   // '0' as String
  
  // ----- Block 2 -----
  0x02,                   // 2 columns
  0x01,                   // 1 row
  0x06,                   // LEB128 - column name 'number' has 6 bytes
  0x6E, 0x75, 0x6D,  
  0x62, 0x65, 0x72,       // column name: 'number'
  0x06,                   // LEB128 - column type 'UInt64' has 6 bytes
  0x55, 0x49, 0x6E,  
  0x74, 0x36, 0x34,       // 'UInt64'
  0x01, 0x00, 0x00, 0x00,  
  0x00, 0x00, 0x00, 0x00, // 1 as UInt64
  0x03,                   // LEB128 - column name 'str' has 3 bytes
  0x73, 0x74, 0x72,       // column name: 'str'
  0x06,                   // LEB128 - column type 'String' has 6 bytes
  0x53, 0x74, 0x72,  
  0x69, 0x6E, 0x67,       // 'String'
  0x01,                   // LEB128 - the string has 1 byte
  0x31,                   // '1' as String
]);
```

<div id="simple-data-types">
  ### Tipos de datos simples
</div>

El formato wire de un valor individual de uno de los tipos de datos más simples es similar al de `RowBinary`/`RowBinaryWithNamesAndTypes`.
La lista completa de tipos que corresponden a esta descripción incluye:

* (U)Int8, (U)Int16, (U)Int32, (U)Int64, (U)Int128, (U)Int256
* Float32, Float64
* Bool
* String
* FixedString(N)
* Date
* Date32
* DateTime
* DateTime64
* IPv4
* IPv6
* UUID

Consulta las descripciones de los tipos anteriores en [&quot;formato wire de los tipos de datos de RowBinary&quot;](/es/interfaces/formats/RowBinary#data-types-wire-format) para obtener más detalles.

<div id="complex-data-types">
  ### Tipos de datos complejos
</div>

La codificación de los siguientes tipos es diferente de `RowBinary` y `RowBinaryWithNamesAndTypes`.

* Nullable
* LowCardinality
* Array
* Map
* Variant
* Dynamic
* JSON

<div id="nullable">
  #### Nullable
</div>

En el formato `Native`, una columna Nullable tendrá un número de bytes igual al número de filas del bloque antes de los datos propiamente dichos. Cada uno de estos bytes indica si el valor es `NULL` o no. Por ejemplo, con esta consulta, cada número impar será `NULL`:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, number, NULL) :: Nullable(UInt64) AS maybe_null                 FROM system.numbers LIMIT 5" \  > out.bin
```

La salida tendrá este aspecto:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01,                         // LEB128 - 1 column
  0x05,                         // LEB128 - 5 rows
  
  // -- Column Header --
  0x0A,                         // LEB128 - column name has 10 bytes
  0x6D, 0x61, 0x79, 0x62, 0x65, 
  0x5F, 0x6E, 0x75, 0x6C, 0x6C, // column name: 'maybe_null'
  
  0x10,                         // LEB128 - column type has 16 bytes
  0x4E, 0x75, 0x6C, 0x6C, 
  0x61, 0x62, 0x6C, 0x65, 
  0x28, 0x55, 0x49, 0x6E, 
  0x74, 0x36, 0x34, 0x29,       // column type: 'Nullable(UInt64)'
  
  // -- Nullable mask --
  0x00,                         // Row 0 is NOT NULL
  0x01,                         // Row 1 is NULL
  0x00,                         // Row 2 is NOT NULL
  0x01,                         // Row 3 is NULL
  0x00,                         // Row 4 is NOT NULL
  
  // -- UInt64 values --
  0x00, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row 0: 0 as UInt64

  // even though we still might have a proper value for this number 
  // in the block, it should be still returned as NULL to the user!
  0x01, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #1: NULL
  
  0x02, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,       // Row #2: 2 as UInt64
  
  0x03, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #3: NULL, similar to Row #1
  
  0x04, 0x00, 0x00, 0x00, 
  0x00, 0x00, 0x00, 0x00,       // Row #4: 4 as UInt64
]);
```

Funciona de forma similar con `Nullable(String)`. El indicador de null siempre proviene del byte de máscara de nullable —
un valor de máscara de `0x01` significa que la fila es `NULL` independientemente del contenido de la cadena. En las filas `NULL`,
la cadena subyacente se almacena como una cadena vacía (longitud LEB128 `0`). Ten en cuenta que una cadena vacía no `NULL`
también tiene longitud LEB128 `0`, por lo que solo el byte de máscara distingue ambos casos. Por ejemplo, la siguiente consulta:

```bash
curl -XPOST "http://localhost:8123?default_format=Native" \  --data-binary "SELECT if(number % 2 = 0, toString(number), NULL) :: Nullable(String) AS maybe_str                 FROM system.numbers LIMIT 5" \  > out.bin
```

La salida tendrá este aspecto:

```js
const data = new Uint8Array([
  // --- Block Header ---
  0x01, // LEB128 - 1 column
  0x05, // LEB128 - 5 rows

  // -- Column Header --
  0x09, // LEB128 - column name has 9 bytes
  0x6d,
  0x61,
  0x79,
  0x62,
  0x65,
  0x5f,
  0x73,
  0x74,
  0x72, // column name: 'maybe_str'

  0x10, // LEB128 - column type has 16 bytes
  0x4e,
  0x75,
  0x6c,
  0x6c,
  0x61,
  0x62,
  0x6c,
  0x65,
  0x28,
  0x53,
  0x74,
  0x72,
  0x69,
  0x6e,
  0x67,
  0x29, // column type: 'Nullable(String)'

  // -- Nullable mask --
  0x00, // Row 0 is NOT NULL
  0x01, // Row 1 is NULL
  0x00, // Row 2 is NOT NULL
  0x01, // Row 3 is NULL
  0x00, // Row 4 is NOT NULL

  // -- String values --
  0x01,
  0x30, // Row 0: LEB128 == 1, '0' as String
  0x00, // Row 1: LEB128 == 0, NULL
  0x01,
  0x32, // Row 2: LEB128 == 1, '2' as String
  0x00, // Row 3: LEB128 == 0, NULL
  0x01,
  0x34, // Row 4: LEB128 == 1, '4' as String
])
```

<div id="lowcardinality">
  #### LowCardinality
</div>

A diferencia de [RowBinary](RowBinary/RowBinary.md#lowcardinality), donde `LowCardinality` es transparente, el formato Native usa una codificación columnar basada en diccionario. Una columna se codifica como un prefijo de versión, seguido de un diccionario de valores únicos y un array de índices enteros a ese diccionario.

:::note
Una columna puede definirse como `LowCardinality(Nullable(T))`, pero no es posible definirla como `Nullable(LowCardinality(T))` — siempre dará como resultado un error del servidor.
:::

El prefijo de versión es un `UInt64(LE)` con valor `1`, escrito una vez por columna. Luego, por bloque, se escribe lo siguiente:

* `UInt64(LE)` — campo de bits `IndexesSerializationType`. Los bits 0–7 codifican el ancho del índice (0 = UInt8, 1 = UInt16, 2 = UInt32, 3 = UInt64). El bit 8 (`NeedGlobalDictionaryBit`) nunca se establece en el formato Native (el servidor lanza una excepción si lo encuentra). El bit 9 indica que hay claves adicionales del diccionario. El bit 10 indica que el diccionario debe restablecerse.
* `UInt64(LE)` — número de claves del diccionario, seguido de las claves serializadas en bloque usando la codificación del tipo interno.
* `UInt64(LE)` — número de filas, seguido de los valores de índice serializados en bloque usando el ancho UInt correspondiente.

El diccionario siempre contiene un valor predeterminado en el índice 0 (p. ej., una cadena vacía para `String`, 0 para tipos numéricos). Para `LowCardinality(Nullable(T))`, el índice 0 representa `NULL`, y las claves se serializan sin el envoltorio `Nullable`.

Por ejemplo, `LowCardinality(String)` con 5 filas `['foo', 'bar', 'baz', 'foo', 'bar']`:

```text
// Version prefix
01 00 00 00 00 00 00 00    // UInt64(LE) = 1

// IndexesSerializationType: UInt8 indexes, has keys, update dictionary
00 06 00 00 00 00 00 00    // UInt64(LE) = 0x0600

04 00 00 00 00 00 00 00    // 4 dictionary keys
00                          // key 0: "" (default)
03 66 6f 6f                 // key 1: "foo"
03 62 61 72                 // key 2: "bar"
03 62 61 7a                 // key 3: "baz"

05 00 00 00 00 00 00 00    // 5 rows
01 02 03 01 02              // indexes → "foo", "bar", "baz", "foo", "bar"
```

Con `LowCardinality(Nullable(String))`, el índice 0 es `NULL`:

```text
01 00 00 00 00 00 00 00    // version
00 06 00 00 00 00 00 00    // IndexesSerializationType
03 00 00 00 00 00 00 00    // 3 keys
00                          // key 0: NULL
00                          // key 1: "" (default)
03 79 65 73                 // key 2: "yes"
05 00 00 00 00 00 00 00    // 5 rows
02 00 02 00 02              // indexes → "yes", NULL, "yes", NULL, "yes"
```

<div id="array">
  #### Array
</div>

A diferencia de [RowBinary](RowBinary/RowBinary.md#array), donde cada array lleva como prefijo un conteo de elementos en LEB128, el formato Native codifica los arrays como dos subflujos columnares:

* N desplazamientos acumulativos `UInt64` (little-endian, 8 bytes cada uno). La fila `i` tiene `offset[i] - offset[i-1]` elementos, con `offset[-1]` implícitamente igual a 0.
* Todos los elementos Nested de todas las filas, serializados en bloque de forma contigua.

Por ejemplo, `Array(UInt32)` con 3 filas `[[0, 10], [1, 11], [2, 12]]`:

```text
// Offsets
02 00 00 00 00 00 00 00    // 2 (row 0: 2 elements)
04 00 00 00 00 00 00 00    // 4 (row 1: 2 elements)
06 00 00 00 00 00 00 00    // 6 (row 2: 2 elements)

// Nested UInt32 values (6 total)
00 00 00 00                 // 0
0a 00 00 00                 // 10
01 00 00 00                 // 1
0b 00 00 00                 // 11
02 00 00 00                 // 2
0c 00 00 00                 // 12
```

Un array vacío tiene el mismo desplazamiento que la fila anterior. Por ejemplo, `Array(String)` con 4 filas `[[], ['0'], ['0','1'], ['0','1','2']]`:

```text
00 00 00 00 00 00 00 00    // 0 (empty)
01 00 00 00 00 00 00 00    // 1
03 00 00 00 00 00 00 00    // 3
06 00 00 00 00 00 00 00    // 6
01 30                       // "0"
01 30                       // "0"
01 31                       // "1"
01 30                       // "0"
01 31                       // "1"
01 32                       // "2"
```

<div id="map">
  #### Map
</div>

Un `Map(K, V)` se codifica como `Array(Tuple(K, V))`: primero los desplazamientos del array, luego todas las claves y, por último, todos los valores. Esto difiere de [RowBinary](RowBinary/RowBinary.md#map), donde las claves y los valores se intercalan en cada entrada.

Por ejemplo, `Map(String, UInt64)` con 3 filas `[{'a':0,'b':10}, {'a':1,'b':11}, {'a':2,'b':12}]`:

```text
// Array offsets
02 00 00 00 00 00 00 00    // 2
04 00 00 00 00 00 00 00    // 4
06 00 00 00 00 00 00 00    // 6

// All keys (6 Strings)
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"
01 61                       // "a"
01 62                       // "b"

// All values (6 UInt64s)
00 00 00 00 00 00 00 00    // 0
0a 00 00 00 00 00 00 00    // 10
01 00 00 00 00 00 00 00    // 1
0b 00 00 00 00 00 00 00    // 11
02 00 00 00 00 00 00 00    // 2
0c 00 00 00 00 00 00 00    // 12
```

<div id="variant">
  #### Variant
</div>

A diferencia de [RowBinary](RowBinary/RowBinary.md#variant), donde cada fila lleva su propio byte discriminador seguido del valor inline, el formato Native separa los discriminadores de los datos.

:::warning
Al igual que en RowBinary, los tipos de la definición siempre se ordenan alfabéticamente, y el discriminador es el índice en esa lista ordenada. `0xFF` (255) representa `NULL`.
:::

Una columna `Variant` se codifica de la siguiente manera:

* Prefijo del modo de discriminadores `UInt64(LE)` (`0` = BASIC, `1` = COMPACT). La salida en formato Native normalmente usa BASIC (`0`); el modo COMPACT puede aparecer al leer datos almacenados con `use_compact_variant_discriminators_serialization` habilitado.
* N discriminadores `UInt8`, uno por fila.
* Los datos de cada tipo variante como una columna en bloque independiente que contiene solo las filas correspondientes, en orden de discriminador.

Por ejemplo, `Variant(String, UInt32)` con 5 filas `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']` (ordenados: `String` = 0, `UInt32` = 1):

```text
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
01 00 ff 01 00              // UInt32, String, NULL, UInt32, String

// String (2 values, rows 1 and 4)
05 68 65 6c 6c 6f          // "hello"
05 68 65 6c 6c 6f          // "hello"

// UInt32 (2 values, rows 0 and 3)
00 00 00 00                 // 0
03 00 00 00                 // 3
```

<div id="dynamic">
  #### Dynamic
</div>

A diferencia de [RowBinary](RowBinary/RowBinary.md#dynamic), donde cada valor es autodescriptivo (prefijo de tipo + valor), el formato Native serializa `Dynamic` como un prefijo de estructura seguido de una columna [Variant](#variant).

El prefijo de estructura contiene una versión de serialización `UInt64(LE)`, luego el número de tipos dinámicos (como VarUInt) y, a continuación, los nombres de los tipos como cadenas. En la versión V1, el recuento de tipos se escribe dos veces por compatibilidad. Los datos que siguen corresponden a una columna `Variant` cuya lista de tipos está formada por los tipos dinámicos más un tipo interno `SharedVariant`, ordenados alfabéticamente.

Por ejemplo, `Dynamic` con 5 filas `[0::UInt32, 'hello', NULL, 3::UInt32, 'hello']`:

```text
// Structure prefix (V1)
01 00 00 00 00 00 00 00    // version = V1
02                          // num types (V1 writes twice)
02                          // num types
06 53 74 72 69 6e 67       // "String"
06 55 49 6e 74 33 32       // "UInt32"

// Variant data: Variant(SharedVariant, String, UInt32)
// discriminants: SharedVariant=0, String=1, UInt32=2
00 00 00 00 00 00 00 00    // discriminators mode = BASIC
02 01 ff 02 01              // UInt32, String, NULL, UInt32, String
// SharedVariant: 0 values
05 68 65 6c 6c 6f          // String: "hello"
05 68 65 6c 6c 6f          // String: "hello"
00 00 00 00                 // UInt32: 0
03 00 00 00                 // UInt32: 3
```

<div id="json">
  #### JSON
</div>

A diferencia de [RowBinary](RowBinary/RowBinary.md#json), donde cada fila se describe a sí misma con nombres de ruta y valores, el formato Native serializa `JSON` con una estructura columnar. La codificación es compleja y depende de la versión: consta de un prefijo de estructura con la versión de serialización, nombres de rutas dinámicas y la disposición de los datos compartidos; a continuación vienen las rutas tipadas (cada una como una columna en bloque), las rutas dinámicas (cada una como una columna [Dynamic](#dynamic)) y los datos compartidos para las rutas de desbordamiento.

Para simplificar la interoperabilidad, considere usar la configuración `output_format_native_write_json_as_string=1`, que serializa las columnas JSON como cadenas de texto JSON sin formato (un `String` por fila).