# Funciones hash {#hash-functions}

Las funciones Hash se pueden usar para la barajada pseudoaleatoria determinista de elementos.

## HalfMD5 {#hash-functions-halfmd5}

[Interpretar](../../query_language/functions/type_conversion_functions.md#type_conversion_functions-reinterpretAsString) todos los parámetros de entrada como cadenas y calcula el [MD5](https://en.wikipedia.org/wiki/MD5) valor hash para cada uno de ellos. Luego combina hashes, toma los primeros 8 bytes del hash de la cadena resultante y los interpreta como `UInt64` en orden de bytes de big-endian.

``` sql
halfMD5(par1, ...)
```

La función es relativamente lenta (5 millones de cadenas cortas por segundo por núcleo del procesador).
Considere usar el [sipHash64](#hash_functions-siphash64) función en su lugar.

**Parámetros**

La función toma un número variable de parámetros de entrada. Los parámetros pueden ser cualquiera de los [tipos de datos compatibles](../../data_types/index.md).

**Valor devuelto**

Un [UInt64](../../data_types/int_uint.md) tipo de datos valor hash.

**Ejemplo**

``` sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type
```

``` text
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD5 {#hash-functions-md5}

Calcula el MD5 de una cadena y devuelve el conjunto de bytes resultante como FixedString(16).
Si no necesita MD5 en particular, pero necesita un hash criptográfico de 128 bits decente, use el ‘sipHash128’ función en su lugar.
Si desea obtener el mismo resultado que la salida de la utilidad md5sum, use lower(hex(MD5(s)) .

## sipHash64 {#hash-functions-siphash64}

Produce un [SipHash](https://131002.net/siphash/) valor hash.

``` sql
sipHash64(par1,...)
```

Esta es una función hash criptográfica. Funciona al menos tres veces más rápido que el [MD5](#hash_functions-md5) función.

Función [interpretar](../../query_language/functions/type_conversion_functions.md#type_conversion_functions-reinterpretAsString) todos los parámetros de entrada como cadenas y calcula el valor hash para cada uno de ellos. Luego combina hashes por el siguiente algoritmo:

1.  Después de hash todos los parámetros de entrada, la función obtiene la matriz de hashes.
2.  La función toma el primero y el segundo elementos y calcula un hash para la matriz de ellos.
3.  Luego, la función toma el valor hash, calculado en el paso anterior, y el tercer elemento de la matriz hash inicial, y calcula un hash para la matriz de ellos.
4.  El paso anterior se repite para todos los elementos restantes de la matriz hash inicial.

**Parámetros**

La función toma un número variable de parámetros de entrada. Los parámetros pueden ser cualquiera de los [tipos de datos compatibles](../../data_types/index.md).

**Valor devuelto**

Un [UInt64](../../data_types/int_uint.md) tipo de datos valor hash.

**Ejemplo**

``` sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type
```

``` text
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## sipHash128 {#hash-functions-siphash128}

Calcula SipHash a partir de una cadena.
Acepta un argumento de tipo String. Devuelve FixedString(16).
Difiere de sipHash64 en que el estado final de plegado xor solo se realiza hasta 128 bits.

## cityHash64 {#cityhash64}

Produce un [Método de codificación de datos:](https://github.com/google/cityhash) valor hash.

``` sql
cityHash64(par1,...)
```

Esta es una función hash rápida no criptográfica. Utiliza el algoritmo CityHash para parámetros de cadena y la función hash no criptográfica rápida específica de la implementación para parámetros con otros tipos de datos. La función utiliza el combinador CityHash para obtener los resultados finales.

**Parámetros**

La función toma un número variable de parámetros de entrada. Los parámetros pueden ser cualquiera de los [tipos de datos compatibles](../../data_types/index.md).

**Valor devuelto**

Un [UInt64](../../data_types/int_uint.md) tipo de datos valor hash.

**Ejemplos**

Ejemplo de llamada:

``` sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type
```

``` text
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

En el ejemplo siguiente se muestra cómo calcular la suma de comprobación de toda la tabla con precisión hasta el orden de fila:

``` sql
SELECT groupBitXor(cityHash64(*)) FROM table
```

## intHash32 {#inthash32}

Calcula un código hash de 32 bits a partir de cualquier tipo de entero.
Esta es una función hash no criptográfica relativamente rápida de calidad media para los números.

## intHash64 {#inthash64}

Calcula un código hash de 64 bits a partir de cualquier tipo de entero.
Funciona más rápido que intHash32. Calidad media.

## SHA1 {#sha1}

## SHA224 {#sha224}

## SHA256 {#sha256}

Calcula SHA-1, SHA-224 o SHA-256 de una cadena y devuelve el conjunto de bytes resultante como FixedString(20), FixedString(28) o FixedString(32).
La función funciona bastante lentamente (SHA-1 procesa alrededor de 5 millones de cadenas cortas por segundo por núcleo del procesador, mientras que SHA-224 y SHA-256 procesan alrededor de 2.2 millones).
Recomendamos usar esta función solo en los casos en que necesite una función hash específica y no pueda seleccionarla.
Incluso en estos casos, recomendamos aplicar la función offline y precalcular valores al insertarlos en la tabla, en lugar de aplicarlo en SELECTS.

## Nombre de la red inalámbrica (SSID):\]) {#urlhashurl-n}

Una función hash no criptográfica rápida y de calidad decente para una cadena obtenida de una URL utilizando algún tipo de normalización.
`URLHash(s)` – Calcula un hash de una cadena sin uno de los símbolos finales `/`,`?` o `#` al final, si está presente.
`URLHash(s, N)` – Calcula un hash de una cadena hasta el nivel N en la jerarquía de URL, sin uno de los símbolos finales `/`,`?` o `#` al final, si está presente.
Los niveles son los mismos que en URLHierarchy. Esta función es específica de Yandex.Métrica.

## Método de codificación de datos: {#farmhash64}

Produce un [Método de codificación de datos:](https://github.com/google/farmhash) valor hash.

``` sql
farmHash64(par1, ...)
```

La función utiliza el `Hash64` de todos [métodos disponibles](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**Parámetros**

La función toma un número variable de parámetros de entrada. Los parámetros pueden ser cualquiera de los [tipos de datos compatibles](../../data_types/index.md).

**Valor devuelto**

Un [UInt64](../../data_types/int_uint.md) tipo de datos valor hash.

**Ejemplo**

``` sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type
```

``` text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## Nombre de la red inalámbrica (SSID): {#hash-functions-javahash}

Calcular [Nivel de Cifrado WEP](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) de una cuerda. Esta función hash no es rápida ni tiene una buena calidad. La única razón para usarlo es cuando este algoritmo ya se usa en otro sistema y debe calcular exactamente el mismo resultado.

**Sintaxis**

``` sql
SELECT javaHash('');
```

**Valor devuelto**

Un `Int32` tipo de datos valor hash.

**Ejemplo**

Consulta:

``` sql
SELECT javaHash('Hello, world!');
```

Resultado:

``` text
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## javaHashUTF16LE {#javahashutf16le}

Calcular [Nivel de Cifrado WEP](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) de una cadena, suponiendo que contiene bytes que representan una cadena en codificación UTF-16LE.

**Sintaxis**

``` sql
javaHashUTF16LE(stringUtf16le)
```

**Parámetros**

-   `stringUtf16le` — una cadena en codificación UTF-16LE.

**Valor devuelto**

Un `Int32` tipo de datos valor hash.

**Ejemplo**

Consulta correcta con cadena codificada UTF-16LE.

Consulta:

``` sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))
```

Resultado:

``` text
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## HiveHash {#hash-functions-hivehash}

Calcular `HiveHash` de una cuerda.

``` sql
SELECT hiveHash('');
```

Esto es sólo [Nivel de Cifrado WEP](#hash_functions-javahash) con poco de signo puesto a cero. Esta función se utiliza en [Colmena de Apache](https://en.wikipedia.org/wiki/Apache_Hive) para versiones anteriores a la 3.0. Esta función hash no es rápida ni tiene una buena calidad. La única razón para usarlo es cuando este algoritmo ya se usa en otro sistema y debe calcular exactamente el mismo resultado.

**Valor devuelto**

Un `Int32` tipo de datos valor hash.

Tipo: `hiveHash`.

**Ejemplo**

Consulta:

``` sql
SELECT hiveHash('Hello, world!');
```

Resultado:

``` text
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## Método de codificación de datos: {#metrohash64}

Produce un [Método de codificación de datos:](http://www.jandrewrogers.com/2015/05/27/metrohash/) valor hash.

``` sql
metroHash64(par1, ...)
```

**Parámetros**

La función toma un número variable de parámetros de entrada. Los parámetros pueden ser cualquiera de los [tipos de datos compatibles](../../data_types/index.md).

**Valor devuelto**

Un [UInt64](../../data_types/int_uint.md) tipo de datos valor hash.

**Ejemplo**

``` sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type
```

``` text
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## SaltarConsistentHash {#jumpconsistenthash}

Calcula JumpConsistentHash forma un UInt64.
Acepta dos argumentos: una clave de tipo UInt64 y el número de cubos. Devuelve Int32.
Para obtener más información, consulte el enlace: [SaltarConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2\_32, murmurHash2\_64 {#murmurhash2-32-murmurhash2-64}

Produce un [Método de codificación de datos:](https://github.com/aappleby/smhasher) valor hash.

``` sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**Parámetros**

Ambas funciones toman un número variable de parámetros de entrada. Los parámetros pueden ser cualquiera de los [tipos de datos compatibles](../../data_types/index.md).

**Valor devuelto**

-   El `murmurHash2_32` función devuelve el valor hash que tiene el [UInt32](../../data_types/int_uint.md) tipo de datos.
-   El `murmurHash2_64` función devuelve el valor hash que tiene el [UInt64](../../data_types/int_uint.md) tipo de datos.

**Ejemplo**

``` sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type
```

``` text
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## murmurHash3\_32, murmurHash3\_64 {#murmurhash3-32-murmurhash3-64}

Produce un [Método de codificación de datos:](https://github.com/aappleby/smhasher) valor hash.

``` sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**Parámetros**

Ambas funciones toman un número variable de parámetros de entrada. Los parámetros pueden ser cualquiera de los [tipos de datos compatibles](../../data_types/index.md).

**Valor devuelto**

-   El `murmurHash3_32` función devuelve un [UInt32](../../data_types/int_uint.md) tipo de datos valor hash.
-   El `murmurHash3_64` función devuelve un [UInt64](../../data_types/int_uint.md) tipo de datos valor hash.

**Ejemplo**

``` sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3\_128 {#murmurhash3-128}

Produce un [Método de codificación de datos:](https://github.com/aappleby/smhasher) valor hash.

``` sql
murmurHash3_128( expr )
```

**Parámetros**

-   `expr` — [Expresiones](../syntax.md#syntax-expressions) devolviendo un [Cadena](../../data_types/string.md)-tipo de valor.

**Valor devuelto**

Un [Cadena fija (16)](../../data_types/fixedstring.md) tipo de datos valor hash.

**Ejemplo**

``` sql
SELECT murmurHash3_128('example_string') AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3──────┬─type────────────┐
│ 6�1�4"S5KT�~~q │ FixedString(16) │
└──────────────────┴─────────────────┘
```

## xxHash32, xxHash64 {#hash-functions-xxhash32}

Calcular `xxHash` de una cuerda. Se propone en dos sabores, 32 y 64 bits.

``` sql
SELECT xxHash32('');

OR

SELECT xxHash64('');
```

**Valor devuelto**

Un `Uint32` o `Uint64` tipo de datos valor hash.

Tipo: `xxHash`.

**Ejemplo**

Consulta:

``` sql
SELECT xxHash32('Hello, world!');
```

Resultado:

``` text
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**Ver también**

-   [xxHash](http://cyan4973.github.io/xxHash/).

[Artículo Original](https://clickhouse.tech/docs/es/query_language/functions/hash_functions/) <!--hide-->
