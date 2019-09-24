# Hash functions

Hash functions can be used for the deterministic pseudo-random shuffling of elements.

## halfMD5 {#hash_functions-halfmd5}

[Interprets](../../query_language/functions/type_conversion_functions.md#type_conversion_functions-reinterpretAsString) all the input parameters as strings and calculates the [MD5](https://en.wikipedia.org/wiki/MD5) hash value for each of them. Then combines hashes, takes the first 8 bytes of the hash of the resulting string, and interprets them as `UInt64` in big-endian byte order.

```sql
halfMD5(par1, ...)
```

The function is relatively slow (5 million short strings per second per processor core).
Consider using the [sipHash64](#hash_functions-siphash64) function instead.

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

A [UInt64](../../data_types/int_uint.md) data type hash value.

**Example**

```sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type
```
```text
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD5 {#hash_functions-md5}

Calculates the MD5 from a string and returns the resulting set of bytes as FixedString(16).
If you don't need MD5 in particular, but you need a decent cryptographic 128-bit hash, use the 'sipHash128' function instead.
If you want to get the same result as output by the md5sum utility, use lower(hex(MD5(s))).

## sipHash64 {#hash_functions-siphash64}

Produces a 64-bit [SipHash](https://131002.net/siphash/) hash value.

```sql
sipHash64(par1,...)
```

This is a cryptographic hash function. It works at least three times faster than the [MD5](#hash_functions-md5) function.

Function [interprets](../../query_language/functions/type_conversion_functions.md#type_conversion_functions-reinterpretAsString) all the input parameters as strings and calculates the hash value for each of them. Then combines hashes by the following algorithm:

1. After hashing all the input parameters, the function gets the array of hashes.
2. Function takes the first and the second elements and calculates a hash for the array of them.
3. Then the function takes the hash value, calculated at the previous step, and the third element of the initial hash array, and calculates a hash for the array of them.
4. The previous step is repeated for all the remaining elements of the initial hash array.

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

A [UInt64](../../data_types/int_uint.md) data type hash value.

**Example**

```sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type
```
```text
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## sipHash128 {#hash_functions-siphash128}

Calculates SipHash from a string.
Accepts a String-type argument. Returns FixedString(16).
Differs from sipHash64 in that the final xor-folding state is only done up to 128 bits.

## cityHash64

Produces a 64-bit [CityHash](https://github.com/google/cityhash) hash value.

```sql
cityHash64(par1,...)
```

This is a fast non-cryptographic hash function. It uses the CityHash algorithm for string parameters and implementation-specific fast non-cryptographic hash function for parameters with other data types. The function uses the CityHash combinator to get the final results.

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

A [UInt64](../../data_types/int_uint.md) data type hash value.

**Examples**

Call example:

```sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type
```
```text
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

The following example shows how to compute the checksum of the entire table with accuracy up to the row order:

```sql
SELECT groupBitXor(cityHash64(*)) FROM table
```


## intHash32

Calculates a 32-bit hash code from any type of integer.
This is a relatively fast non-cryptographic hash function of average quality for numbers.

## intHash64

Calculates a 64-bit hash code from any type of integer.
It works faster than intHash32. Average quality.

## SHA1

## SHA224

## SHA256

Calculates SHA-1, SHA-224, or SHA-256 from a string and returns the resulting set of bytes as FixedString(20), FixedString(28), or FixedString(32).
The function works fairly slowly (SHA-1 processes about 5 million short strings per second per processor core, while SHA-224 and SHA-256 process about 2.2 million).
We recommend using this function only in cases when you need a specific hash function and you can't select it.
Even in these cases, we recommend applying the function offline and pre-calculating values when inserting them into the table, instead of applying it in SELECTS.

## URLHash(url\[, N\])

A fast, decent-quality non-cryptographic hash function for a string obtained from a URL using some type of normalization.
`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/`,`?` or `#` at the end, if present.
`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` or `#` at the end, if present.
Levels are the same as in URLHierarchy. This function is specific to Yandex.Metrica.

## farmHash64

Produces a 64-bit [FarmHash](https://github.com/google/farmhash) hash value.

```sql
farmHash64(par1, ...)
```

The function uses the `Hash64` method from all [available methods](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

A [UInt64](../../data_types/int_uint.md) data type hash value.

**Example**

```sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type
```
```text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash {#hash_functions-javahash}

Calculates [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) from a string. This hash function is neither fast nor having a good quality. The only reason to use it is when this algorithm is already used in another system and you have to calculate exactly the same result.

```sql
SELECT javaHash('');
```

**Returned value**

A `Int32` data type hash value.

Type: `javaHash`.

**Example**

Query:

```sql
SELECT javaHash('Hello, world!');
```

Result:

```text
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## hiveHash {#hash_functions-hivehash}

Calculates `HiveHash` from a string.

```sql
SELECT hiveHash('');
```

This is just [JavaHash](#hash_functions-javahash) with zeroed out sign bit. This function is used in [Apache Hive](https://en.wikipedia.org/wiki/Apache_Hive) for versions before 3.0. This hash function is neither fast nor having a good quality. The only reason to use it is when this algorithm is already used in another system and you have to calculate exactly the same result.

**Returned value**

A `Int32` data type hash value.

Type: `hiveHash`.

**Example**

Query:

```sql
SELECT hiveHash('Hello, world!');
```

Result:

```text
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## metroHash64

Produces a 64-bit [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) hash value.

```sql
metroHash64(par1, ...)
```

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

A [UInt64](../../data_types/int_uint.md) data type hash value.

**Example**

```sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type
```
```text
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## jumpConsistentHash

Calculates JumpConsistentHash form a UInt64.
Accepts two arguments: a UInt64-type key and the number of buckets. Returns Int32.
For more information, see the link: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2_32, murmurHash2_64

Produces a [MurmurHash2](https://github.com/aappleby/smhasher) hash value.

```sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**Parameters**

Both functions take a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

- The `murmurHash2_32` function returns hash value having the [UInt32](../../data_types/int_uint.md) data type.
- The `murmurHash2_64` function returns hash value having the [UInt64](../../data_types/int_uint.md) data type.

**Example**

```sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type
```
```text
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## murmurHash3_32, murmurHash3_64

Produces a [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

```sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**Parameters**

Both functions take a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

- The `murmurHash3_32` function returns a [UInt32](../../data_types/int_uint.md) data type hash value.
- The `murmurHash3_64` function returns a [UInt64](../../data_types/int_uint.md) data type hash value.

**Example**

```sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type
```
```text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3_128

Produces a 128-bit [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

```sql
murmurHash3_128( expr )
```

**Parameters**

- `expr` — [Expressions](../syntax.md#syntax-expressions) returning a [String](../../data_types/string.md)-type value.

**Returned Value**

A [FixedString(16)](../../data_types/fixedstring.md) data type hash value.

**Example**

```sql
SELECT murmurHash3_128('example_string') AS MurmurHash3, toTypeName(MurmurHash3) AS type
```
```text
┌─MurmurHash3──────┬─type────────────┐
│ 6�1�4"S5KT�~~q │ FixedString(16) │
└──────────────────┴─────────────────┘
```

## xxHash32, xxHash64 {#hash_functions-xxhash32}

Calculates `xxHash` from a string. It is proposed in two flavors, 32 and 64 bits.

```sql
SELECT xxHash32('');

OR

SELECT xxHash64('');
```

**Returned value**

A `Uint32` or `Uint64` data type hash value.

Type: `xxHash`.

**Example**

Query:

```sql
SELECT xxHash32('Hello, world!');
```

Result:

```text
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**See Also**

- [xxHash](http://cyan4973.github.io/xxHash/).

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/hash_functions/) <!--hide-->
