# Hash functions

Hash functions can be used for deterministic pseudo-random shuffling of elements.

## halfMD5 {#hash_functions-halfmd5}

Converts all the input parameters to strings and concatenates them. Then calculates the MD5 hash value from the resulting string, takes the first 8 bytes of the hash and interprets them as `UInt64` in big-endian byte order.

```
halfMD5(par1, ...)
```

The function works relatively slow (5 million short strings per second per processor core).
Consider using the [sipHash64](#hash_functions-siphash64) function instead.

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

Hash value having the [UInt64](../../data_types/int_uint.md) data type.

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

Produces 64-bit [SipHash](https://131002.net/siphash/) hash value.

```
sipHash64(par1,...)
```

This is a cryptographic hash function. It works at least three times faster than the [MD5](#hash_functions-md5) function.

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

Hash value having the [UInt64](../../data_types/int_uint.md) data type.

**Example**

```sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type
```
```
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## sipHash128

Calculates SipHash from a string.
Accepts a String-type argument. Returns FixedString(16).
Differs from sipHash64 in that the final xor-folding state is only done up to 128 bytes.

## cityHash64

Produces 64-bit hash value.

```
cityHash64(par1,...)
```

This is the fast non-cryptographic hash function. It uses [CityHash](https://github.com/google/cityhash) algorithm for string parameters and implementation-specific fast non-cryptographic hash function for the parameters with other data types. To get the final result, the function uses the CityHash combinator.

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

Hash value having the [UInt64](../../data_types/int_uint.md) data type.

**Examples**

Call example:

```sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type
```
```
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

The following example shows how to compute the checksum of the entire table with accuracy up to the row order:

```
SELECT sum(cityHash64(*)) FROM table
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

```
farmHash64(par1, ...)
```

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

Hash value having the [UInt64](../../data_types/int_uint.md) data type.

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

Calculates JavaHash from a string.
Accepts a String-type argument. Returns Int32.
For more information, see the link: [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452)

## hiveHash

Calculates HiveHash from a string.
Accepts a String-type argument. Returns Int32.
Same as for [JavaHash](#hash_functions-javahash), except that the return value never has a negative number.

## metroHash64

Produces a 64-bit [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) hash value.

```
metroHash64(par1, ...)
```

**Parameters**

The function takes a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

Hash value having the [UInt64](../../data_types/int_uint.md) data type.

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
Accepts a UInt64-type argument. Returns Int32.
For more information, see the link: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2_32, murmurHash2_64

Produces a [MurmurHash2](https://github.com/aappleby/smhasher) hash value.

```
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

```
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**Parameters**

Both functions take a variable number of input parameters. Parameters can be any of the [supported data types](../../data_types/index.md).

**Returned Value**

- The `murmurHash3_32` function returns hash value having the [UInt32](../../data_types/int_uint.md) data type.
- The `murmurHash3_64` function returns hash value having the [UInt64](../../data_types/int_uint.md) data type.

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

```
murmurHash3_128( expr )
```

**Parameters**

- `expr` — [Expressions](../syntax.md#syntax-expressions) returning [String](../../data_types/string.md)-typed value.

**Returned Value**

Hash value having [FixedString(16) data type](../../data_types/fixedstring.md).

**Example**

```sql
SELECT murmurHash3_128('example_string') AS MurmurHash3, toTypeName(MurmurHash3) AS type
```
```text
┌─MurmurHash3──────┬─type────────────┐
│ 6�1�4"S5KT�~~q │ FixedString(16) │
└──────────────────┴─────────────────┘
```

## xxHash32, xxHash64

Calculates xxHash from a string.
ccepts a String-type argument. Returns UInt64 Or UInt32.
For more information, see the link: [xxHash](http://cyan4973.github.io/xxHash/)

[Original article](https://clickhouse.yandex/docs/en/query_language/functions/hash_functions/) <!--hide-->
