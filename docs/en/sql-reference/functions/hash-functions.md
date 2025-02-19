---
slug: /en/sql-reference/functions/hash-functions
sidebar_position: 85
sidebar_label: Hash
---

# Hash Functions

Hash functions can be used for the deterministic pseudo-random shuffling of elements.

Simhash is a hash function, which returns close hash values for close (similar) arguments.

## halfMD5

[Interprets](../functions/type-conversion-functions.md/#type_conversion_functions-reinterpretAsString) all the input parameters as strings and calculates the [MD5](https://en.wikipedia.org/wiki/MD5) hash value for each of them. Then combines hashes, takes the first 8 bytes of the hash of the resulting string, and interprets them as `UInt64` in big-endian byte order.

```sql
halfMD5(par1, ...)
```

The function is relatively slow (5 million short strings per second per processor core).
Consider using the [sipHash64](#siphash64) function instead.

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../data-types/index.md). For some data types calculated value of hash function may be the same for the same values even if types of arguments differ (integers of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data).

**Returned Value**

A [UInt64](../data-types/int-uint.md) data type hash value.

**Example**

```sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type;
```

```response
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD4

Calculates the MD4 from a string and returns the resulting set of bytes as FixedString(16).

## MD5

Calculates the MD5 from a string and returns the resulting set of bytes as FixedString(16).
If you do not need MD5 in particular, but you need a decent cryptographic 128-bit hash, use the ‘sipHash128’ function instead.
If you want to get the same result as output by the md5sum utility, use lower(hex(MD5(s))).

## RIPEMD160

Produces [RIPEMD-160](https://en.wikipedia.org/wiki/RIPEMD) hash value.

**Syntax**

```sql
RIPEMD160(input)
```

**Parameters**

- `input`: Input string. [String](../data-types/string.md)

**Returned value**

- A 160-bit `RIPEMD-160` hash value of type [FixedString(20)](../data-types/fixedstring.md).

**Example**

Use the [hex](../functions/encoding-functions.md/#hex) function to represent the result as a hex-encoded string.

Query:

```sql
SELECT HEX(RIPEMD160('The quick brown fox jumps over the lazy dog'));
```

```response
┌─HEX(RIPEMD160('The quick brown fox jumps over the lazy dog'))─┐
│ 37F332F68DB77BD9D7EDD4969571AD671CF9DD3B                      │
└───────────────────────────────────────────────────────────────┘
```

## sipHash64

Produces a 64-bit [SipHash](https://en.wikipedia.org/wiki/SipHash) hash value.

```sql
sipHash64(par1,...)
```

This is a cryptographic hash function. It works at least three times faster than the [MD5](#md5) hash function.

The function [interprets](../functions/type-conversion-functions.md/#type_conversion_functions-reinterpretAsString) all the input parameters as strings and calculates the hash value for each of them. It then combines the hashes by the following algorithm:

1. The first and the second hash value are concatenated to an array which is hashed.
2. The previously calculated hash value and the hash of the third input parameter are hashed in a similar way.
3. This calculation is repeated for all remaining hash values of the original input.

**Arguments**

The function takes a variable number of input parameters of any of the [supported data types](../data-types/index.md).

**Returned Value**

A [UInt64](../data-types/int-uint.md) data type hash value.

Note that the calculated hash values may be equal for the same input values of different argument types. This affects for example integer types of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data.

**Example**

```sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type;
```

```response
┌──────────────SipHash─┬─type───┐
│ 11400366955626497465 │ UInt64 │
└──────────────────────┴────────┘
```

## sipHash64Keyed

Same as [sipHash64](#siphash64) but additionally takes an explicit key argument instead of using a fixed key.

**Syntax**

```sql
sipHash64Keyed((k0, k1), par1,...)
```

**Arguments**

Same as [sipHash64](#siphash64), but the first argument is a tuple of two UInt64 values representing the key.

**Returned value**

A [UInt64](../data-types/int-uint.md) data type hash value.

**Example**

Query:

```sql
SELECT sipHash64Keyed((506097522914230528, 1084818905618843912), array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type;
```

```response
┌─────────────SipHash─┬─type───┐
│ 8017656310194184311 │ UInt64 │
└─────────────────────┴────────┘
```

## sipHash128

Like [sipHash64](#siphash64) but produces a 128-bit hash value, i.e. the final xor-folding state is done up to 128 bits.

:::note
This 128-bit variant differs from the reference implementation and it's weaker.
This version exists because, when it was written, there was no official 128-bit extension for SipHash.
New projects should probably use [sipHash128Reference](#siphash128reference).
:::

**Syntax**

```sql
sipHash128(par1,...)
```

**Arguments**

Same as for [sipHash64](#siphash64).

**Returned value**

A 128-bit `SipHash` hash value of type [FixedString(16)](../data-types/fixedstring.md).

**Example**

Query:

```sql
SELECT hex(sipHash128('foo', '\x01', 3));
```

Result:

```response
┌─hex(sipHash128('foo', '', 3))────┐
│ 9DE516A64A414D4B1B609415E4523F24 │
└──────────────────────────────────┘
```

## sipHash128Keyed

Same as [sipHash128](#siphash128) but additionally takes an explicit key argument instead of using a fixed key.

:::note
This 128-bit variant differs from the reference implementation and it's weaker.
This version exists because, when it was written, there was no official 128-bit extension for SipHash.
New projects should probably use [sipHash128ReferenceKeyed](#siphash128referencekeyed).
:::

**Syntax**

```sql
sipHash128Keyed((k0, k1), par1,...)
```

**Arguments**

Same as [sipHash128](#siphash128), but the first argument is a tuple of two UInt64 values representing the key.

**Returned value**

A 128-bit `SipHash` hash value of type [FixedString(16)](../data-types/fixedstring.md).

**Example**

Query:

```sql
SELECT hex(sipHash128Keyed((506097522914230528, 1084818905618843912),'foo', '\x01', 3));
```

Result:

```response
┌─hex(sipHash128Keyed((506097522914230528, 1084818905618843912), 'foo', '', 3))─┐
│ B8467F65C8B4CFD9A5F8BD733917D9BF                                              │
└───────────────────────────────────────────────────────────────────────────────┘
```

## sipHash128Reference

Like [sipHash128](#siphash128) but implements the 128-bit algorithm from the original authors of SipHash.

**Syntax**

```sql
sipHash128Reference(par1,...)
```

**Arguments**

Same as for [sipHash128](#siphash128).

**Returned value**

A 128-bit `SipHash` hash value of type [FixedString(16)](../data-types/fixedstring.md).

**Example**

Query:

```sql
SELECT hex(sipHash128Reference('foo', '\x01', 3));
```

Result:

```response
┌─hex(sipHash128Reference('foo', '', 3))─┐
│ 4D1BE1A22D7F5933C0873E1698426260       │
└────────────────────────────────────────┘
```

## sipHash128ReferenceKeyed

Same as [sipHash128Reference](#siphash128reference) but additionally takes an explicit key argument instead of using a fixed key.

**Syntax**

```sql
sipHash128ReferenceKeyed((k0, k1), par1,...)
```

**Arguments**

Same as [sipHash128Reference](#siphash128reference), but the first argument is a tuple of two UInt64 values representing the key.

**Returned value**

A 128-bit `SipHash` hash value of type [FixedString(16)](../data-types/fixedstring.md).

**Example**

Query:

```sql
SELECT hex(sipHash128ReferenceKeyed((506097522914230528, 1084818905618843912),'foo', '\x01', 3));
```

Result:

```response
┌─hex(sipHash128ReferenceKeyed((506097522914230528, 1084818905618843912), 'foo', '', 3))─┐
│ 630133C9722DC08646156B8130C4CDC8                                                       │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## cityHash64

Produces a 64-bit [CityHash](https://github.com/google/cityhash) hash value.

```sql
cityHash64(par1,...)
```

This is a fast non-cryptographic hash function. It uses the CityHash algorithm for string parameters and implementation-specific fast non-cryptographic hash function for parameters with other data types. The function uses the CityHash combinator to get the final results.

Note that Google changed the algorithm of CityHash after it has been added to ClickHouse. In other words, ClickHouse's cityHash64 and Google's upstream CityHash now produce different results. ClickHouse cityHash64 corresponds to CityHash v1.0.2.

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../data-types/index.md). For some data types calculated value of hash function may be the same for the same values even if types of arguments differ (integers of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data).

**Returned Value**

A [UInt64](../data-types/int-uint.md) data type hash value.

**Examples**

Call example:

```sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type;
```

```response
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

**Syntax**

```sql
intHash32(int)
```

**Arguments**

- `int` — Integer to hash. [(U)Int*](../data-types/int-uint.md).

**Returned value**

- 32-bit hash code. [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT intHash32(42);
```

Result:

```response
┌─intHash32(42)─┐
│    1228623923 │
└───────────────┘
```

## intHash64

Calculates a 64-bit hash code from any type of integer.
This is a relatively fast non-cryptographic hash function of average quality for numbers.
It works faster than [intHash32](#inthash32).

**Syntax**

```sql
intHash64(int)
```

**Arguments**

- `int` — Integer to hash. [(U)Int*](../data-types/int-uint.md).

**Returned value**

- 64-bit hash code. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT intHash64(42);
```

Result:

```response
┌────────intHash64(42)─┐
│ 11490350930367293593 │
└──────────────────────┘
```

## SHA1, SHA224, SHA256, SHA512, SHA512_256

Calculates SHA-1, SHA-224, SHA-256, SHA-512, SHA-512-256 hash from a string and returns the resulting set of bytes as [FixedString](../data-types/fixedstring.md).

**Syntax**

```sql
SHA1('s')
...
SHA512('s')
```

The function works fairly slowly (SHA-1 processes about 5 million short strings per second per processor core, while SHA-224 and SHA-256 process about 2.2 million).
We recommend using this function only in cases when you need a specific hash function and you can’t select it.
Even in these cases, we recommend applying the function offline and pre-calculating values when inserting them into the table, instead of applying it in `SELECT` queries.

**Arguments**

- `s` — Input string for SHA hash calculation. [String](../data-types/string.md).

**Returned value**

- SHA hash as a hex-unencoded FixedString. SHA-1 returns as FixedString(20), SHA-224 as FixedString(28), SHA-256 — FixedString(32), SHA-512 — FixedString(64). [FixedString](../data-types/fixedstring.md).

**Example**

Use the [hex](../functions/encoding-functions.md/#hex) function to represent the result as a hex-encoded string.

Query:

```sql
SELECT hex(SHA1('abc'));
```

Result:

```response
┌─hex(SHA1('abc'))─────────────────────────┐
│ A9993E364706816ABA3E25717850C26C9CD0D89D │
└──────────────────────────────────────────┘
```

## BLAKE3

Calculates BLAKE3 hash string and returns the resulting set of bytes as [FixedString](../data-types/fixedstring.md).

**Syntax**

```sql
BLAKE3('s')
```

This cryptographic hash-function is integrated into ClickHouse with BLAKE3 Rust library. The function is rather fast and shows approximately two times faster performance compared to SHA-2, while generating hashes of the same length as SHA-256.

**Arguments**

- s - input string for BLAKE3 hash calculation. [String](../data-types/string.md).

**Return value**

- BLAKE3 hash as a byte array with type FixedString(32). [FixedString](../data-types/fixedstring.md).

**Example**

Use function [hex](../functions/encoding-functions.md/#hex) to represent the result as a hex-encoded string.

Query:
```sql
SELECT hex(BLAKE3('ABC'))
```

Result:
```sql
┌─hex(BLAKE3('ABC'))───────────────────────────────────────────────┐
│ D1717274597CF0289694F75D96D444B992A096F1AFD8E7BBFA6EBB1D360FEDFC │
└──────────────────────────────────────────────────────────────────┘
```

## URLHash(url\[, N\])

A fast, decent-quality non-cryptographic hash function for a string obtained from a URL using some type of normalization.
`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/`,`?` or `#` at the end, if present.
`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` or `#` at the end, if present.
Levels are the same as in URLHierarchy.

## farmFingerprint64

## farmHash64

Produces a 64-bit [FarmHash](https://github.com/google/farmhash) or Fingerprint value. `farmFingerprint64` is preferred for a stable and portable value.

```sql
farmFingerprint64(par1, ...)
farmHash64(par1, ...)
```

These functions use the `Fingerprint64` and `Hash64` methods respectively from all [available methods](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../data-types/index.md). For some data types calculated value of hash function may be the same for the same values even if types of arguments differ (integers of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data).

**Returned Value**

A [UInt64](../data-types/int-uint.md) data type hash value.

**Example**

```sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type;
```

```response
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash

Calculates JavaHash from a [string](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452),
[Byte](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Byte.java#l405),
[Short](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Short.java#l410),
[Integer](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Integer.java#l959),
[Long](https://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/Long.java#l1060).
This hash function is neither fast nor having a good quality. The only reason to use it is when this algorithm is already used in another system and you have to calculate exactly the same result.

Note that Java only support calculating signed integers hash, so if you want to calculate unsigned integers hash you must cast it to proper signed ClickHouse types.

**Syntax**

```sql
SELECT javaHash('')
```

**Returned value**

A `Int32` data type hash value.

**Example**

Query:

```sql
SELECT javaHash(toInt32(123));
```

Result:

```response
┌─javaHash(toInt32(123))─┐
│               123      │
└────────────────────────┘
```

Query:

```sql
SELECT javaHash('Hello, world!');
```

Result:

```response
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## javaHashUTF16LE

Calculates [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) from a string, assuming it contains bytes representing a string in UTF-16LE encoding.

**Syntax**

```sql
javaHashUTF16LE(stringUtf16le)
```

**Arguments**

- `stringUtf16le` — a string in UTF-16LE encoding.

**Returned value**

A `Int32` data type hash value.

**Example**

Correct query with UTF-16LE encoded string.

Query:

```sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'));
```

Result:

```response
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## hiveHash

Calculates `HiveHash` from a string.

```sql
SELECT hiveHash('')
```

This is just [JavaHash](#javahash) with zeroed out sign bit. This function is used in [Apache Hive](https://en.wikipedia.org/wiki/Apache_Hive) for versions before 3.0. This hash function is neither fast nor having a good quality. The only reason to use it is when this algorithm is already used in another system and you have to calculate exactly the same result.

**Returned value**

- `hiveHash` hash value. [Int32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT hiveHash('Hello, world!');
```

Result:

```response
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## metroHash64

Produces a 64-bit [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) hash value.

```sql
metroHash64(par1, ...)
```

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../data-types/index.md). For some data types calculated value of hash function may be the same for the same values even if types of arguments differ (integers of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data).

**Returned Value**

A [UInt64](../data-types/int-uint.md) data type hash value.

**Example**

```sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type;
```

```response
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## jumpConsistentHash

Calculates JumpConsistentHash form a UInt64.
Accepts two arguments: a UInt64-type key and the number of buckets. Returns Int32.
For more information, see the link: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## kostikConsistentHash

An O(1) time and space consistent hash algorithm by Konstantin 'kostik' Oblakov. Previously `yandexConsistentHash`.

**Syntax**

```sql
kostikConsistentHash(input, n)
```

Alias: `yandexConsistentHash` (left for backwards compatibility sake).

**Parameters**

- `input`: A UInt64-type key [UInt64](../data-types/int-uint.md).
- `n`: Number of buckets. [UInt16](../data-types/int-uint.md).

**Returned value**

- A [UInt16](../data-types/int-uint.md) data type hash value.

**Implementation details**

It is efficient only if n <= 32768.

**Example**

Query:

```sql
SELECT kostikConsistentHash(16045690984833335023, 2);
```

```response
┌─kostikConsistentHash(16045690984833335023, 2)─┐
│                                             1 │
└───────────────────────────────────────────────┘
```

## murmurHash2_32, murmurHash2_64

Produces a [MurmurHash2](https://github.com/aappleby/smhasher) hash value.

```sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**Arguments**

Both functions take a variable number of input parameters. Arguments can be any of the [supported data types](../data-types/index.md). For some data types calculated value of hash function may be the same for the same values even if types of arguments differ (integers of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data).

**Returned Value**

- The `murmurHash2_32` function returns hash value having the [UInt32](../data-types/int-uint.md) data type.
- The `murmurHash2_64` function returns hash value having the [UInt64](../data-types/int-uint.md) data type.

**Example**

```sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type;
```

```response
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## gccMurmurHash

Calculates a 64-bit [MurmurHash2](https://github.com/aappleby/smhasher) hash value using the same hash seed as [gcc](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191). It is portable between Clang and GCC builds.

**Syntax**

```sql
gccMurmurHash(par1, ...)
```

**Arguments**

- `par1, ...` — A variable number of parameters that can be any of the [supported data types](../data-types/index.md/#data_types).

**Returned value**

- Calculated hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
    gccMurmurHash(1, 2, 3) AS res1,
    gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2
```

Result:

```response
┌─────────────────res1─┬────────────────res2─┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘
```


## kafkaMurmurHash

Calculates a 32-bit [MurmurHash2](https://github.com/aappleby/smhasher) hash value using the same hash seed as [Kafka](https://github.com/apache/kafka/blob/461c5cfe056db0951d9b74f5adc45973670404d7/clients/src/main/java/org/apache/kafka/common/utils/Utils.java#L482) and without the highest bit to be compatible with [Default Partitioner](https://github.com/apache/kafka/blob/139f7709bd3f5926901a21e55043388728ccca78/clients/src/main/java/org/apache/kafka/clients/producer/internals/BuiltInPartitioner.java#L328).

**Syntax**

```sql
MurmurHash(par1, ...)
```

**Arguments**

- `par1, ...` — A variable number of parameters that can be any of the [supported data types](../data-types/index.md/#data_types).

**Returned value**

- Calculated hash value. [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT
    kafkaMurmurHash('foobar') AS res1,
    kafkaMurmurHash(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS res2
```

Result:

```response
┌───────res1─┬─────res2─┐
│ 1357151166 │ 85479775 │
└────────────┴──────────┘
```

## murmurHash3_32, murmurHash3_64

Produces a [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

```sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**Arguments**

Both functions take a variable number of input parameters. Arguments can be any of the [supported data types](../data-types/index.md). For some data types calculated value of hash function may be the same for the same values even if types of arguments differ (integers of different size, named and unnamed `Tuple` with the same data, `Map` and the corresponding `Array(Tuple(key, value))` type with the same data).

**Returned Value**

- The `murmurHash3_32` function returns a [UInt32](../data-types/int-uint.md) data type hash value.
- The `murmurHash3_64` function returns a [UInt64](../data-types/int-uint.md) data type hash value.

**Example**

```sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;
```

```response
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3_128

Produces a 128-bit [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

**Syntax**

```sql
murmurHash3_128(expr)
```

**Arguments**

- `expr` — A list of [expressions](../syntax.md/#syntax-expressions). [String](../data-types/string.md).

**Returned value**

A 128-bit `MurmurHash3` hash value. [FixedString(16)](../data-types/fixedstring.md).

**Example**

Query:

```sql
SELECT hex(murmurHash3_128('foo', 'foo', 'foo'));
```

Result:

```response
┌─hex(murmurHash3_128('foo', 'foo', 'foo'))─┐
│ F8F7AD9B6CD4CF117A71E277E2EC2931          │
└───────────────────────────────────────────┘
```

## xxh3

Produces a 64-bit [xxh3](https://github.com/Cyan4973/xxHash) hash value.

**Syntax**

```sql
xxh3(expr)
```

**Arguments**

- `expr` — A list of [expressions](../syntax.md/#syntax-expressions) of any data type.

**Returned value**

A 64-bit `xxh3` hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT xxh3('Hello', 'world')
```

Result:

```response
┌─xxh3('Hello', 'world')─┐
│    5607458076371731292 │
└────────────────────────┘
```

## xxHash32, xxHash64

Calculates `xxHash` from a string. It is proposed in two flavors, 32 and 64 bits.

```sql
SELECT xxHash32('')

OR

SELECT xxHash64('')
```

**Returned value**

- Hash value. [UInt32/64](../data-types/int-uint.md).

:::note
The return type will be `UInt32` for `xxHash32` and `UInt64` for `xxHash64`.
:::

**Example**

Query:

```sql
SELECT xxHash32('Hello, world!');
```

Result:

```response
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**See Also**

- [xxHash](http://cyan4973.github.io/xxHash/).

## ngramSimHash

Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`. Is case sensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../functions/bit-functions.md/#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

```sql
ngramSimHash(string[, ngramsize])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT ngramSimHash('ClickHouse') AS Hash;
```

Result:

```response
┌───────Hash─┐
│ 1627567969 │
└────────────┘
```

## ngramSimHashCaseInsensitive

Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`. Is case insensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../functions/bit-functions.md/#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

```sql
ngramSimHashCaseInsensitive(string[, ngramsize])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT ngramSimHashCaseInsensitive('ClickHouse') AS Hash;
```

Result:

```response
┌──────Hash─┐
│ 562180645 │
└───────────┘
```

## ngramSimHashUTF8

Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`. Is case sensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../functions/bit-functions.md/#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

```sql
ngramSimHashUTF8(string[, ngramsize])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT ngramSimHashUTF8('ClickHouse') AS Hash;
```

Result:

```response
┌───────Hash─┐
│ 1628157797 │
└────────────┘
```

## ngramSimHashCaseInsensitiveUTF8

Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`. Is case insensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../functions/bit-functions.md/#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

```sql
ngramSimHashCaseInsensitiveUTF8(string[, ngramsize])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT ngramSimHashCaseInsensitiveUTF8('ClickHouse') AS Hash;
```

Result:

```response
┌───────Hash─┐
│ 1636742693 │
└────────────┘
```

## wordShingleSimHash

Splits a ASCII string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`. Is case sensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../functions/bit-functions.md/#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

```sql
wordShingleSimHash(string[, shinglesize])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT wordShingleSimHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Result:

```response
┌───────Hash─┐
│ 2328277067 │
└────────────┘
```

## wordShingleSimHashCaseInsensitive

Splits a ASCII string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`. Is case insensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../functions/bit-functions.md/#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

```sql
wordShingleSimHashCaseInsensitive(string[, shinglesize])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT wordShingleSimHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Result:

```response
┌───────Hash─┐
│ 2194812424 │
└────────────┘
```

## wordShingleSimHashUTF8

Splits a UTF-8 string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`. Is case sensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../functions/bit-functions.md/#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

```sql
wordShingleSimHashUTF8(string[, shinglesize])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT wordShingleSimHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Result:

```response
┌───────Hash─┐
│ 2328277067 │
└────────────┘
```

## wordShingleSimHashCaseInsensitiveUTF8

Splits a UTF-8 string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`. Is case insensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../functions/bit-functions.md/#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

```sql
wordShingleSimHashCaseInsensitiveUTF8(string[, shinglesize])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT wordShingleSimHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Result:

```response
┌───────Hash─┐
│ 2194812424 │
└────────────┘
```

## wyHash64

Produces a 64-bit [wyHash64](https://github.com/wangyi-fudan/wyhash) hash value.

**Syntax**

```sql
wyHash64(string)
```

**Arguments**

- `string` — String. [String](../data-types/string.md).

**Returned value**

- Hash value. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT wyHash64('ClickHouse') AS Hash;
```

Result:

```response
┌─────────────────Hash─┐
│ 12336419557878201794 │
└──────────────────────┘
```

## ngramMinHash

Splits a ASCII string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case sensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

```sql
ngramMinHash(string[, ngramsize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two hashes — the minimum and the maximum. [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md)).

**Example**

Query:

```sql
SELECT ngramMinHash('ClickHouse') AS Tuple;
```

Result:

```response
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,9054248444481805918) │
└────────────────────────────────────────────┘
```

## ngramMinHashCaseInsensitive

Splits a ASCII string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case insensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

```sql
ngramMinHashCaseInsensitive(string[, ngramsize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two hashes — the minimum and the maximum. [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md)).

**Example**

Query:

```sql
SELECT ngramMinHashCaseInsensitive('ClickHouse') AS Tuple;
```

Result:

```response
┌─Tuple──────────────────────────────────────┐
│ (2106263556442004574,13203602793651726206) │
└────────────────────────────────────────────┘
```

## ngramMinHashUTF8

Splits a UTF-8 string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case sensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

```sql
ngramMinHashUTF8(string[, ngramsize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two hashes — the minimum and the maximum. [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md)).

**Example**

Query:

```sql
SELECT ngramMinHashUTF8('ClickHouse') AS Tuple;
```

Result:

```response
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,6742163577938632877) │
└────────────────────────────────────────────┘
```

## ngramMinHashCaseInsensitiveUTF8

Splits a UTF-8 string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case insensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

```sql
ngramMinHashCaseInsensitiveUTF8(string [, ngramsize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two hashes — the minimum and the maximum. [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md)).

**Example**

Query:

```sql
SELECT ngramMinHashCaseInsensitiveUTF8('ClickHouse') AS Tuple;
```

Result:

```response
┌─Tuple───────────────────────────────────────┐
│ (12493625717655877135,13203602793651726206) │
└─────────────────────────────────────────────┘
```

## ngramMinHashArg

Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [ngramMinHash](#ngramminhash) function with the same input. Is case sensitive.

**Syntax**

```sql
ngramMinHashArg(string[, ngramsize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two tuples with `hashnum` n-grams each. [Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md))).

**Example**

Query:

```sql
SELECT ngramMinHashArg('ClickHouse') AS Tuple;
```

Result:

```response
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('Hou','lic','ick','ous','ckH','Cli')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgCaseInsensitive

Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [ngramMinHashCaseInsensitive](#ngramminhashcaseinsensitive) function with the same input. Is case insensitive.

**Syntax**

```sql
ngramMinHashArgCaseInsensitive(string[, ngramsize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two tuples with `hashnum` n-grams each. [Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md))).

**Example**

Query:

```sql
SELECT ngramMinHashArgCaseInsensitive('ClickHouse') AS Tuple;
```

Result:

```response
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','kHo','use','Cli'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgUTF8

Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [ngramMinHashUTF8](#ngramminhashutf8) function with the same input. Is case sensitive.

**Syntax**

```sql
ngramMinHashArgUTF8(string[, ngramsize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two tuples with `hashnum` n-grams each. [Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md))).

**Example**

Query:

```sql
SELECT ngramMinHashArgUTF8('ClickHouse') AS Tuple;
```

Result:

```response
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('kHo','Hou','lic','ick','ous','ckH')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgCaseInsensitiveUTF8

Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [ngramMinHashCaseInsensitiveUTF8](#ngramminhashcaseinsensitiveutf8) function with the same input. Is case insensitive.

**Syntax**

```sql
ngramMinHashArgCaseInsensitiveUTF8(string[, ngramsize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two tuples with `hashnum` n-grams each. [Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md))).

**Example**

Query:

```sql
SELECT ngramMinHashArgCaseInsensitiveUTF8('ClickHouse') AS Tuple;
```

Result:

```response
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ckH','ous','ick','lic','kHo','use'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHash

Splits a ASCII string into parts (shingles) of `shinglesize` words and calculates hash values for each word shingle. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case sensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

```sql
wordShingleMinHash(string[, shinglesize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two hashes — the minimum and the maximum. [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md)).

**Example**

Query:

```sql
SELECT wordShingleMinHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Result:

```response
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
```

## wordShingleMinHashCaseInsensitive

Splits a ASCII string into parts (shingles) of `shinglesize` words and calculates hash values for each word shingle. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case insensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

```sql
wordShingleMinHashCaseInsensitive(string[, shinglesize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two hashes — the minimum and the maximum. [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md)).

**Example**

Query:

```sql
SELECT wordShingleMinHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Result:

```response
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
```

## wordShingleMinHashUTF8

Splits a UTF-8 string into parts (shingles) of `shinglesize` words and calculates hash values for each word shingle. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case sensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

```sql
wordShingleMinHashUTF8(string[, shinglesize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two hashes — the minimum and the maximum. [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md)).

**Example**

Query:

```sql
SELECT wordShingleMinHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Result:

```response
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
```

## wordShingleMinHashCaseInsensitiveUTF8

Splits a UTF-8 string into parts (shingles) of `shinglesize` words and calculates hash values for each word shingle. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case insensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../functions/tuple-functions.md/#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

```sql
wordShingleMinHashCaseInsensitiveUTF8(string[, shinglesize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two hashes — the minimum and the maximum. [Tuple](../data-types/tuple.md)([UInt64](../data-types/int-uint.md), [UInt64](../data-types/int-uint.md)).

**Example**

Query:

```sql
SELECT wordShingleMinHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Result:

```response
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
```

## wordShingleMinHashArg

Splits a ASCII string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [wordshingleMinHash](#wordshingleminhash) function with the same input. Is case sensitive.

**Syntax**

```sql
wordShingleMinHashArg(string[, shinglesize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two tuples with `hashnum` word shingles each. [Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md))).

**Example**

Query:

```sql
SELECT wordShingleMinHashArg('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Result:

```response
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgCaseInsensitive

Splits a ASCII string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [wordShingleMinHashCaseInsensitive](#wordshingleminhashcaseinsensitive) function with the same input. Is case insensitive.

**Syntax**

```sql
wordShingleMinHashArgCaseInsensitive(string[, shinglesize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two tuples with `hashnum` word shingles each. [Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md))).

**Example**

Query:

```sql
SELECT wordShingleMinHashArgCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Result:

```response
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgUTF8

Splits a UTF-8 string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [wordShingleMinHashUTF8](#wordshingleminhashutf8) function with the same input. Is case sensitive.

**Syntax**

```sql
wordShingleMinHashArgUTF8(string[, shinglesize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two tuples with `hashnum` word shingles each. [Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md))).

**Example**

Query:

```sql
SELECT wordShingleMinHashArgUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Result:

```response
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgCaseInsensitiveUTF8

Splits a UTF-8 string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [wordShingleMinHashCaseInsensitiveUTF8](#wordshingleminhashcaseinsensitiveutf8) function with the same input. Is case insensitive.

**Syntax**

```sql
wordShingleMinHashArgCaseInsensitiveUTF8(string[, shinglesize, hashnum])
```

**Arguments**

- `string` — String. [String](../data-types/string.md).
- `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../data-types/int-uint.md).
- `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../data-types/int-uint.md).

**Returned value**

- Tuple with two tuples with `hashnum` word shingles each. [Tuple](../data-types/tuple.md)([Tuple](../data-types/tuple.md)([String](../data-types/string.md)), [Tuple](../data-types/tuple.md)([String](../data-types/string.md))).

**Example**

Query:

```sql
SELECT wordShingleMinHashArgCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Result:

```response
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
```

## sqidEncode

Encodes numbers as a [Sqid](https://sqids.org/) which is a YouTube-like ID string.
The output alphabet is `abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789`.
Do not use this function for hashing - the generated IDs can be decoded back into the original numbers.

**Syntax**

```sql
sqidEncode(number1, ...)
```

Alias: `sqid`

**Arguments**

- A variable number of UInt8, UInt16, UInt32 or UInt64 numbers.

**Returned Value**

A sqid [String](../data-types/string.md).

**Example**

```sql
SELECT sqidEncode(1, 2, 3, 4, 5);
```

```response
┌─sqidEncode(1, 2, 3, 4, 5)─┐
│ gXHfJ1C6dN                │
└───────────────────────────┘
```

## sqidDecode

Decodes a [Sqid](https://sqids.org/) back into its original numbers.
Returns an empty array in case the input string is not a valid sqid.

**Syntax**

```sql
sqidDecode(sqid)
```

**Arguments**

- A sqid - [String](../data-types/string.md)

**Returned Value**

The sqid transformed to numbers [Array(UInt64)](../data-types/array.md).

**Example**

```sql
SELECT sqidDecode('gXHfJ1C6dN');
```

```response
┌─sqidDecode('gXHfJ1C6dN')─┐
│ [1,2,3,4,5]              │
└──────────────────────────┘
```
