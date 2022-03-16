---
toc_priority: 50
toc_title: Hash
---

# Hash Functions {#hash-functions}

Hash functions can be used for the deterministic pseudo-random shuffling of elements.

Simhash is a hash function, which returns close hash values for close (similar) arguments.

## halfMD5 {#hash-functions-halfmd5}

[Interprets](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) all the input parameters as strings and calculates the [MD5](https://en.wikipedia.org/wiki/MD5) hash value for each of them. Then combines hashes, takes the first 8 bytes of the hash of the resulting string, and interprets them as `UInt64` in big-endian byte order.

``` sql
halfMD5(par1, ...)
```

The function is relatively slow (5 million short strings per second per processor core).
Consider using the [sipHash64](#hash_functions-siphash64) function instead.

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../../sql-reference/data-types/index.md).

**Returned Value**

A [UInt64](../../sql-reference/data-types/int-uint.md) data type hash value.

**Example**

``` sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type;
```

``` text
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD4 {#hash_functions-md4}

Calculates the MD4 from a string and returns the resulting set of bytes as FixedString(16).

## MD5 {#hash_functions-md5}

Calculates the MD5 from a string and returns the resulting set of bytes as FixedString(16).
If you do not need MD5 in particular, but you need a decent cryptographic 128-bit hash, use the ‘sipHash128’ function instead.
If you want to get the same result as output by the md5sum utility, use lower(hex(MD5(s))).

## sipHash64 {#hash_functions-siphash64}

Produces a 64-bit [SipHash](https://131002.net/siphash/) hash value.

``` sql
sipHash64(par1,...)
```

This is a cryptographic hash function. It works at least three times faster than the [MD5](#hash_functions-md5) function.

Function [interprets](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) all the input parameters as strings and calculates the hash value for each of them. Then combines hashes by the following algorithm:

1.  After hashing all the input parameters, the function gets the array of hashes.
2.  Function takes the first and the second elements and calculates a hash for the array of them.
3.  Then the function takes the hash value, calculated at the previous step, and the third element of the initial hash array, and calculates a hash for the array of them.
4.  The previous step is repeated for all the remaining elements of the initial hash array.

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../../sql-reference/data-types/index.md).

**Returned Value**

A [UInt64](../../sql-reference/data-types/int-uint.md) data type hash value.

**Example**

``` sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type;
```

``` text
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## sipHash128 {#hash_functions-siphash128}

Produces a 128-bit [SipHash](https://131002.net/siphash/) hash value. Differs from [sipHash64](#hash_functions-siphash64) in that the final xor-folding state is done up to 128 bits.

**Syntax**

``` sql
sipHash128(par1,...)
```

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../../sql-reference/data-types/index.md).

**Returned value**

A 128-bit `SipHash` hash value.

Type: [FixedString(16)](../../sql-reference/data-types/fixedstring.md).

**Example**

Query:

``` sql
SELECT hex(sipHash128('foo', '\x01', 3));
```

Result:

``` text
┌─hex(sipHash128('foo', '', 3))────┐
│ 9DE516A64A414D4B1B609415E4523F24 │
└──────────────────────────────────┘
```

## cityHash64 {#cityhash64}

Produces a 64-bit [CityHash](https://github.com/google/cityhash) hash value.

``` sql
cityHash64(par1,...)
```

This is a fast non-cryptographic hash function. It uses the CityHash algorithm for string parameters and implementation-specific fast non-cryptographic hash function for parameters with other data types. The function uses the CityHash combinator to get the final results.

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../../sql-reference/data-types/index.md).

**Returned Value**

A [UInt64](../../sql-reference/data-types/int-uint.md) data type hash value.

**Examples**

Call example:

``` sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type;
```

``` text
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

The following example shows how to compute the checksum of the entire table with accuracy up to the row order:

``` sql
SELECT groupBitXor(cityHash64(*)) FROM table
```

## intHash32 {#inthash32}

Calculates a 32-bit hash code from any type of integer.
This is a relatively fast non-cryptographic hash function of average quality for numbers.

## intHash64 {#inthash64}

Calculates a 64-bit hash code from any type of integer.
It works faster than intHash32. Average quality.

## SHA1, SHA224, SHA256, SHA512 {#sha}

Calculates SHA-1, SHA-224, SHA-256, SHA-512 hash from a string and returns the resulting set of bytes as [FixedString](../data-types/fixedstring.md).

**Syntax**

``` sql
SHA1('s')
...
SHA512('s')
```

The function works fairly slowly (SHA-1 processes about 5 million short strings per second per processor core, while SHA-224 and SHA-256 process about 2.2 million).
We recommend using this function only in cases when you need a specific hash function and you can’t select it.
Even in these cases, we recommend applying the function offline and pre-calculating values when inserting them into the table, instead of applying it in `SELECT` queries.

**Arguments**

-   `s` — Input string for SHA hash calculation. [String](../data-types/string.md).

**Returned value**

-   SHA hash as a hex-unencoded FixedString. SHA-1 returns as FixedString(20), SHA-224 as FixedString(28), SHA-256 — FixedString(32), SHA-512 — FixedString(64).

Type: [FixedString](../data-types/fixedstring.md).

**Example**

Use the [hex](../functions/encoding-functions.md#hex) function to represent the result as a hex-encoded string.

Query:

``` sql
SELECT hex(SHA1('abc'));
```

Result:

``` text
┌─hex(SHA1('abc'))─────────────────────────┐
│ A9993E364706816ABA3E25717850C26C9CD0D89D │
└──────────────────────────────────────────┘
```

## URLHash(url\[, N\]) {#urlhashurl-n}

A fast, decent-quality non-cryptographic hash function for a string obtained from a URL using some type of normalization.
`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/`,`?` or `#` at the end, if present.
`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` or `#` at the end, if present.
Levels are the same as in URLHierarchy. 

## farmFingerprint64 {#farmfingerprint64}

## farmHash64 {#farmhash64}

Produces a 64-bit [FarmHash](https://github.com/google/farmhash) or Fingerprint value. `farmFingerprint64` is preferred for a stable and portable value.

``` sql
farmFingerprint64(par1, ...)
farmHash64(par1, ...)
```

These functions use the `Fingerprint64` and `Hash64` methods respectively from all [available methods](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../../sql-reference/data-types/index.md).

**Returned Value**

A [UInt64](../../sql-reference/data-types/int-uint.md) data type hash value.

**Example**

``` sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type;
```

``` text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash {#hash_functions-javahash}

Calculates [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) from a string. This hash function is neither fast nor having a good quality. The only reason to use it is when this algorithm is already used in another system and you have to calculate exactly the same result.

**Syntax**

``` sql
SELECT javaHash('')
```

**Returned value**

A `Int32` data type hash value.

**Example**

Query:

``` sql
SELECT javaHash('Hello, world!');
```

Result:

``` text
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## javaHashUTF16LE {#javahashutf16le}

Calculates [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) from a string, assuming it contains bytes representing a string in UTF-16LE encoding.

**Syntax**

``` sql
javaHashUTF16LE(stringUtf16le)
```

**Arguments**

-   `stringUtf16le` — a string in UTF-16LE encoding.

**Returned value**

A `Int32` data type hash value.

**Example**

Correct query with UTF-16LE encoded string.

Query:

``` sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'));
```

Result:

``` text
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## hiveHash {#hash-functions-hivehash}

Calculates `HiveHash` from a string.

``` sql
SELECT hiveHash('')
```

This is just [JavaHash](#hash_functions-javahash) with zeroed out sign bit. This function is used in [Apache Hive](https://en.wikipedia.org/wiki/Apache_Hive) for versions before 3.0. This hash function is neither fast nor having a good quality. The only reason to use it is when this algorithm is already used in another system and you have to calculate exactly the same result.

**Returned value**

A `Int32` data type hash value.

Type: `hiveHash`.

**Example**

Query:

``` sql
SELECT hiveHash('Hello, world!');
```

Result:

``` text
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## metroHash64 {#metrohash64}

Produces a 64-bit [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) hash value.

``` sql
metroHash64(par1, ...)
```

**Arguments**

The function takes a variable number of input parameters. Arguments can be any of the [supported data types](../../sql-reference/data-types/index.md).

**Returned Value**

A [UInt64](../../sql-reference/data-types/int-uint.md) data type hash value.

**Example**

``` sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type;
```

``` text
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## jumpConsistentHash {#jumpconsistenthash}

Calculates JumpConsistentHash form a UInt64.
Accepts two arguments: a UInt64-type key and the number of buckets. Returns Int32.
For more information, see the link: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2_32, murmurHash2_64 {#murmurhash2-32-murmurhash2-64}

Produces a [MurmurHash2](https://github.com/aappleby/smhasher) hash value.

``` sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**Arguments**

Both functions take a variable number of input parameters. Arguments can be any of the [supported data types](../../sql-reference/data-types/index.md).

**Returned Value**

-   The `murmurHash2_32` function returns hash value having the [UInt32](../../sql-reference/data-types/int-uint.md) data type.
-   The `murmurHash2_64` function returns hash value having the [UInt64](../../sql-reference/data-types/int-uint.md) data type.

**Example**

``` sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type;
```

``` text
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## gccMurmurHash {#gccmurmurhash}

Calculates a 64-bit [MurmurHash2](https://github.com/aappleby/smhasher) hash value using the same hash seed as [gcc](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191). It is portable between CLang and GCC builds.

**Syntax**

``` sql
gccMurmurHash(par1, ...)
```

**Arguments**

-   `par1, ...` — A variable number of parameters that can be any of the [supported data types](../../sql-reference/data-types/index.md#data_types).

**Returned value**

-   Calculated hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT
    gccMurmurHash(1, 2, 3) AS res1,
    gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2
```

Result:

``` text
┌─────────────────res1─┬────────────────res2─┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘
```

## murmurHash3_32, murmurHash3_64 {#murmurhash3-32-murmurhash3-64}

Produces a [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

``` sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**Arguments**

Both functions take a variable number of input parameters. Arguments can be any of the [supported data types](../../sql-reference/data-types/index.md).

**Returned Value**

-   The `murmurHash3_32` function returns a [UInt32](../../sql-reference/data-types/int-uint.md) data type hash value.
-   The `murmurHash3_64` function returns a [UInt64](../../sql-reference/data-types/int-uint.md) data type hash value.

**Example**

``` sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type;
```

``` text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3_128 {#murmurhash3-128}

Produces a 128-bit [MurmurHash3](https://github.com/aappleby/smhasher) hash value.

**Syntax**

``` sql
murmurHash3_128(expr)
```

**Arguments**

-   `expr` — A list of [expressions](../../sql-reference/syntax.md#syntax-expressions). [String](../../sql-reference/data-types/string.md).

**Returned value**

A 128-bit `MurmurHash3` hash value.

Type: [FixedString(16)](../../sql-reference/data-types/fixedstring.md).

**Example**

Query:

``` sql
SELECT hex(murmurHash3_128('foo', 'foo', 'foo'));
```

Result:

``` text
┌─hex(murmurHash3_128('foo', 'foo', 'foo'))─┐
│ F8F7AD9B6CD4CF117A71E277E2EC2931          │
└───────────────────────────────────────────┘
```

## xxHash32, xxHash64 {#hash-functions-xxhash32}

Calculates `xxHash` from a string. It is proposed in two flavors, 32 and 64 bits.

``` sql
SELECT xxHash32('')

OR

SELECT xxHash64('')
```

**Returned value**

A `Uint32` or `Uint64` data type hash value.

Type: `xxHash`.

**Example**

Query:

``` sql
SELECT xxHash32('Hello, world!');
```

Result:

``` text
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**See Also**

-   [xxHash](http://cyan4973.github.io/xxHash/).

## ngramSimHash {#ngramsimhash}

Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`. Is case sensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

``` sql
ngramSimHash(string[, ngramsize])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT ngramSimHash('ClickHouse') AS Hash;
```

Result:

``` text
┌───────Hash─┐
│ 1627567969 │
└────────────┘
```

## ngramSimHashCaseInsensitive {#ngramsimhashcaseinsensitive}

Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`. Is case insensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

``` sql
ngramSimHashCaseInsensitive(string[, ngramsize])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT ngramSimHashCaseInsensitive('ClickHouse') AS Hash;
```

Result:

``` text
┌──────Hash─┐
│ 562180645 │
└───────────┘
```

## ngramSimHashUTF8 {#ngramsimhashutf8}

Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`. Is case sensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

``` sql
ngramSimHashUTF8(string[, ngramsize])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT ngramSimHashUTF8('ClickHouse') AS Hash;
```

Result:

``` text
┌───────Hash─┐
│ 1628157797 │
└────────────┘
```

## ngramSimHashCaseInsensitiveUTF8 {#ngramsimhashcaseinsensitiveutf8}

Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-gram `simhash`. Is case insensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

``` sql
ngramSimHashCaseInsensitiveUTF8(string[, ngramsize])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT ngramSimHashCaseInsensitiveUTF8('ClickHouse') AS Hash;
```

Result:

``` text
┌───────Hash─┐
│ 1636742693 │
└────────────┘
```

## wordShingleSimHash {#wordshinglesimhash}

Splits a ASCII string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`. Is case sensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

``` sql
wordShingleSimHash(string[, shinglesize])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT wordShingleSimHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Result:

``` text
┌───────Hash─┐
│ 2328277067 │
└────────────┘
```

## wordShingleSimHashCaseInsensitive {#wordshinglesimhashcaseinsensitive}

Splits a ASCII string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`. Is case insensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

``` sql
wordShingleSimHashCaseInsensitive(string[, shinglesize])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT wordShingleSimHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Result:

``` text
┌───────Hash─┐
│ 2194812424 │
└────────────┘
```

## wordShingleSimHashUTF8 {#wordshinglesimhashutf8}

Splits a UTF-8 string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`. Is case sensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

``` sql
wordShingleSimHashUTF8(string[, shinglesize])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optinal. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT wordShingleSimHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Result:

``` text
┌───────Hash─┐
│ 2328277067 │
└────────────┘
```

## wordShingleSimHashCaseInsensitiveUTF8 {#wordshinglesimhashcaseinsensitiveutf8}

Splits a UTF-8 string into parts (shingles) of `shinglesize` words and returns the word shingle `simhash`. Is case insensitive.

Can be used for detection of semi-duplicate strings with [bitHammingDistance](../../sql-reference/functions/bit-functions.md#bithammingdistance). The smaller is the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) of the calculated `simhashes` of two strings, the more likely these strings are the same.

**Syntax**

``` sql
wordShingleSimHashCaseInsensitiveUTF8(string[, shinglesize])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT wordShingleSimHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Hash;
```

Result:

``` text
┌───────Hash─┐
│ 2194812424 │
└────────────┘
```

## ngramMinHash {#ngramminhash}

Splits a ASCII string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case sensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

``` sql
ngramMinHash(string[, ngramsize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two hashes — the minimum and the maximum.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT ngramMinHash('ClickHouse') AS Tuple;
```

Result:

``` text
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,9054248444481805918) │
└────────────────────────────────────────────┘
```

## ngramMinHashCaseInsensitive {#ngramminhashcaseinsensitive}

Splits a ASCII string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case insensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

``` sql
ngramMinHashCaseInsensitive(string[, ngramsize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two hashes — the minimum and the maximum.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT ngramMinHashCaseInsensitive('ClickHouse') AS Tuple;
```

Result:

``` text
┌─Tuple──────────────────────────────────────┐
│ (2106263556442004574,13203602793651726206) │
└────────────────────────────────────────────┘
```

## ngramMinHashUTF8 {#ngramminhashutf8}

Splits a UTF-8 string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case sensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

``` sql
ngramMinHashUTF8(string[, ngramsize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two hashes — the minimum and the maximum.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT ngramMinHashUTF8('ClickHouse') AS Tuple;
```

Result:

``` text
┌─Tuple──────────────────────────────────────┐
│ (18333312859352735453,6742163577938632877) │
└────────────────────────────────────────────┘
```

## ngramMinHashCaseInsensitiveUTF8 {#ngramminhashcaseinsensitiveutf8}

Splits a UTF-8 string into n-grams of `ngramsize` symbols and calculates hash values for each n-gram. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case insensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

``` sql
ngramMinHashCaseInsensitiveUTF8(string [, ngramsize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two hashes — the minimum and the maximum.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT ngramMinHashCaseInsensitiveUTF8('ClickHouse') AS Tuple;
```

Result:

``` text
┌─Tuple───────────────────────────────────────┐
│ (12493625717655877135,13203602793651726206) │
└─────────────────────────────────────────────┘
```

## ngramMinHashArg {#ngramminhasharg}

Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [ngramMinHash](#ngramminhash) function with the same input. Is case sensitive.

**Syntax**

``` sql
ngramMinHashArg(string[, ngramsize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two tuples with `hashnum` n-grams each.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Example**

Query:

``` sql
SELECT ngramMinHashArg('ClickHouse') AS Tuple;
```

Result:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('Hou','lic','ick','ous','ckH','Cli')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgCaseInsensitive {#ngramminhashargcaseinsensitive}

Splits a ASCII string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [ngramMinHashCaseInsensitive](#ngramminhashcaseinsensitive) function with the same input. Is case insensitive.

**Syntax**

``` sql
ngramMinHashArgCaseInsensitive(string[, ngramsize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two tuples with `hashnum` n-grams each.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Example**

Query:

``` sql
SELECT ngramMinHashArgCaseInsensitive('ClickHouse') AS Tuple;
```

Result:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','kHo','use','Cli'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgUTF8 {#ngramminhashargutf8}

Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [ngramMinHashUTF8](#ngramminhashutf8) function with the same input. Is case sensitive.

**Syntax**

``` sql
ngramMinHashArgUTF8(string[, ngramsize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two tuples with `hashnum` n-grams each.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Example**

Query:

``` sql
SELECT ngramMinHashArgUTF8('ClickHouse') AS Tuple;
```

Result:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ous','ick','lic','Hou','kHo','use'),('kHo','Hou','lic','ick','ous','ckH')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## ngramMinHashArgCaseInsensitiveUTF8 {#ngramminhashargcaseinsensitiveutf8}

Splits a UTF-8 string into n-grams of `ngramsize` symbols and returns the n-grams with minimum and maximum hashes, calculated by the [ngramMinHashCaseInsensitiveUTF8](#ngramminhashcaseinsensitiveutf8) function with the same input. Is case insensitive.

**Syntax**

``` sql
ngramMinHashArgCaseInsensitiveUTF8(string[, ngramsize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `ngramsize` — The size of an n-gram. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two tuples with `hashnum` n-grams each.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Example**

Query:

``` sql
SELECT ngramMinHashArgCaseInsensitiveUTF8('ClickHouse') AS Tuple;
```

Result:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────────────┐
│ (('ckH','ous','ick','lic','kHo','use'),('kHo','lic','ick','ous','ckH','Hou')) │
└───────────────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHash {#wordshingleminhash}

Splits a ASCII string into parts (shingles) of `shinglesize` words and calculates hash values for each word shingle. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case sensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

``` sql
wordShingleMinHash(string[, shinglesize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two hashes — the minimum and the maximum.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT wordShingleMinHash('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Result:

``` text
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
```

## wordShingleMinHashCaseInsensitive {#wordshingleminhashcaseinsensitive}

Splits a ASCII string into parts (shingles) of `shinglesize` words and calculates hash values for each word shingle. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case insensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

``` sql
wordShingleMinHashCaseInsensitive(string[, shinglesize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two hashes — the minimum and the maximum.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT wordShingleMinHashCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Result:

``` text
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
```

## wordShingleMinHashUTF8 {#wordshingleminhashutf8}

Splits a UTF-8 string into parts (shingles) of `shinglesize` words and calculates hash values for each word shingle. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case sensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

``` sql
wordShingleMinHashUTF8(string[, shinglesize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two hashes — the minimum and the maximum.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT wordShingleMinHashUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Result:

``` text
┌─Tuple──────────────────────────────────────┐
│ (16452112859864147620,5844417301642981317) │
└────────────────────────────────────────────┘
```

## wordShingleMinHashCaseInsensitiveUTF8 {#wordshingleminhashcaseinsensitiveutf8}

Splits a UTF-8 string into parts (shingles) of `shinglesize` words and calculates hash values for each word shingle. Uses `hashnum` minimum hashes to calculate the minimum hash and `hashnum` maximum hashes to calculate the maximum hash. Returns a tuple with these hashes. Is case insensitive.

Can be used for detection of semi-duplicate strings with [tupleHammingDistance](../../sql-reference/functions/tuple-functions.md#tuplehammingdistance). For two strings: if one of the returned hashes is the same for both strings, we think that those strings are the same.

**Syntax**

``` sql
wordShingleMinHashCaseInsensitiveUTF8(string[, shinglesize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two hashes — the minimum and the maximum.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([UInt64](../../sql-reference/data-types/int-uint.md), [UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT wordShingleMinHashCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).') AS Tuple;
```

Result:

``` text
┌─Tuple─────────────────────────────────────┐
│ (3065874883688416519,1634050779997673240) │
└───────────────────────────────────────────┘
```

## wordShingleMinHashArg {#wordshingleminhasharg}

Splits a ASCII string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [wordshingleMinHash](#wordshingleminhash) function with the same input. Is case sensitive.

**Syntax**

``` sql
wordShingleMinHashArg(string[, shinglesize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two tuples with `hashnum` word shingles each.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Example**

Query:

``` sql
SELECT wordShingleMinHashArg('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Result:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgCaseInsensitive {#wordshingleminhashargcaseinsensitive}

Splits a ASCII string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [wordShingleMinHashCaseInsensitive](#wordshingleminhashcaseinsensitive) function with the same input. Is case insensitive.

**Syntax**

``` sql
wordShingleMinHashArgCaseInsensitive(string[, shinglesize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two tuples with `hashnum` word shingles each.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Example**

Query:

``` sql
SELECT wordShingleMinHashArgCaseInsensitive('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Result:

``` text
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgUTF8 {#wordshingleminhashargutf8}

Splits a UTF-8 string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [wordShingleMinHashUTF8](#wordshingleminhashutf8) function with the same input. Is case sensitive.

**Syntax**

``` sql
wordShingleMinHashArgUTF8(string[, shinglesize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two tuples with `hashnum` word shingles each.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Example**

Query:

``` sql
SELECT wordShingleMinHashArgUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Result:

``` text
┌─Tuple─────────────────────────────────────────────────────────────────┐
│ (('OLAP','database','analytical'),('online','oriented','processing')) │
└───────────────────────────────────────────────────────────────────────┘
```

## wordShingleMinHashArgCaseInsensitiveUTF8 {#wordshingleminhashargcaseinsensitiveutf8}

Splits a UTF-8 string into parts (shingles) of `shinglesize` words each and returns the shingles with minimum and maximum word hashes, calculated by the [wordShingleMinHashCaseInsensitiveUTF8](#wordshingleminhashcaseinsensitiveutf8) function with the same input. Is case insensitive.

**Syntax**

``` sql
wordShingleMinHashArgCaseInsensitiveUTF8(string[, shinglesize, hashnum])
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md).
-   `shinglesize` — The size of a word shingle. Optional. Possible values: any number from `1` to `25`. Default value: `3`. [UInt8](../../sql-reference/data-types/int-uint.md).
-   `hashnum` — The number of minimum and maximum hashes used to calculate the result. Optional. Possible values: any number from `1` to `25`. Default value: `6`. [UInt8](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   Tuple with two tuples with `hashnum` word shingles each.

Type: [Tuple](../../sql-reference/data-types/tuple.md)([Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md)), [Tuple](../../sql-reference/data-types/tuple.md)([String](../../sql-reference/data-types/string.md))).

**Example**

Query:

``` sql
SELECT wordShingleMinHashArgCaseInsensitiveUTF8('ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP).', 1, 3) AS Tuple;
```

Result:

``` text
┌─Tuple──────────────────────────────────────────────────────────────────┐
│ (('queries','database','analytical'),('oriented','processing','DBMS')) │
└────────────────────────────────────────────────────────────────────────┘
```
