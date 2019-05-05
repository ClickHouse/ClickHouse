# Hash函数

Hash函数可以用于元素的不可逆的伪随机打乱。

## halfMD5

计算字符串的MD5。然后获取结果的前8个字节并将它们作为big endian形式的UInt64返回。
此函数相当低效（500万个短字符串/秒/核心）。
如果您不需要一定使用MD5，请使用‘sipHash64’函数。

## MD5

计算字符串的MD5并将结果放入FixedString(16)中返回。
如果您只是需要一个128位的hash，同时不需要一定使用MD5，请使用‘sipHash128’函数。
如果您要获得与md5sum程序相同的输出结果，请使用lower(hex(MD5(s)))。

## sipHash64

计算字符串的SipHash。
接受String类型的参数，返回UInt64。
SipHash是一种加密哈希函数。它的处理性能至少比MD5快三倍。
有关详细信息，请参阅链接：<https://131002.net/siphash/>

## sipHash128

计算字符串的SipHash。
接受String类型的参数，返回FixedString(16)。
与sipHash64函数的不同在于它的最终计算结果为128位。

## cityHash64

Calculates CityHash64 from a string or a similar hash function for any number of any type of arguments.
For String-type arguments, CityHash is used. This is a fast non-cryptographic hash function for strings with decent quality.
For other types of arguments, a decent implementation-specific fast non-cryptographic hash function is used.
If multiple arguments are passed, the function is calculated using the same rules and chain combinations using the CityHash combinator.
For example, you can compute the checksum of an entire table with accuracy up to the row order: `SELECT sum(cityHash64(*)) FROM table`.

## intHash32

为任何类型的整数计算32位的哈希。
这是相对高效的非加密Hash函数。
This is a relatively fast non-cryptographic hash function of average quality for numbers.

## intHash64

Calculates a 64-bit hash code from any type of integer.
It works faster than intHash32. Average quality.

## SHA1

## SHA224

## SHA256

计算字符串的SHA-1，SHA-224或SHA-256，并将结果字节集返回为FixedString（20），FixedString（28）或FixedString（32）。
该函数相当低效（SHA-1大约500万个短字符串/秒/核心，而SHA-224和SHA-256大约220万个短字符串/秒/核心）。
我们建议仅在必须使用这些Hash函数且无法更改的情况下使用这些函数。
即使在这些情况下，我们建议将函数脱机应用并在将值插入表格时预先计算值，而不是在SELECTS中应用它。
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

计算字符串的FarmHash64。
接受一个String类型的参数。返回UInt64。
有关详细信息，请参阅链接：[FarmHash64](https://github.com/google/farmhash)

## javaHash {#hash_functions-javahash}

计算字符串的JavaHash。
接受一个String类型的参数。返回Int32。
有关更多信息，请参阅链接：[JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452)

## hiveHash

计算字符串的HiveHash。
接受一个String类型的参数。返回Int32。
与[JavaHash](＃hash_functions-javahash)相同，但不会返回负数。

## metroHash64

计算字符串的MetroHash。
接受一个String类型的参数。返回UInt64。
有关详细信息，请参阅链接：[MetroHash64](http://www.jandrewrogers.com/2015/05/27/metrohash/)

## jumpConsistentHash

计算UInt64的JumpConsistentHash。
接受UInt64类型的参数。返回Int32。
有关更多信息，请参见链接：[JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2_32, murmurHash2_64

计算字符串的MurmurHash2。
接受一个String类型的参数。返回UInt64或UInt32。
有关更多信息，请参阅链接：[MurmurHash2](https://github.com/aappleby/smhasher)

## murmurHash3_32, murmurHash3_64, murmurHash3_128

计算字符串的MurmurHash3。
接受一个String类型的参数。返回UInt64或UInt32或FixedString(16)。
有关更多信息，请参阅链接：[MurmurHash3](https://github.com/aappleby/smhasher)

## xxHash32, xxHash64

计算字符串的xxHash。
接受一个String类型的参数。返回UInt64或UInt32。
有关更多信息，请参见链接：[xxHash](http://cyan4973.github.io/xxHash/)

[来源文章](https://clickhouse.yandex/docs/en/query_language/functions/hash_functions/) <!--hide-->
