# Hash函数 {#hashhan-shu}

Hash函数可以用于将元素不可逆的伪随机打乱。

## halfMD5 {#halfmd5}

计算字符串的MD5。然后获取结果的前8个字节并将它们作为UInt64（大端）返回。
此函数相当低效（500万个短字符串/秒/核心）。
如果您不需要一定使用MD5，请使用’sipHash64’函数。

## MD5 {#md5}

计算字符串的MD5并将结果放入FixedString(16)中返回。
如果您只是需要一个128位的hash，同时不需要一定使用MD5，请使用’sipHash128’函数。
如果您要获得与md5sum程序相同的输出结果，请使用lower(hex(MD5(s)))。

## sipHash64 {#siphash64}

计算字符串的SipHash。
接受String类型的参数，返回UInt64。
SipHash是一种加密哈希函数。它的处理性能至少比MD5快三倍。
有关详细信息，请参阅链接：https://131002.net/siphash/

## sipHash128 {#hash_functions-siphash128}

计算字符串的SipHash。
接受String类型的参数，返回FixedString(16)。
与sipHash64函数的不同在于它的最终计算结果为128位。

## cityHash64 {#cityhash64}

计算任意数量字符串的CityHash64或使用特定实现的Hash函数计算任意数量其他类型的Hash。
对于字符串，使用CityHash算法。 这是一个快速的非加密哈希函数，用于字符串。
对于其他类型的参数，使用特定实现的Hash函数，这是一种快速的非加密的散列函数。
如果传递了多个参数，则使用CityHash组合这些参数的Hash结果。
例如，您可以计算整个表的checksum，其结果取决于行的顺序：`SELECT sum(cityHash64(*)) FROM table`。

## intHash32 {#inthash32}

为任何类型的整数计算32位的哈希。
这是相对高效的非加密Hash函数。

## intHash64 {#inthash64}

从任何类型的整数计算64位哈希码。
它的工作速度比intHash32函数快。

## SHA1 {#sha1}

## SHA224 {#sha224}

## SHA256 {#sha256}

计算字符串的SHA-1，SHA-224或SHA-256，并将结果字节集返回为FixedString(20)，FixedString(28)或FixedString(32)。
该函数相当低效（SHA-1大约500万个短字符串/秒/核心，而SHA-224和SHA-256大约220万个短字符串/秒/核心）。
我们建议仅在必须使用这些Hash函数且无法更改的情况下使用这些函数。
即使在这些情况下，我们仍建议将函数采用在写入数据时使用预计算的方式将其计算完毕。而不是在SELECT中计算它们。

## URLHash(url\[,N\]) {#urlhashurl-n}

一种快速的非加密哈希函数，用于规范化的从URL获得的字符串。
`URLHash(s)` - 从一个字符串计算一个哈希，如果结尾存在尾随符号`/`，`？`或`#`则忽略。
`URLHash（s，N）` - 计算URL层次结构中字符串到N级别的哈希值，如果末尾存在尾随符号`/`，`？`或`#`则忽略。
URL的层级与URLHierarchy中的层级相同。 此函数被用于Yandex.Metrica。

## farmHash64 {#farmhash64}

计算字符串的FarmHash64。
接受一个String类型的参数。返回UInt64。
有关详细信息，请参阅链接：[FarmHash64](https://github.com/google/farmhash)

## javaHash {#hash_functions-javahash}

计算字符串的JavaHash。
接受一个String类型的参数。返回Int32。
有关更多信息，请参阅链接：[JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452)

## hiveHash {#hivehash}

计算字符串的HiveHash。
接受一个String类型的参数。返回Int32。
与[JavaHash](#hash_functions-javahash)相同，但不会返回负数。

## metroHash64 {#metrohash64}

计算字符串的MetroHash。
接受一个String类型的参数。返回UInt64。
有关详细信息，请参阅链接：[MetroHash64](http://www.jandrewrogers.com/2015/05/27/metrohash/)

## jumpConsistentHash {#jumpconsistenthash}

计算UInt64的JumpConsistentHash。
接受UInt64类型的参数。返回Int32。
有关更多信息，请参见链接：[JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2_32,murmurHash2_64 {#murmurhash2-32-murmurhash2-64}

计算字符串的MurmurHash2。
接受一个String类型的参数。返回UInt64或UInt32。
有关更多信息，请参阅链接：[MurmurHash2](https://github.com/aappleby/smhasher)

## murmurHash3_32,murmurHash3_64,murmurHash3_128 {#murmurhash3-32-murmurhash3-64-murmurhash3-128}

计算字符串的MurmurHash3。
接受一个String类型的参数。返回UInt64或UInt32或FixedString(16)。
有关更多信息，请参阅链接：[MurmurHash3](https://github.com/aappleby/smhasher)

## xxHash32,xxHash64 {#xxhash32-xxhash64}

计算字符串的xxHash。
接受一个String类型的参数。返回UInt64或UInt32。
有关更多信息，请参见链接：[xxHash](http://cyan4973.github.io/xxHash/)

[来源文章](https://clickhouse.com/docs/en/query_language/functions/hash_functions/) <!--hide-->
