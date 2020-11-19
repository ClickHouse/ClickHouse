---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: Karma
---

# Karma Fonksiyonlar {#hash-functions}

Hash fonksiyonları elementlerin deterministik sözde rastgele karıştırma için kullanılabilir.

## halfMD5 {#hash-functions-halfmd5}

[Yorumluyor](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) tüm giriş parametrelerini dizeler olarak hesaplar ve [MD5](https://en.wikipedia.org/wiki/MD5) her biri için karma değeri. Sonra karmaları birleştirir, elde edilen dizenin karmasının ilk 8 baytını alır ve bunları şöyle yorumlar `UInt64` büyük endian bayt sırasına göre.

``` sql
halfMD5(par1, ...)
```

İşlev nispeten yavaştır (işlemci çekirdeği başına saniyede 5 milyon kısa dizge).
Kullanmayı düşünün [sifash64](#hash_functions-siphash64) bunun yerine işlev.

**Parametre**

Fonksiyon, değişken sayıda giriş parametresi alır. Parametreler herhangi biri olabilir [desteklenen veri türleri](../../sql-reference/data-types/index.md).

**Döndürülen Değer**

A [Uİnt64](../../sql-reference/data-types/int-uint.md) veri türü karma değeri.

**Örnek**

``` sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type
```

``` text
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD5 {#hash_functions-md5}

MD5 bir dizeden hesaplar ve elde edilen bayt kümesini FixedString(16) olarak döndürür.
Özellikle MD5'E ihtiyacınız yoksa, ancak iyi bir şifreleme 128 bit karmasına ihtiyacınız varsa, ‘sipHash128’ bunun yerine işlev.
Md5sum yardımcı programı tarafından çıktı ile aynı sonucu elde etmek istiyorsanız, lower(hex(MD5(s))) kullanın.

## sifash64 {#hash_functions-siphash64}

64-bit üretir [Sifash](https://131002.net/siphash/) karma değeri.

``` sql
sipHash64(par1,...)
```

Bu bir şifreleme karma işlevidir. En az üç kat daha hızlı çalışır [MD5](#hash_functions-md5) İşlev.

İşlev [yorumluyor](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) tüm giriş parametreleri dizeleri olarak ve bunların her biri için karma değerini hesaplar. Sonra aşağıdaki algoritma ile karmaları birleştirir:

1.  Tüm giriş parametrelerini karma yaptıktan sonra, işlev karma dizisini alır.
2.  Fonksiyon birinci ve ikinci öğeleri alır ve bunların dizisi için bir karma hesaplar.
3.  Daha sonra işlev, önceki adımda hesaplanan karma değeri ve ilk karma dizinin üçüncü öğesini alır ve bunların dizisi için bir karma hesaplar.
4.  Önceki adım, ilk karma dizinin kalan tüm öğeleri için tekrarlanır.

**Parametre**

Fonksiyon, değişken sayıda giriş parametresi alır. Parametreler herhangi biri olabilir [desteklenen veri türleri](../../sql-reference/data-types/index.md).

**Döndürülen Değer**

A [Uİnt64](../../sql-reference/data-types/int-uint.md) veri türü karma değeri.

**Örnek**

``` sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type
```

``` text
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## sifash128 {#hash_functions-siphash128}

Bir dizeden Sifash hesaplar.
Bir dize türü bağımsız değişkeni kabul eder. Fixedstring(16) Döndürür.
Sifash64'ten farklıdır, çünkü son xor katlama durumu sadece 128 bit'e kadar yapılır.

## cityHash64 {#cityhash64}

64-bit üretir [CityHash](https://github.com/google/cityhash) karma değeri.

``` sql
cityHash64(par1,...)
```

Bu hızlı olmayan şifreleme karma işlevidir. Dize parametreleri için CityHash algoritmasını ve diğer veri türleriyle parametreler için uygulamaya özgü hızlı kriptografik olmayan karma işlevini kullanır. İşlev, nihai sonuçları almak için CityHash birleştiricisini kullanır.

**Parametre**

Fonksiyon, değişken sayıda giriş parametresi alır. Parametreler herhangi biri olabilir [desteklenen veri türleri](../../sql-reference/data-types/index.md).

**Döndürülen Değer**

A [Uİnt64](../../sql-reference/data-types/int-uint.md) veri türü karma değeri.

**Örnekler**

Çağrı örneği:

``` sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type
```

``` text
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

Aşağıdaki örnek, tüm tablonun sağlama toplamının satır sırasına kadar doğrulukla nasıl hesaplanacağını gösterir:

``` sql
SELECT groupBitXor(cityHash64(*)) FROM table
```

## intHash32 {#inthash32}

Herhangi bir tamsayı türünden 32 bit karma kodu hesaplar.
Bu, sayılar için ortalama kalitenin nispeten hızlı bir kriptografik olmayan karma işlevidir.

## intHash64 {#inthash64}

Herhangi bir tamsayı türünden 64 bit karma kodu hesaplar.
Inthash32'den daha hızlı çalışır. Ortalama kalite.

## SHA1 {#sha1}

## SHA224 {#sha224}

## SHA256 {#sha256}

Bir dizeden SHA-1, SHA-224 veya SHA-256 hesaplar ve elde edilen bayt kümesini FixedString(20), FixedString(28) veya FixedString(32) olarak döndürür.
İşlev oldukça yavaş çalışır (SHA-1, işlemci çekirdeği başına saniyede yaklaşık 5 milyon kısa dizgiyi işler, SHA-224 ve SHA-256 ise yaklaşık 2.2 milyon işlem yapar).
Bu işlevi yalnızca belirli bir karma işleve ihtiyacınız olduğunda ve bunu seçemediğinizde kullanmanızı öneririz.
Bu gibi durumlarda bile, SELECTS'TE uygulamak yerine, tabloya eklerken işlev çevrimdışı ve ön hesaplama değerlerini uygulamanızı öneririz.

## URLHash(url \[, N\]) {#urlhashurl-n}

Bir tür normalleştirme kullanarak bir URL'den elde edilen bir dize için hızlı, iyi kalitede olmayan şifreleme karma işlevi.
`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/`,`?` veya `#` sonunda, varsa.
`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` veya `#` sonunda, varsa.
Düzeyleri URLHierarchy aynıdır. Bu fonksiyon (kayıt olmak için özeldir.Metrica.

## farmHash64 {#farmhash64}

64-bit üretir [FarmHash](https://github.com/google/farmhash) karma değeri.

``` sql
farmHash64(par1, ...)
```

Fonksiyonu kullanır `Hash64` tüm yöntem [mevcut yöntemler](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**Parametre**

Fonksiyon, değişken sayıda giriş parametresi alır. Parametreler herhangi biri olabilir [desteklenen veri türleri](../../sql-reference/data-types/index.md).

**Döndürülen Değer**

A [Uİnt64](../../sql-reference/data-types/int-uint.md) veri türü karma değeri.

**Örnek**

``` sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type
```

``` text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## javaHash {#hash_functions-javahash}

Hesaplıyor [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) bir ipten. Bu karma işlevi ne hızlı ne de iyi bir kaliteye sahip değildir. Bunu kullanmanın tek nedeni, bu algoritmanın zaten başka bir sistemde kullanılmasıdır ve tam olarak aynı sonucu hesaplamanız gerekir.

**Sözdizimi**

``` sql
SELECT javaHash('');
```

**Döndürülen değer**

A `Int32` veri türü karma değeri.

**Örnek**

Sorgu:

``` sql
SELECT javaHash('Hello, world!');
```

Sonuç:

``` text
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## javaHashUTF16LE {#javahashutf16le}

Hesaplıyor [JavaHash](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) bir dizeden, UTF-16LE kodlamasında bir dizeyi temsil eden bayt içerdiğini varsayarak.

**Sözdizimi**

``` sql
javaHashUTF16LE(stringUtf16le)
```

**Parametre**

-   `stringUtf16le` — a string in UTF-16LE encoding.

**Döndürülen değer**

A `Int32` veri türü karma değeri.

**Örnek**

UTF-16LE kodlanmış dize ile doğru sorgu.

Sorgu:

``` sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))
```

Sonuç:

``` text
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## hiveHash {#hash-functions-hivehash}

Hesaplıyor `HiveHash` bir ipten.

``` sql
SELECT hiveHash('');
```

Bu sadece [JavaHash](#hash_functions-javahash) sıfırlanmış işaret biti ile. Bu işlev kullanılır [Apache Kov Hanı](https://en.wikipedia.org/wiki/Apache_Hive) 3.0 öncesi sürümler için. Bu karma işlevi ne hızlı ne de iyi bir kaliteye sahip değildir. Bunu kullanmanın tek nedeni, bu algoritmanın zaten başka bir sistemde kullanılmasıdır ve tam olarak aynı sonucu hesaplamanız gerekir.

**Döndürülen değer**

A `Int32` veri türü karma değeri.

Tür: `hiveHash`.

**Örnek**

Sorgu:

``` sql
SELECT hiveHash('Hello, world!');
```

Sonuç:

``` text
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## metroHash64 {#metrohash64}

64-bit üretir [MetroHash](http://www.jandrewrogers.com/2015/05/27/metrohash/) karma değeri.

``` sql
metroHash64(par1, ...)
```

**Parametre**

Fonksiyon, değişken sayıda giriş parametresi alır. Parametreler herhangi biri olabilir [desteklenen veri türleri](../../sql-reference/data-types/index.md).

**Döndürülen Değer**

A [Uİnt64](../../sql-reference/data-types/int-uint.md) veri türü karma değeri.

**Örnek**

``` sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type
```

``` text
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## jumpConsistentHash {#jumpconsistenthash}

Bir Uint64 Formu jumpconsistenthash hesaplar.
İki bağımsız değişkeni kabul eder: bir uint64 tipi anahtar ve kova sayısı. Int32 Döndürür.
Daha fazla bilgi için bağlantıya bakın: [JumpConsistentHash](https://arxiv.org/pdf/1406.2294.pdf)

## murmurHash2\_32, murmurHash2\_64 {#murmurhash2-32-murmurhash2-64}

Üreten bir [MurmurHash2](https://github.com/aappleby/smhasher) karma değeri.

``` sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**Parametre**

Her iki işlev de değişken sayıda giriş parametresi alır. Parametreler herhangi biri olabilir [desteklenen veri türleri](../../sql-reference/data-types/index.md).

**Döndürülen Değer**

-   Bu `murmurHash2_32` fonksiyon hash değerini döndürür [Uİnt32](../../sql-reference/data-types/int-uint.md) veri türü.
-   Bu `murmurHash2_64` fonksiyon hash değerini döndürür [Uİnt64](../../sql-reference/data-types/int-uint.md) veri türü.

**Örnek**

``` sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type
```

``` text
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## gccMurmurHash {#gccmurmurhash}

64-bit hesaplar [MurmurHash2](https://github.com/aappleby/smhasher) aynı karma tohum kullanarak karma değeri [gcc](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191). CLang ve GCC yapıları arasında taşınabilir.

**Sözdizimi**

``` sql
gccMurmurHash(par1, ...);
```

**Parametre**

-   `par1, ...` — A variable number of parameters that can be any of the [desteklenen veri türleri](../../sql-reference/data-types/index.md#data_types).

**Döndürülen değer**

-   Hesaplanan karma değeri.

Tür: [Uİnt64](../../sql-reference/data-types/int-uint.md).

**Örnek**

Sorgu:

``` sql
SELECT
    gccMurmurHash(1, 2, 3) AS res1,
    gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2
```

Sonuç:

``` text
┌─────────────────res1─┬────────────────res2─┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘
```

## murmurHash3\_32, murmurHash3\_64 {#murmurhash3-32-murmurhash3-64}

Üreten bir [MurmurHash3](https://github.com/aappleby/smhasher) karma değeri.

``` sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**Parametre**

Her iki işlev de değişken sayıda giriş parametresi alır. Parametreler herhangi biri olabilir [desteklenen veri türleri](../../sql-reference/data-types/index.md).

**Döndürülen Değer**

-   Bu `murmurHash3_32` fonksiyon bir [Uİnt32](../../sql-reference/data-types/int-uint.md) veri türü karma değeri.
-   Bu `murmurHash3_64` fonksiyon bir [Uİnt64](../../sql-reference/data-types/int-uint.md) veri türü karma değeri.

**Örnek**

``` sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## murmurHash3\_128 {#murmurhash3-128}

128-bit üretir [MurmurHash3](https://github.com/aappleby/smhasher) karma değeri.

``` sql
murmurHash3_128( expr )
```

**Parametre**

-   `expr` — [İfadeler](../syntax.md#syntax-expressions) dönen bir [Dize](../../sql-reference/data-types/string.md)- tip değeri.

**Döndürülen Değer**

A [FixedString (16)](../../sql-reference/data-types/fixedstring.md) veri türü karma değeri.

**Örnek**

``` sql
SELECT murmurHash3_128('example_string') AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3──────┬─type────────────┐
│ 6�1�4"S5KT�~~q │ FixedString(16) │
└──────────────────┴─────────────────┘
```

## xxHash32, xxHash64 {#hash-functions-xxhash32}

Hesaplıyor `xxHash` bir ipten. İki tat, 32 ve 64 bit olarak önerilmiştir.

``` sql
SELECT xxHash32('');

OR

SELECT xxHash64('');
```

**Döndürülen değer**

A `Uint32` veya `Uint64` veri türü karma değeri.

Tür: `xxHash`.

**Örnek**

Sorgu:

``` sql
SELECT xxHash32('Hello, world!');
```

Sonuç:

``` text
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**Ayrıca Bakınız**

-   [xxHash](http://cyan4973.github.io/xxHash/).

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/hash_functions/) <!--hide-->
