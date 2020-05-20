---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "Dizeleri ile \xE7al\u0131\u015Fma"
---

# Dizelerle çalışmak için işlevler {#functions-for-working-with-strings}

## boş {#empty}

Boş bir dize için 1 veya boş olmayan bir dize için 0 döndürür.
Sonuç türü Uint8'dir.
Bir boşluk veya boş bayt olsa bile, en az bir bayt içeriyorsa, bir dize boş olarak kabul edilir.
İşlev ayrıca diziler için de çalışır.

## notEmpty {#notempty}

Boş bir dize için 0 veya boş olmayan bir dize için 1 döndürür.
Sonuç türü Uint8'dir.
İşlev ayrıca diziler için de çalışır.

## uzunluk {#length}

Bir dizenin uzunluğunu bayt cinsinden döndürür (karakterlerde değil, kod noktalarında değil).
Sonuç türü Uint64'tür.
İşlev ayrıca diziler için de çalışır.

## lengthUTF8 {#lengthutf8}

Dizenin UTF-8 kodlanmış metni oluşturan bir bayt kümesi içerdiğini varsayarak, Unicode kod noktalarında (karakterlerde değil) bir dizenin uzunluğunu döndürür. Bu varsayım karşılanmazsa, bir sonuç döndürür (bir istisna atmaz).
Sonuç türü Uint64'tür.

## char\_length, CHAR\_LENGTH {#char-length}

Dizenin UTF-8 kodlanmış metni oluşturan bir bayt kümesi içerdiğini varsayarak, Unicode kod noktalarında (karakterlerde değil) bir dizenin uzunluğunu döndürür. Bu varsayım karşılanmazsa, bir sonuç döndürür (bir istisna atmaz).
Sonuç türü Uint64'tür.

## character\_length, CHARACTER\_LENGTH {#character-length}

Dizenin UTF-8 kodlanmış metni oluşturan bir bayt kümesi içerdiğini varsayarak, Unicode kod noktalarında (karakterlerde değil) bir dizenin uzunluğunu döndürür. Bu varsayım karşılanmazsa, bir sonuç döndürür (bir istisna atmaz).
Sonuç türü Uint64'tür.

## alt, lcase {#lower}

Bir dizedeki ASCII Latin sembollerini küçük harfe dönüştürür.

## üst, ucase {#upper}

Bir dizedeki ASCII Latin sembollerini büyük harfe dönüştürür.

## lowerUTF8 {#lowerutf8}

Dizenin UTF-8 kodlu bir metni oluşturan bir bayt kümesi içerdiğini varsayarak bir dizeyi küçük harfe dönüştürür.
Dili algılamaz. Yani Türkçe için sonuç tam olarak doğru olmayabilir.
UTF-8 bayt dizisinin uzunluğu bir kod noktasının büyük ve küçük harf için farklıysa, sonuç bu kod noktası için yanlış olabilir.
Dize, UTF-8 olmayan bir bayt kümesi içeriyorsa, davranış tanımsızdır.

## upperUTF8 {#upperutf8}

Dize, UTF-8 kodlanmış bir metni oluşturan bir bayt kümesi içerdiğini varsayarak bir dizeyi büyük harfe dönüştürür.
Dili algılamaz. Yani Türkçe için sonuç tam olarak doğru olmayabilir.
UTF-8 bayt dizisinin uzunluğu bir kod noktasının büyük ve küçük harf için farklıysa, sonuç bu kod noktası için yanlış olabilir.
Dize, UTF-8 olmayan bir bayt kümesi içeriyorsa, davranış tanımsızdır.

## ısvalidutf8 {#isvalidutf8}

Bayt kümesi geçerli UTF-8 kodlanmış, aksi takdirde 0 ise, 1 döndürür.

## toValidUTF8 {#tovalidutf8}

Geçersiz UTF-8 karakterlerini değiştirir `�` (U+FFFD) karakteri. Bir satırda çalışan tüm geçersiz karakterler bir yedek karaktere daraltılır.

``` sql
toValidUTF8( input_string )
```

Parametre:

-   input\_string — Any set of bytes represented as the [Dize](../../sql-reference/data-types/string.md) veri türü nesnesi.

Döndürülen değer: geçerli UTF-8 dizesi.

**Örnek**

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b')
```

``` text
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## tekrarlama {#repeat}

Bir dizeyi belirtilen kadar çok tekrarlar ve çoğaltılmış değerleri tek bir dize olarak birleştirir.

**Sözdizimi**

``` sql
repeat(s, n)
```

**Parametre**

-   `s` — The string to repeat. [Dize](../../sql-reference/data-types/string.md).
-   `n` — The number of times to repeat the string. [Uİnt](../../sql-reference/data-types/int-uint.md).

**Döndürülen değer**

Dize içeren tek dize `s` tekrarlanan `n` zamanlar. Eğer `n` \< 1, işlev boş dize döndürür.

Tür: `String`.

**Örnek**

Sorgu:

``` sql
SELECT repeat('abc', 10)
```

Sonuç:

``` text
┌─repeat('abc', 10)──────────────┐
│ abcabcabcabcabcabcabcabcabcabc │
└────────────────────────────────┘
```

## tersi {#reverse}

Dizeyi tersine çevirir (bayt dizisi olarak).

## reverseUTF8 {#reverseutf8}

Dizenin UTF-8 metnini temsil eden bir bayt kümesi içerdiğini varsayarak bir Unicode kod noktası dizisini tersine çevirir. Aksi takdirde, başka bir şey yapar(bir istisna atmaz).

## format(pattern, s0, s1, …) {#format}

Bağımsız değişkenlerde listelenen dize ile sabit desen biçimlendirme. `pattern` basitleştirilmiş bir Python biçimi desenidir. Biçim dizesi içerir “replacement fields” kıvırcık parantez ile çevrili `{}`. Parantez içinde bulunmayan herhangi bir şey, çıktıya değişmeden kopyalanan hazır metin olarak kabul edilir. Literal metne bir ayraç karakteri eklemeniz gerekiyorsa, iki katına çıkararak kaçabilir: `{{ '{{' }}` ve `{{ '}}' }}`. Alan adları sayılar (sıfırdan başlayarak) veya boş olabilir (daha sonra sonuç numaraları olarak kabul edilir).

``` sql
SELECT format('{1} {0} {1}', 'World', 'Hello')
```

``` text
┌─format('{1} {0} {1}', 'World', 'Hello')─┐
│ Hello World Hello                       │
└─────────────────────────────────────────┘
```

``` sql
SELECT format('{} {}', 'Hello', 'World')
```

``` text
┌─format('{} {}', 'Hello', 'World')─┐
│ Hello World                       │
└───────────────────────────────────┘
```

## concat {#concat}

Bağımsız değişkenlerde listelenen dizeleri ayırıcı olmadan birleştirir.

**Sözdizimi**

``` sql
concat(s1, s2, ...)
```

**Parametre**

String veya FixedString türünün değerleri.

**Döndürülen değerler**

Bağımsız değişkenlerin birleştirilmesinden kaynaklanan dizeyi döndürür.

Argüman değerlerinden herhangi biri ise `NULL`, `concat` dönüşler `NULL`.

**Örnek**

Sorgu:

``` sql
SELECT concat('Hello, ', 'World!')
```

Sonuç:

``` text
┌─concat('Hello, ', 'World!')─┐
│ Hello, World!               │
└─────────────────────────────┘
```

## concatassumeınjective {#concatassumeinjective}

Aynı olarak [concat](#concat) emin olun bu ihtiyaç fark var `concat(s1, s2, ...) → sn` enjekte edilir, grup tarafından optimizasyonu için kullanılacaktır.

İşlev adlı “injective” bağımsız değişkenlerin farklı değerleri için her zaman farklı sonuç döndürürse. Başka bir deyişle: farklı argümanlar asla aynı sonucu vermez.

**Sözdizimi**

``` sql
concatAssumeInjective(s1, s2, ...)
```

**Parametre**

String veya FixedString türünün değerleri.

**Döndürülen değerler**

Bağımsız değişkenlerin birleştirilmesinden kaynaklanan dizeyi döndürür.

Argüman değerlerinden herhangi biri ise `NULL`, `concatAssumeInjective` dönüşler `NULL`.

**Örnek**

Giriş tablosu:

``` sql
CREATE TABLE key_val(`key1` String, `key2` String, `value` UInt32) ENGINE = TinyLog;
INSERT INTO key_val VALUES ('Hello, ','World',1), ('Hello, ','World',2), ('Hello, ','World!',3), ('Hello',', World!',2);
SELECT * from key_val;
```

``` text
┌─key1────┬─key2─────┬─value─┐
│ Hello,  │ World    │     1 │
│ Hello,  │ World    │     2 │
│ Hello,  │ World!   │     3 │
│ Hello   │ , World! │     2 │
└─────────┴──────────┴───────┘
```

Sorgu:

``` sql
SELECT concat(key1, key2), sum(value) FROM key_val GROUP BY concatAssumeInjective(key1, key2)
```

Sonuç:

``` text
┌─concat(key1, key2)─┬─sum(value)─┐
│ Hello, World!      │          3 │
│ Hello, World!      │          2 │
│ Hello, World       │          3 │
└────────────────────┴────────────┘
```

## alt dize (s, ofset, uzunluk), orta (s, ofset, uzunluk), substr (s, ofset, uzunluk) {#substring}

Bayttan başlayarak bir alt dize döndürür ‘offset’ ind thatex yani ‘length’ uzun bayt. Karakter indeksleme birinden başlar (standart SQL'DE olduğu gibi). Bu ‘offset’ ve ‘length’ bağımsız değişkenler sabit olmalıdır.

## substringUTF8(s, ofset, uzunluk) {#substringutf8}

Olarak aynı ‘substring’, ancak Unicode kod noktaları için. Dizenin UTF-8 kodlanmış bir metni temsil eden bir bayt kümesi içerdiği varsayımı altında çalışır. Bu varsayım karşılanmazsa, bir sonuç döndürür (bir istisna atmaz).

## appendTrailingCharİfAbsent (s, c) {#appendtrailingcharifabsent}

Eğer... ‘s’ dize boş değildir ve ‘c’ sonunda karakter, ekler ‘c’ sonuna kadar karakter.

## convertCharset (s, from, to) {#convertcharset}

Dize döndürür ‘s’ bu kodlamadan dönüştürüldü ‘from’ kod encodinglamaya ‘to’.

## base64Encode (s) {#base64encode}

Kodluyor ‘s’ Base64 içine dize

## base64Decode (s) {#base64decode}

Base64 kodlu dizeyi çözme ‘s’ orijinal dizeye. Başarısızlık durumunda bir istisna yükseltir.

## tryBase64Decode (s) {#trybase64decode}

Base64decode'a benzer, ancak hata durumunda boş bir dize döndürülür.

## endsWith (s, sonek) {#endswith}

Belirtilen sonek ile bitip bitmeyeceğini döndürür. Dize belirtilen sonek ile biterse 1 değerini döndürür, aksi takdirde 0 değerini döndürür.

## startsWith (str, önek) {#startswith}

Dize belirtilen önek ile başlayıp başlamadığını 1 döndürür, aksi halde 0 döndürür.

``` sql
SELECT startsWith('Spider-Man', 'Spi');
```

**Döndürülen değerler**

-   1, dize belirtilen önek ile başlarsa.
-   0, dize belirtilen önek ile başlamazsa.

**Örnek**

Sorgu:

``` sql
SELECT startsWith('Hello, world!', 'He');
```

Sonuç:

``` text
┌─startsWith('Hello, world!', 'He')─┐
│                                 1 │
└───────────────────────────────────┘
```

## kırpmak {#trim}

Belirtilen tüm karakterleri bir dizenin başlangıcından veya sonundan kaldırır.
Varsayılan olarak, bir dizenin her iki ucundan ortak boşlukların (ASCII karakteri 32) tüm ardışık tekrarlarını kaldırır.

**Sözdizimi**

``` sql
trim([[LEADING|TRAILING|BOTH] trim_character FROM] input_string)
```

**Parametre**

-   `trim_character` — specified characters for trim. [Dize](../../sql-reference/data-types/string.md).
-   `input_string` — string for trim. [Dize](../../sql-reference/data-types/string.md).

**Döndürülen değer**

Önde gelen ve (veya) belirtilen karakterleri izleyen bir dize.

Tür: `String`.

**Örnek**

Sorgu:

``` sql
SELECT trim(BOTH ' ()' FROM '(   Hello, world!   )')
```

Sonuç:

``` text
┌─trim(BOTH ' ()' FROM '(   Hello, world!   )')─┐
│ Hello, world!                                 │
└───────────────────────────────────────────────┘
```

## trimLeft {#trimleft}

Bir dizenin başlangıcından ortak boşluk (ASCII karakteri 32) tüm ardışık tekrarlarını kaldırır. Diğer boşluk karakterlerini (sekme, boşluksuz boşluk, vb.) kaldırmaz.).

**Sözdizimi**

``` sql
trimLeft(input_string)
```

Takma ad: `ltrim(input_string)`.

**Parametre**

-   `input_string` — string to trim. [Dize](../../sql-reference/data-types/string.md).

**Döndürülen değer**

Bir dize olmadan lider ortak whitespaces.

Tür: `String`.

**Örnek**

Sorgu:

``` sql
SELECT trimLeft('     Hello, world!     ')
```

Sonuç:

``` text
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## trimRight {#trimright}

Bir dizenin sonundan ortak boşluk (ASCII karakteri 32) tüm ardışık tekrarlarını kaldırır. Diğer boşluk karakterlerini (sekme, boşluksuz boşluk, vb.) kaldırmaz.).

**Sözdizimi**

``` sql
trimRight(input_string)
```

Takma ad: `rtrim(input_string)`.

**Parametre**

-   `input_string` — string to trim. [Dize](../../sql-reference/data-types/string.md).

**Döndürülen değer**

Ortak whitespaces firar olmadan bir dize.

Tür: `String`.

**Örnek**

Sorgu:

``` sql
SELECT trimRight('     Hello, world!     ')
```

Sonuç:

``` text
┌─trimRight('     Hello, world!     ')─┐
│      Hello, world!                   │
└──────────────────────────────────────┘
```

## trimBoth {#trimboth}

Bir dizenin her iki ucundan ortak boşluk (ASCII karakteri 32) tüm ardışık tekrarlarını kaldırır. Diğer boşluk karakterlerini (sekme, boşluksuz boşluk, vb.) kaldırmaz.).

**Sözdizimi**

``` sql
trimBoth(input_string)
```

Takma ad: `trim(input_string)`.

**Parametre**

-   `input_string` — string to trim. [Dize](../../sql-reference/data-types/string.md).

**Döndürülen değer**

Bir dize olmadan lider ve sondaki ortak whitespaces.

Tür: `String`.

**Örnek**

Sorgu:

``` sql
SELECT trimBoth('     Hello, world!     ')
```

Sonuç:

``` text
┌─trimBoth('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## CRC32 (s) {#crc32}

CRC-32-IEEE 802.3 polinom ve başlangıç değerini kullanarak bir dizenin CRC32 sağlama toplamını döndürür `0xffffffff` (zlib uygulaması).

Sonuç türü Uint32'dir.

## Crc32ieee (s) {#crc32ieee}

CRC-32-IEEE 802.3 polinomunu kullanarak bir dizenin CRC32 sağlama toplamını döndürür.

Sonuç türü Uint32'dir.

## CRC64 (s) {#crc64}

CRC-64-ECMA polinomunu kullanarak bir dizenin CRC64 sağlama toplamını döndürür.

Sonuç türü Uint64'tür.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/string_functions/) <!--hide-->
