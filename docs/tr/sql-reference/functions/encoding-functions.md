---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 52
toc_title: "Kodlam\u0131\u015F"
---

# Kodlama Fonksiyonları {#encoding-functions}

## kömürleşmek {#char}

Geçirilen bağımsız değişkenlerin sayısı olarak uzunluğu olan dizeyi döndürür ve her bayt karşılık gelen bağımsız değişken değerine sahiptir. Sayısal türlerin birden çok bağımsız değişkeni kabul eder. Bağımsız değişken değeri uint8 veri türü aralığının dışındaysa, Olası yuvarlama ve taşma ile Uint8'e dönüştürülür.

**Sözdizimi**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**Parametre**

-   `number_1, number_2, ..., number_n` — Numerical arguments interpreted as integers. Types: [Tamsayı](../../sql-reference/data-types/int-uint.md), [Yüzdürmek](../../sql-reference/data-types/float.md).

**Döndürülen değer**

-   verilen bayt dizisi.

Tür: `String`.

**Örnek**

Sorgu:

``` sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello
```

Sonuç:

``` text
┌─hello─┐
│ hello │
└───────┘
```

Karşılık gelen baytları geçirerek bir rasgele kodlama dizesi oluşturabilirsiniz. İşte UTF-8 için örnek:

Sorgu:

``` sql
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
```

Sonuç:

``` text
┌─hello──┐
│ привет │
└────────┘
```

Sorgu:

``` sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

Sonuç:

``` text
┌─hello─┐
│ 你好  │
└───────┘
```

## büyü {#hex}

Bağımsız değişkenin onaltılık gösterimini içeren bir dize döndürür.

**Sözdizimi**

``` sql
hex(arg)
```

İşlev büyük harfler kullanıyor `A-F` ve herhangi bir önek kullanmamak (gibi `0x`) veya sonekler (gibi `h`).

Tamsayı argümanları için, onaltılık basamak yazdırır (“nibbles”) en önemliden en önemlisine (big endian veya “human readable” sipariş). En önemli sıfır olmayan baytla başlar (önde gelen sıfır bayt atlanır), ancak önde gelen basamak sıfır olsa bile her baytın her iki basamağını da yazdırır.

Örnek:

**Örnek**

Sorgu:

``` sql
SELECT hex(1);
```

Sonuç:

``` text
01
```

Tip değerleri `Date` ve `DateTime` karşılık gelen tamsayılar olarak biçimlendirilir (tarih için çağdan bu yana geçen gün sayısı ve datetime için Unix zaman damgasının değeri).

İçin `String` ve `FixedString`, tüm bayt sadece iki onaltılık sayı olarak kodlanır. Sıfır bayt ihmal edilmez.

Kayan nokta ve ondalık türlerinin değerleri, bellekteki gösterimi olarak kodlanır. Küçük endian mimarisini desteklediğimiz için, bunlar küçük endian'da kodlanmıştır. Sıfır önde gelen / sondaki bayt ihmal edilmez.

**Parametre**

-   `arg` — A value to convert to hexadecimal. Types: [Dize](../../sql-reference/data-types/string.md), [Uİnt](../../sql-reference/data-types/int-uint.md), [Yüzdürmek](../../sql-reference/data-types/float.md), [Ondalık](../../sql-reference/data-types/decimal.md), [Tarihli](../../sql-reference/data-types/date.md) veya [DateTime](../../sql-reference/data-types/datetime.md).

**Döndürülen değer**

-   Bağımsız değişken onaltılık gösterimi ile bir dize.

Tür: `String`.

**Örnek**

Sorgu:

``` sql
SELECT hex(toFloat32(number)) as hex_presentation FROM numbers(15, 2);
```

Sonuç:

``` text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

Sorgu:

``` sql
SELECT hex(toFloat64(number)) as hex_presentation FROM numbers(15, 2);
```

Sonuç:

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

## unhex (str) {#unhexstr}

Onaltılık basamak herhangi bir sayıda içeren bir dize kabul eder ve karşılık gelen bayt içeren bir dize döndürür. Hem büyük hem de küçük harfleri destekler a-F. onaltılık basamak sayısı bile olmak zorunda değildir. Tek ise, son rakam 00-0F baytın en az önemli yarısı olarak yorumlanır. Bağımsız değişken dizesi onaltılık basamaklardan başka bir şey içeriyorsa, uygulama tanımlı bazı sonuçlar döndürülür (bir özel durum atılmaz).
Sonucu bir sayıya dönüştürmek istiyorsanız, ‘reverse’ ve ‘reinterpretAsType’ işlevler.

## UUİDStringToNum (str) {#uuidstringtonumstr}

Biçiminde 36 karakter içeren bir dize kabul eder `123e4567-e89b-12d3-a456-426655440000` ve bir fixedstring(16) bayt kümesi olarak döndürür.

## UUİDNumToString (str) {#uuidnumtostringstr}

FixedString(16) değerini kabul eder. Metin biçiminde 36 karakter içeren bir dize döndürür.

## bitmaskToList (num) {#bitmasktolistnum}

Bir tamsayı kabul eder. Özetlendiğinde kaynak sayısını toplayan iki güç listesini içeren bir dize döndürür. Artan düzende metin biçiminde boşluk bırakmadan virgülle ayrılırlar.

## bitmaskToArray (num) {#bitmasktoarraynum}

Bir tamsayı kabul eder. Özetlendiğinde kaynak sayısını toplayan iki güç listesini içeren bir uint64 sayı dizisi döndürür. Dizideki sayılar artan düzendedir.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/encoding_functions/) <!--hide-->
