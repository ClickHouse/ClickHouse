---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: Bitlik
---

# Bit Fonksiyonları {#bit-functions}

Bit işlevleri, uint8, Uİnt16, Uİnt32, Uint64, Int8, Int16, Int32, Int64, Float32 veya Float64 türlerinden herhangi bir çift için çalışır.

Sonuç türü, bağımsız değişkenlerinin maksimum bitlerine eşit bit içeren bir tamsayıdır. Bağımsız değişkenlerden en az biri imzalanırsa, sonuç imzalı bir sayıdır. Bir bağımsız değişken bir kayan noktalı sayı ise, Int64 için cast.

## bıtor(a, b) {#bitanda-b}

## bitOr(a, b) {#bitora-b}

## bitXor(a, b) {#bitxora-b}

## bitNot (a) {#bitnota}

## bitShiftLeft(a, b) {#bitshiftlefta-b}

## bitShiftRight(a, b) {#bitshiftrighta-b}

## bitRotateLeft(a, b) {#bitrotatelefta-b}

## bitRotateRight(a, b) {#bitrotaterighta-b}

## bitTest {#bittest}

Herhangi bir tamsayı alır ve dönüştürür [ikili form](https://en.wikipedia.org/wiki/Binary_number), belirtilen konumda bir bit değerini döndürür. Geri sayım sağdan sola 0 başlar.

**Sözdizimi**

``` sql
SELECT bitTest(number, index)
```

**Parametre**

-   `number` – integer number.
-   `index` – position of bit.

**Döndürülen değerler**

Belirtilen konumda bit değeri döndürür.

Tür: `UInt8`.

**Örnek**

Örneğin, taban-2 (ikili) sayı sistemindeki 43 sayısı 101011'dir.

Sorgu:

``` sql
SELECT bitTest(43, 1)
```

Sonuç:

``` text
┌─bitTest(43, 1)─┐
│              1 │
└────────────────┘
```

Başka bir örnek:

Sorgu:

``` sql
SELECT bitTest(43, 2)
```

Sonuç:

``` text
┌─bitTest(43, 2)─┐
│              0 │
└────────────────┘
```

## bitTestAll {#bittestall}

Sonucu döndürür [mantıksal conjuction](https://en.wikipedia.org/wiki/Logical_conjunction) Verilen pozisyonlarda tüm bitlerin (ve operatörü). Geri sayım sağdan sola 0 başlar.

Bitsel işlemler için conjuction:

0 AND 0 = 0

0 AND 1 = 0

1 AND 0 = 0

1 AND 1 = 1

**Sözdizimi**

``` sql
SELECT bitTestAll(number, index1, index2, index3, index4, ...)
```

**Parametre**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit. For example, for set of positions (`index1`, `index2`, `index3`, `index4`) doğru ise ve sadece tüm pozisyon trueları doğru ise (`index1` ⋀ `index2`, ⋀ `index3` ⋀ `index4`).

**Döndürülen değerler**

Mantıksal conjuction sonucunu döndürür.

Tür: `UInt8`.

**Örnek**

Örneğin, taban-2 (ikili) sayı sistemindeki 43 sayısı 101011'dir.

Sorgu:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5)
```

Sonuç:

``` text
┌─bitTestAll(43, 0, 1, 3, 5)─┐
│                          1 │
└────────────────────────────┘
```

Başka bir örnek:

Sorgu:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5, 2)
```

Sonuç:

``` text
┌─bitTestAll(43, 0, 1, 3, 5, 2)─┐
│                             0 │
└───────────────────────────────┘
```

## bitTestAny {#bittestany}

Sonucu döndürür [mantıksal ayrılma](https://en.wikipedia.org/wiki/Logical_disjunction) Verilen konumlardaki tüm bitlerin (veya operatör). Geri sayım sağdan sola 0 başlar.

Bitsel işlemler için ayrılma:

0 OR 0 = 0

0 OR 1 = 1

1 OR 0 = 1

1 OR 1 = 1

**Sözdizimi**

``` sql
SELECT bitTestAny(number, index1, index2, index3, index4, ...)
```

**Parametre**

-   `number` – integer number.
-   `index1`, `index2`, `index3`, `index4` – positions of bit.

**Döndürülen değerler**

Mantıksal disjuction sonucunu döndürür.

Tür: `UInt8`.

**Örnek**

Örneğin, taban-2 (ikili) sayı sistemindeki 43 sayısı 101011'dir.

Sorgu:

``` sql
SELECT bitTestAny(43, 0, 2)
```

Sonuç:

``` text
┌─bitTestAny(43, 0, 2)─┐
│                    1 │
└──────────────────────┘
```

Başka bir örnek:

Sorgu:

``` sql
SELECT bitTestAny(43, 4, 2)
```

Sonuç:

``` text
┌─bitTestAny(43, 4, 2)─┐
│                    0 │
└──────────────────────┘
```

## bitCount {#bitcount}

Bir sayının ikili gösteriminde birine ayarlanmış bit sayısını hesaplar.

**Sözdizimi**

``` sql
bitCount(x)
```

**Parametre**

-   `x` — [Tamsayı](../../sql-reference/data-types/int-uint.md) veya [kayan nokta](../../sql-reference/data-types/float.md) numara. İşlev, bellekteki değer gösterimini kullanır. Kayan noktalı sayıları desteklemeye izin verir.

**Döndürülen değer**

-   Giriş numarasında birine ayarlanmış bit sayısı.

İşlev, giriş değerini daha büyük bir türe dönüştürmez ([işaret uzantısı](https://en.wikipedia.org/wiki/Sign_extension)). Bu yüzden, örneğin , `bitCount(toUInt8(-1)) = 8`.

Tür: `UInt8`.

**Örnek**

Örneğin 333 sayısını alın. İkili gösterimi: 0000000101001101.

Sorgu:

``` sql
SELECT bitCount(333)
```

Sonuç:

``` text
┌─bitCount(333)─┐
│             5 │
└───────────────┘
```

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/bit_functions/) <!--hide-->
