---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 42
toc_title: "Ondal\u0131k"
---

# Ondalık(P, S), Decimal32 (S), Decimal64( S), Decimal128 (S) {#decimalp-s-decimal32s-decimal64s-decimal128s}

Ekleme, çıkarma ve çarpma işlemleri sırasında hassasiyeti koruyan imzalı sabit noktalı sayılar. Bölünme için en az önemli basamak atılır (yuvarlatılmamış).

## Parametre {#parameters}

-   P-hassas. Geçerli Aralık: \[1: 38 \]. Kaç ondalık basamak sayısı (kesir dahil) olabilir belirler.
-   S-scale. Geçerli Aralık: \[0: P\]. Kaç ondalık basamak kesir olabilir belirler.

P parametre değerine bağlı olarak ondalık (P, S) bir eşanlamlıdır:
- P \[ 1 : 9\] - Decimal32(S) için)
- P \[ 10 : 18\] - Decimal64(ler) için)
- P \[ 19 : 38\] - Decimal128(ler) için)

## Ondalık Değer Aralıkları {#decimal-value-ranges}

-   Decimal32(S) - ( -1 \* 10^(9 - S), 1 \* 10^(9-S) )
-   Decimal64(S) - ( -1 \* 10^(18 - S), 1 \* 10^(18-S) )
-   Decimal128(S) - ( -1 \* 10^(38 - S), 1 \* 10^(38-S) )

Örneğin, Decimal32 (4) -99999.9999 99999.9999 0.0001 adım ile sayılar içerebilir.

## İç Temsil {#internal-representation}

Dahili veri, ilgili bit genişliğine sahip normal imzalı tamsayılar olarak temsil edilir. Bellekte saklanabilen gerçek değer aralıkları, yukarıda belirtilenden biraz daha büyüktür ve yalnızca bir dizeden dönüştürmede kontrol edilir.

Modern CPU 128-bit tamsayıları doğal olarak desteklemediğinden, Decimal128 üzerindeki işlemler öykünülür. Bu Decimal128 nedeniyle Decimal32/Decimal64'ten önemli ölçüde daha yavaş çalışır.

## İşlemler ve sonuç türü {#operations-and-result-type}

Ondalık sonuçtaki ikili işlemler daha geniş sonuç türünde (herhangi bir bağımsız değişken sırası ile) sonuçlanır.

-   `Decimal64(S1) <op> Decimal32(S2) -> Decimal64(S)`
-   `Decimal128(S1) <op> Decimal32(S2) -> Decimal128(S)`
-   `Decimal128(S1) <op> Decimal64(S2) -> Decimal128(S)`

Ölçek kuralları:

-   ekleme, çıkarma: s = max (S1, S2).
-   multuply: S = S1 + S2.
-   böl: S = S1.

Ondalık ve tamsayılar arasındaki benzer işlemler için sonuç, bir bağımsız değişkenle aynı boyutta ondalık olur.

Ondalık ve Float32 / Float64 arasındaki işlemler tanımlanmamıştır. Bunlara ihtiyacınız varsa, todecimal32, toDecimal64, toDecimal128 veya toFloat32, toFloat64 builtins kullanarak bağımsız değişkenlerden birini açıkça yayınlayabilirsiniz. Sonucun hassasiyeti kaybedeceğini ve tür dönüşümünün hesaplamalı olarak pahalı bir işlem olduğunu unutmayın.

Float64 (örneğin, var veya stddev) ondalık dönüş sonucu bazı işlevler. Ara hesaplamalar hala Float64 ve aynı değerlere sahip ondalık girişler arasında farklı sonuçlara yol açabilecek ondalık olarak gerçekleştirilebilir.

## Taşma Kontrolleri {#overflow-checks}

Ondalık hesaplamalar sırasında tamsayı taşmaları gerçekleşebilir. Bir kesirdeki aşırı rakamlar atılır (yuvarlatılmamış). Tamsayı bölümünde aşırı basamak bir istisna yol açacaktır.

``` sql
SELECT toDecimal32(2, 4) AS x, x / 3
```

``` text
┌──────x─┬─divide(toDecimal32(2, 4), 3)─┐
│ 2.0000 │                       0.6666 │
└────────┴──────────────────────────────┘
```

``` sql
SELECT toDecimal32(4.2, 8) AS x, x * x
```

``` text
DB::Exception: Scale is out of bounds.
```

``` sql
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
DB::Exception: Decimal math overflow.
```

Taşma kontrolleri operasyonların yavaşlamasına neden olur. Taşmaların mümkün olmadığı biliniyorsa, kontrolleri kullanarak devre dışı bırakmak mantıklıdır `decimal_check_overflow` ayar. Kontroller devre dışı bırakıldığında ve taşma gerçekleştiğinde, sonuç yanlış olacaktır:

``` sql
SET decimal_check_overflow = 0;
SELECT toDecimal32(4.2, 8) AS x, 6 * x
```

``` text
┌──────────x─┬─multiply(6, toDecimal32(4.2, 8))─┐
│ 4.20000000 │                     -17.74967296 │
└────────────┴──────────────────────────────────┘
```

Taşma kontrolleri sadece aritmetik işlemlerde değil, değer karşılaştırmasında da gerçekleşir:

``` sql
SELECT toDecimal32(1, 8) < 100
```

``` text
DB::Exception: Can't compare.
```

[Orijinal makale](https://clickhouse.tech/docs/en/data_types/decimal/) <!--hide-->
