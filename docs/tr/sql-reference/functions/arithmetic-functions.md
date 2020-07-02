---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: Aritmetik
---

# Aritmetik Fonksiyonlar {#arithmetic-functions}

Tüm aritmetik işlevler için, sonuç türü, böyle bir tür varsa, sonucun sığdığı en küçük sayı türü olarak hesaplanır. Minimum, bit sayısına, imzalanıp imzalanmadığına ve yüzüp yüzmediğine bağlı olarak aynı anda alınır. Yeterli bit yoksa, en yüksek bit türü alınır.

Örnek:

``` sql
SELECT toTypeName(0), toTypeName(0 + 0), toTypeName(0 + 0 + 0), toTypeName(0 + 0 + 0 + 0)
```

``` text
┌─toTypeName(0)─┬─toTypeName(plus(0, 0))─┬─toTypeName(plus(plus(0, 0), 0))─┬─toTypeName(plus(plus(plus(0, 0), 0), 0))─┐
│ UInt8         │ UInt16                 │ UInt32                          │ UInt64                                   │
└───────────────┴────────────────────────┴─────────────────────────────────┴──────────────────────────────────────────┘
```

Aritmetik fonksiyonlar, uint8, Uİnt16, Uİnt32, Uint64, Int8, Int16, Int32, Int64, Float32 veya Float64 türlerinden herhangi bir çift için çalışır.

Taşma, C++ile aynı şekilde üretilir.

## artı (a, b), A + B operatörü {#plusa-b-a-b-operator}

Sayıların toplamını hesaplar.
Ayrıca bir tarih veya tarih ve Saat ile tamsayı numaraları ekleyebilirsiniz. Bir tarih durumunda, bir tamsayı eklemek, karşılık gelen gün sayısını eklemek anlamına gelir. Zamanla bir tarih için, karşılık gelen saniye sayısını eklemek anlamına gelir.

## eksi (a, b), A - B operatörü {#minusa-b-a-b-operator}

Farkı hesaplar. Sonuç her zaman imzalanır.

You can also calculate integer numbers from a date or date with time. The idea is the same – see above for ‘plus’.

## çarp operatorma (a, b), A \* B operatörü {#multiplya-b-a-b-operator}

Sayıların ürününü hesaplar.

## böl (a, b), A / B operatörü {#dividea-b-a-b-operator}

Sayıların bölümünü hesaplar. Sonuç türü her zaman bir kayan nokta türüdür.
Tam sayı bölümü değildir. Tamsayı bölümü için, ‘intDiv’ İşlev.
Sıfıra bölerek zaman olsun ‘inf’, ‘-inf’, veya ‘nan’.

## ıntdiv(a, b) {#intdiva-b}

Sayıların bölümünü hesaplar. Tamsayılara bölünür, yuvarlanır (mutlak değere göre).
Sıfıra bölünürken veya en az negatif sayıyı eksi bir ile bölürken bir istisna atılır.

## ıntdivorzero(a, b) {#intdivorzeroa-b}

Farklıdır ‘intDiv’ bu, sıfıra bölünürken veya en az bir negatif sayıyı eksi bir ile bölerek sıfır döndürür.

## modulo (a, b), A % B operatörü {#moduloa-b-a-b-operator}

Bölünmeden sonra kalan hesaplar.
Bağımsız değişkenler kayan nokta sayılarıysa, ondalık bölümü bırakarak tamsayılara önceden dönüştürülürler.
Kalan C++ile aynı anlamda alınır. Kesik bölme negatif sayılar için kullanılır.
Sıfıra bölünürken veya en az negatif sayıyı eksi bir ile bölürken bir istisna atılır.

## moduloOrZero(a, b) {#moduloorzeroa-b}

Farklıdır ‘modulo’ bölen sıfır olduğunda sıfır döndürür.

## negate (a), - bir operatör {#negatea-a-operator}

Ters işareti ile bir sayı hesaplar. Sonuç her zaman imzalanır.

## abs (a) {#arithm_func-abs}

\(A\) sayısının mutlak değerini hesaplar. Yani, \< 0 ise,- A döndürür. imzasız türler için hiçbir şey yapmaz. İmzalı tamsayı türleri için imzasız bir sayı döndürür.

## gcd (a, b) {#gcda-b}

Sayıların en büyük ortak böleni döndürür.
Sıfıra bölünürken veya en az negatif sayıyı eksi bir ile bölürken bir istisna atılır.

## lcm(a, b) {#lcma-b}

Sayıların en az ortak katını döndürür.
Sıfıra bölünürken veya en az negatif sayıyı eksi bir ile bölürken bir istisna atılır.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/arithmetic_functions/) <!--hide-->
